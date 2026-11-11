import argparse
import json
import os
import hashlib
import pika
from .config import PREFETCH, MAX_RETRIES
from .store import initialize, is_processed, bump_demo_failure, commit_order
from .topology import WORK_QUEUE, DLX_EXCHANGE, ROUTING_KEY, connection_parameters



class PermanentOrderError(Exception):
    pass


class TransientOrderError(Exception):
    pass


def rejected_attempts(headers):
    deaths = (headers or {}).get("x-death", [])
    for entry in deaths:
        if entry.get("queue") == WORK_QUEUE and entry.get("reason") == "rejected":
            return int(entry.get("count", 0))
    return 0


def send_to_dlq(ch, method, properties, body, reason):
    headers = dict(properties.headers or {})
    headers["x-final-failure"] = reason
    dlq_properties = pika.BasicProperties(
        content_type=properties.content_type or "application/octet-stream",
        content_encoding=properties.content_encoding,
        delivery_mode=pika.DeliveryMode.Persistent,
        message_id=properties.message_id,
        timestamp=properties.timestamp,
        type=properties.type,
        app_id=properties.app_id,
        headers=headers,
    )
    # The channel is in confirm mode. Do not ACK the source delivery unless this
    # DLQ publish is confirmed. A crash after confirm and before ACK can duplicate;
    # the stable message_id is preserved for safe DLQ handling.
    ch.basic_publish(exchange=DLX_EXCHANGE, routing_key=ROUTING_KEY,
                     body=body, properties=dlq_properties, mandatory=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def callback_factory(channel, crash_before_ack_once):
    def on_message(ch, method, properties, body):
        message_id = properties.message_id or "missing-id"
        try:
            event = json.loads(body.decode("utf-8"))
            if event.get("message_id") != message_id:
                raise PermanentOrderError("body/property message_id mismatch")
            if is_processed(message_id):
                print(f"DUPLICATE skipped message_id={message_id}", flush=True)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            headers = properties.headers or {}
            if headers.get("x-demo-permanent-failure"):
                raise PermanentOrderError("requested permanent-failure demo")
            fail_first = int(headers.get("x-demo-fail-first", 0))
            if fail_first > 0:
                failure_count = bump_demo_failure(message_id)
                if failure_count <= fail_first:
                    raise TransientOrderError(f"requested transient failure {failure_count}/{fail_first}")

            committed = commit_order(event)
            print(f"PROCESSED message_id={message_id} order_id={event['order_id']} inserted={committed}", flush=True)
            marker_name = hashlib.sha256(message_id.encode("utf-8")).hexdigest()
            crash_marker = os.path.join("app", "data", f"crash-before-ack-{marker_name}")
            if crash_before_ack_once and not os.path.exists(crash_marker):
                os.makedirs(os.path.dirname(crash_marker), exist_ok=True)
                with open(crash_marker, "w", encoding="utf-8") as marker:
                    marker.write(message_id)
                print("SIMULATED crash after database commit and before ACK", flush=True)
                os._exit(23)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except PermanentOrderError as exc:
            print(f"PERMANENT failure message_id={message_id}: {exc}; sending to DLQ", flush=True)
            send_to_dlq(ch, method, properties, body, str(exc))
        except TransientOrderError as exc:
            retries = rejected_attempts(properties.headers)
            if retries >= MAX_RETRIES:
                print(f"RETRY LIMIT message_id={message_id}: {exc}; sending to DLQ", flush=True)
                send_to_dlq(ch, method, properties, body, f"retry limit: {exc}")
            else:
                print(f"RETRY message_id={message_id} rejected_attempts={retries}: {exc}", flush=True)
                ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            print(f"INVALID message_id={message_id}: {exc}; dead-lettering", flush=True)
            send_to_dlq(ch, method, properties, body, f"invalid message: {exc}")
        except Exception as exc:
            # Unexpected application errors are retried via the delayed retry queue,
            # but only up to the broker-generated x-death count limit above.
            retries = rejected_attempts(properties.headers)
            if retries >= MAX_RETRIES:
                print(f"UNEXPECTED retry limit message_id={message_id}: {exc}; sending to DLQ", flush=True)
                send_to_dlq(ch, method, properties, body, f"unexpected error: {exc}")
            else:
                print(f"UNEXPECTED retry message_id={message_id}: {exc}", flush=True)
                ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
    return on_message


def main():
    parser = argparse.ArgumentParser(description="Consume orders with ACK, retry, DLQ, and idempotency")
    parser.add_argument("--crash-before-ack-once", action="store_true")
    args = parser.parse_args()
    initialize()
    connection = pika.BlockingConnection(connection_parameters())
    channel = connection.channel()
    channel.confirm_delivery()
    channel.basic_qos(prefetch_count=PREFETCH)
    channel.basic_consume(queue=WORK_QUEUE,
                          on_message_callback=callback_factory(channel, args.crash_before_ack_once),
                          auto_ack=False)
    print(f"Worker consuming {WORK_QUEUE}; prefetch={PREFETCH}; max retries={MAX_RETRIES}", flush=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Stopping worker", flush=True)
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
