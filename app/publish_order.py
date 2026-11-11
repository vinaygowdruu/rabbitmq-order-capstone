import argparse
import json
import time
import uuid
from datetime import datetime, timezone

import pika
from .config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_VHOST
from .topology import WORK_EXCHANGE, ROUTING_KEY, connection_parameters


def publish(event, headers):
    last_error = None
    for attempt in range(1, 6):
        connection = None
        try:
            connection = pika.BlockingConnection(connection_parameters())
            channel = connection.channel()
            channel.confirm_delivery()
            properties = pika.BasicProperties(
                content_type="application/json", content_encoding="utf-8",
                delivery_mode=pika.DeliveryMode.Persistent,
                message_id=event["message_id"], timestamp=int(time.time()),
                type="order.created", app_id="rabbitmq-order-capstone", headers=headers,
            )
            # With confirm_delivery() enabled, pika 1.x basic_publish returns None on a
            # broker ack and raises UnroutableError / NackError otherwise.
            channel.basic_publish(
                exchange=WORK_EXCHANGE, routing_key=ROUTING_KEY,
                body=json.dumps(event, separators=(",", ":")).encode("utf-8"),
                properties=properties, mandatory=True,
            )
            print(f"CONFIRMED message_id={event['message_id']} order_id={event['order_id']}")
            return
        except pika.exceptions.AMQPError as exc:
            last_error = exc
            print(f"Publish attempt {attempt} uncertain/failed: {exc}")
            if attempt < 5:
                time.sleep(min(2 ** (attempt - 1), 10))
        finally:
            if connection is not None and connection.is_open:
                connection.close()
    raise SystemExit(f"Publish not confirmed after retries: {last_error}")


def main():
    parser = argparse.ArgumentParser(description="Publish a confirmed order event")
    parser.add_argument("--order-id", required=True)
    parser.add_argument("--amount", type=float, required=True)
    parser.add_argument("--fail-first", type=int, default=0, help="Demo: simulate this many transient failures")
    parser.add_argument("--permanent-failure", action="store_true", help="Demo: route a permanent failure to the DLQ")
    parser.add_argument("--message-id", help="Supply a stable ID when retrying the same business event")
    args = parser.parse_args()
    if args.amount < 0:
        parser.error("amount must be zero or greater")
    event = {
        "message_id": args.message_id or str(uuid.uuid4()),
        "event_type": "order.created",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "order_id": args.order_id,
        "amount": round(args.amount, 2),
    }
    headers = {"x-demo-fail-first": args.fail_first}
    if args.permanent_failure:
        headers["x-demo-permanent-failure"] = True
    publish(event, headers)


if __name__ == "__main__":
    main()
