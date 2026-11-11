import pika
from .config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_VHOST, RETRY_DELAY_MS

WORK_EXCHANGE = "orders.exchange"
RETRY_EXCHANGE = "orders.retry.exchange"
DLX_EXCHANGE = "orders.dlx"
WORK_QUEUE = "orders"
RETRY_QUEUE = "orders.retry"
DLQ = "orders.dlq"
ROUTING_KEY = "order.created"


def connection_parameters():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, virtual_host=RABBITMQ_VHOST,
        credentials=credentials, heartbeat=30, blocked_connection_timeout=30,
        connection_attempts=5, retry_delay=2,
    )


def declare_topology(channel):
    channel.exchange_declare(exchange=WORK_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=RETRY_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type="direct", durable=True)

    channel.queue_declare(
        queue=WORK_QUEUE, durable=True,
        arguments={"x-queue-type": "quorum", "x-dead-letter-exchange": RETRY_EXCHANGE,
                   "x-dead-letter-routing-key": ROUTING_KEY},
    )
    channel.queue_declare(
        queue=RETRY_QUEUE, durable=True,
        arguments={"x-queue-type": "quorum", "x-message-ttl": RETRY_DELAY_MS,
                   "x-dead-letter-exchange": WORK_EXCHANGE,
                   "x-dead-letter-routing-key": ROUTING_KEY},
    )
    channel.queue_declare(
        queue=DLQ, durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    channel.queue_bind(queue=WORK_QUEUE, exchange=WORK_EXCHANGE, routing_key=ROUTING_KEY)
    channel.queue_bind(queue=RETRY_QUEUE, exchange=RETRY_EXCHANGE, routing_key=ROUTING_KEY)
    channel.queue_bind(queue=DLQ, exchange=DLX_EXCHANGE, routing_key=ROUTING_KEY)


def main():
    connection = pika.BlockingConnection(connection_parameters())
    try:
        channel = connection.channel()
        declare_topology(channel)
        print("Topology declared: quorum queues orders, orders.retry, orders.dlq")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
