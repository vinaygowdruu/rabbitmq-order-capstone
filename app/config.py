import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "capstone")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "capstone-dev-password")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
PREFETCH = int(os.getenv("PREFETCH", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_MS = int(os.getenv("RETRY_DELAY_MS", "10000"))
DB_PATH = os.getenv("ORDERS_DB", "app/data/orders.db")
