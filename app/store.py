import sqlite3
from contextlib import contextmanager
from pathlib import Path
from .config import DB_PATH


def connect():
    path = Path(DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db


@contextmanager
def db_session():
    db = connect()
    try:
        with db:
            yield db
    finally:
        db.close()


def initialize():
    with db_session() as db:
        db.execute("""CREATE TABLE IF NOT EXISTS processed_messages (
            message_id TEXT PRIMARY KEY, processed_at TEXT NOT NULL)""")
        db.execute("""CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY, amount REAL NOT NULL, message_id TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL)""")
        db.execute("""CREATE TABLE IF NOT EXISTS demo_failures (
            message_id TEXT PRIMARY KEY, failures INTEGER NOT NULL DEFAULT 0)""")


def is_processed(message_id):
    with db_session() as db:
        return db.execute("SELECT 1 FROM processed_messages WHERE message_id=?", (message_id,)).fetchone() is not None


def bump_demo_failure(message_id):
    with db_session() as db:
        db.execute("""INSERT INTO demo_failures(message_id, failures) VALUES(?, 1)
                      ON CONFLICT(message_id) DO UPDATE SET failures=failures+1""", (message_id,))
        return db.execute("SELECT failures FROM demo_failures WHERE message_id=?", (message_id,)).fetchone()[0]


def commit_order(event):
    with db_session() as db:
        existing = db.execute("SELECT 1 FROM processed_messages WHERE message_id=?", (event["message_id"],)).fetchone()
        if existing:
            return False
        db.execute("INSERT INTO orders(order_id, amount, message_id, created_at) VALUES(?,?,?,?)",
                   (event["order_id"], event["amount"], event["message_id"], event["occurred_at"]))
        db.execute("INSERT INTO processed_messages(message_id, processed_at) VALUES(?,?)",
                   (event["message_id"], event["occurred_at"]))
        return True
