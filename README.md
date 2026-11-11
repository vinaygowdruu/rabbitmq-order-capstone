# RabbitMQ Order Processing Capstone

A local Windows-friendly learning project that combines RabbitMQ, Python, quorum queues, publisher confirms, bounded retries, a dead-letter queue, idempotent processing, and Prometheus/Grafana monitoring.

> This is a learning environment, not a production deployment. The default credentials and single-host Docker volumes are for local practice only. The RabbitMQ dead-letter policy is configured for at-least-once transfer; duplicated deliveries are still possible and the worker is written to be idempotent.

## What you will build

```text
publish_order.py --confirm --> orders.exchange --> orders (quorum)
                                                  | worker ACK after SQLite commit
                                                  | transient failure: reject
                                                  v
                                        orders.retry.exchange
                                                  |
                                   orders.retry (quorum, 10s TTL)
                                                  |
                                   orders.exchange -> orders

                         permanent failure / retry limit
                                                  v
                                          orders.dlx -> orders.dlq
```

Three RabbitMQ nodes form a cluster. The `orders`, `orders.retry`, and `orders.dlq` queues are quorum queues. The publisher uses confirms and mandatory routing. The worker uses manual ACKs, prefetch, and SQLite uniqueness on `message_id` to make the business effect idempotent. Prometheus scrapes all three broker nodes; Grafana is available for dashboards.

## Prerequisites

- Windows 10/11 with Docker Desktop running in Linux-container mode
- Python 3.10 or later
- PowerShell

## 1. Start the cluster and monitoring

From this project directory:

```powershell
docker compose up -d
```

Wait for the services to become healthy, then inspect:

```powershell
docker compose ps
docker exec rabbit1 rabbitmq-diagnostics -q cluster_status
```

The RabbitMQ nodes use a shared Erlang cookie and fixed hostnames on the Compose network. Node 2 and node 3 start after node 1 reports healthy, then discover and join it using the configured classic peer-discovery list. If you restart a node, keep its named volume; do not delete node data as a first recovery step.

### Local addresses

| Service | Address | Purpose |
| --- | --- | --- |
| RabbitMQ node 1 AMQP | `localhost:5672` | Python client connection |
| RabbitMQ node 2 AMQP | `localhost:5673` | Alternate client endpoint |
| RabbitMQ node 3 AMQP | `localhost:5674` | Alternate client endpoint |
| Management UI node 1 | <http://localhost:15672> | Broker/queue inspection |
| Management UI node 2 | <http://localhost:15673> | Broker/queue inspection |
| Management UI node 3 | <http://localhost:15674> | Broker/queue inspection |
| Prometheus | <http://localhost:9090> | Metrics queries |
| Grafana | <http://localhost:3000> | Dashboards |

RabbitMQ local login: `capstone` / `capstone-dev-password`. Grafana local login: `admin` / `capstone-admin-password`. These are intentionally simple development credentials. Change them before any shared or remotely reachable deployment.

RabbitMQ's AMQP listener is 5672 inside each container. The project maps it to 5672, 5673, and 5674 on Windows. The management and Prometheus listeners are 15672 and 15692 inside each node; host mappings vary by node.

## 2. Enable the quorum-queue at-least-once dead-letter policy

The project opts into RabbitMQ's at-least-once dead-lettering for the two source queues that use a DLX. The setting requires a quorum source queue, `overflow=reject-publish`, and the `stream_queue` feature flag if it is not already enabled.

Run once after the cluster is formed:

```powershell
docker exec rabbit1 rabbitmqctl enable_feature_flag stream_queue
docker exec rabbit1 rabbitmqctl set_policy orders-at-least-once "^(orders|orders.retry)$" '{"dead-letter-strategy":"at-least-once","overflow":"reject-publish","max-length":10000}' --apply-to quorum_queues --priority 10
```

If RabbitMQ says the feature flag is already enabled, continue. Check the active policy with:

```powershell
docker exec rabbit1 rabbitmqctl list_policies
```

The source queues retain dead-lettered messages until the target accepts them. This improves transfer safety but consumes more resources and can produce duplicates. Do not remove or weaken this policy while messages are pending without understanding the effect. See the [Quorum Queues guide](https://www.rabbitmq.com/docs/quorum-queues) and [Dead Letter Exchanges guide](https://www.rabbitmq.com/docs/dlx).

## 3. Install the Python client

Open a second PowerShell window in the project directory:

```powershell
Copy-Item .env.example .env  # optional: edit the local passwords first
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r app\requirements.txt
```

If PowerShell blocks virtual environment activation, run the scripts using `.venv\Scripts\python.exe` directly instead.

## 4. Declare topology and publish an order

```powershell
python -m app.topology
python -m app.publish_order --order-id ORD-1001 --amount 42.50
```

Expected behavior: the publisher prints a message ID and a broker confirm result. `mandatory=True` makes unroutable messages visible to the publisher instead of silently treating them as delivered.

To send a controlled transient-failure demo that succeeds after two retries:

```powershell
python -m app.publish_order --order-id ORD-RETRY-1 --amount 19.99 --fail-first 2
```

To send a permanent failure that reaches the DLQ after the configured retry limit:

```powershell
python -m app.publish_order --order-id ORD-POISON-1 --amount 0 --permanent-failure
```

These demo flags are carried as message headers and exist only to make the lab repeatable. The normal order payload is JSON.

## 5. Run the worker

In another PowerShell window:

```powershell
.\.venv\Scripts\Activate.ps1
python -m app.worker
```

The worker has `prefetch_count=5`, uses manual acknowledgements, writes successful orders to `data\orders.db`, and ACKs only after the SQLite transaction commits. Start a second worker in another terminal to see competing consumers share work.

Inspect queues while messages are moving:

```powershell
docker exec rabbit1 rabbitmqctl list_queues name type messages_ready messages_unacknowledged consumers
```

The retry queue waits 10 seconds before returning the message to `orders`. The worker inspects RabbitMQ's `x-death` metadata to enforce the retry limit. Transient failures are rejected without requeue to enter the delayed retry cycle; terminal failures use an explicit confirmed publish to the DLQ. A permanent failure or an exhausted retry limit is republished to `orders.dlq` with publisher confirms; only after the DLQ publish is confirmed does the worker ACK the original delivery. A crash between those operations can duplicate the DLQ entry, so the original message ID is preserved.

## 6. Verify idempotency and redelivery

The SQLite database is local to the worker process machine and persists under `app/data/`. `message_id` has a primary key, so if a consumer commits the order and then crashes before ACK, RabbitMQ may redeliver but the business effect is not applied a second time.

To simulate a crash before ACK, first publish a fresh order, then start the worker with `--crash-before-ack-once`. The worker commits the message, terminates before ACK on the first processing attempt for that message ID, and then the broker can redeliver it. Run a replacement worker and observe the duplicate-safe path:

```powershell
python -m app.publish_order --order-id ORD-CRASH-1 --amount 8.25
python -m app.worker --crash-before-ack-once
# After it exits:
python -m app.worker
```

Use a fresh order ID for this exercise. Inspect logs for the same `message_id` and a duplicate skip, then query the SQLite orders table if desired.

## 7. Inspect the dead-letter queue

```powershell
docker exec rabbit1 rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
```

`orders.dlq` should contain the poison order. In the Management UI, open Queues and inspect the message headers, especially `x-death`, which records dead-letter reasons and queue history. A production DLQ process should alert, preserve context, and provide an audited replay or remediation operation.

## 8. Monitoring

Open Prometheus at <http://localhost:9090>. Example PromQL queries:

```promql
rabbitmq_queue_messages_ready
rabbitmq_queue_messages_unacked
rabbitmq_channel_messages_published_total
rabbitmq_channel_messages_delivered_total
rabbitmq_channel_messages_acked_total
rabbitmq_build_info
```

Metric names can vary with RabbitMQ/plugin version and scrape configuration. Use Prometheus's `/metrics` page on each node to find the exact names available in this image. Prometheus scrapes `rabbit1:15692`, `rabbit2:15692`, and `rabbit3:15692` over the Docker network.

Grafana is at <http://localhost:3000>. Add a Prometheus data source with URL `http://prometheus:9090` (the name resolves inside Compose), then build panels for ready/unacked messages, publish/deliver/ACK rates, and node health. Alert on sustained backlog growth, zero consumers for a work queue, rising unacknowledged counts, and node/resource alarms rather than a single momentary spike.

Useful broker diagnostics:

```powershell
docker exec rabbit1 rabbitmq-diagnostics -q ping
docker exec rabbit1 rabbitmq-diagnostics -q cluster_status
docker exec rabbit1 rabbitmqctl list_connections name peer_host peer_port state
docker exec rabbit1 rabbitmqctl list_consumers
docker logs rabbit2 --tail 100
```

## 9. Failure drills

### Consumer failure

Publish several orders, stop the worker before ACK, and inspect `messages_unacknowledged`. When RabbitMQ detects the closed connection, those deliveries are requeued. Start a replacement worker and confirm that processing is idempotent.

### One broker node unavailable

```powershell
docker stop rabbit2
# observe cluster status, queue health, publisher behavior, and confirms
docker start rabbit2
```

A three-member quorum queue can make progress with two members. Confirm that node 2 rejoins and inspect queue health before concluding the test.

### Lose quorum

Stopping two of the three members prevents a three-member quorum queue from making progress. Do this only in the disposable local environment. AMQP publisher confirms can be delayed or nacked during election or quorum loss. Restore the original members and their volumes; do not infer success from the management page being reachable. A confirmed publish is different from a publish whose outcome is unknown.

## 10. Architecture and limitations

- Publisher confirms cover broker acceptance, not consumer processing.
- Manual consumer ACKs cover the consumer-to-broker leg. ACK only after durable work.
- At-least-once delivery means duplicate processing is possible. Stable message IDs and idempotency are required.
- The retry cycle is intentionally bounded. Immediate requeue loops are avoided.
- Quorum queues require a majority of their members to make progress. Three replicas tolerate one member being unavailable, not two.
- This example stores idempotency in SQLite on one machine. Multiple workers must share a transactional database or another shared idempotency store in a multi-host deployment.
- The demo topology declares queues as quorum queues. For a small local cluster this is useful for learning; temporary/high-churn queues and every possible workload are not automatically good fits for quorum queues.
- Prometheus/Grafana are included as a local monitoring stack. A production deployment needs secure credentials, TLS, resource planning, backup/restore procedures, alert routing, and tested operational runbooks.

## 11. Stop and reset

Stop containers but keep the data volumes:

```powershell
docker compose down
```

To completely reset this disposable lab, remove the named broker volumes as well:

```powershell
docker compose down -v
```

The second command permanently deletes the local RabbitMQ cluster data. Use it only when you intentionally want a fresh lab.

## Project files

- `docker-compose.yml` — three broker nodes, Prometheus, and Grafana
- `rabbitmq/rabbitmq.conf` — static peer discovery, cluster partition behavior, and listener settings
- `rabbitmq/enabled_plugins` — management and Prometheus plugins
- `prometheus/prometheus.yml` — scrape targets
- `grafana/provisioning/datasources/prometheus.yml` — preconfigured local Prometheus source
- `app/topology.py` — exchanges, quorum queues, bindings, TTL, and dead-letter wiring
- `app/publish_order.py` — confirmed, mandatory publisher with stable message IDs
- `app/worker.py` — competing consumer, prefetch, bounded retry, idempotent storage, and crash drill
- `app/store.py` — SQLite schema and transactions

## References

- [RabbitMQ cluster formation](https://www.rabbitmq.com/docs/cluster-formation)
- [RabbitMQ quorum queues](https://www.rabbitmq.com/docs/quorum-queues)
- [RabbitMQ acknowledgements and publisher confirms](https://www.rabbitmq.com/docs/confirms)
- [RabbitMQ dead-letter exchanges](https://www.rabbitmq.com/docs/dlx)
- [RabbitMQ Prometheus monitoring](https://www.rabbitmq.com/docs/prometheus)
