#!/bin/bash
rabbitmqctl set_policy orders-at-least-once \
  '^(orders|orders\.retry)$' \
  '{"dead-letter-strategy":"at-least-once","overflow":"reject-publish","max-length":10000}' \
  --apply-to quorum_queues --priority 10
