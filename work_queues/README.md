# Work Queues

Go implementation of RabbitMQ tutorial 2 (Work Queues), using
`github.com/rabbitmq/rabbitmq-amqp-go-client` (AMQP 1.0) instead of the
classic `amqp091-go` client the official tutorial uses. Concepts map
1:1; API calls differ.

## Idea

`new_task` publishes tasks to a shared queue. Multiple `worker`
instances pull from that same queue and split the work — instead of
one process doing everything immediately, tasks queue up and get
processed by whichever worker is free. Adding more workers scales
throughput.

## Files

- `new_task/main.go` — producer. Sends one message built from CLI args
  to queue `task_queue`.
- `worker/main.go` — consumer. Loops forever, pulls one task at a
  time, "does work" (sleeps), then acknowledges.

## Fake work: dot-counting

Task body's `.` count = simulated work seconds. `"hello..."` sleeps
3s in the worker before ack. Lets you fake variable task duration
without real work.

## Concepts and how they map to this client

**Durability (tasks survive broker restart)**
Tutorial requires durable queue + persistent messages. Here:
- Queue declared as `QuorumQueueSpecification` — quorum queues are
  inherently durable/replicated.
- Messages: `ensureDurable()` in the client sets `Header.Durable =
  true` automatically when publishing, unless you override it. No
  extra code needed (unlike `amqp091-go` where you must pass
  `DeliveryMode: amqp.Persistent` explicitly).

**Manual acknowledgment**
Tutorial disables auto-ack so a crashed worker's in-flight task gets
requeued instead of lost. Here: consumer is created without
pre-settle mode, so each `delivery.Accept(ctx)` in `worker/main.go` is
an explicit ack. If the worker dies before calling `Accept`, RabbitMQ
redelivers the message to another worker.

**Fair dispatch (prefetch)**
Tutorial calls `channel.Qos(1, 0, false)` so RabbitMQ won't push a
new message to a worker until it acks the current one — otherwise
round-robin dispatch can pile up slow tasks on one worker while others
sit idle.

This client is AMQP 1.0, which uses link *credit* instead of AMQP
0-9-1's Qos/prefetch. `worker/main.go` sets:

```go
conn.NewConsumer(ctx, "task_queue", &rmq.ConsumerOptions{InitialCredits: 1})
```

`InitialCredits: 1` means the broker only ever has permission to send
this worker one unsettled message. It won't grant more credit until
that message is settled (`Accept`), so a slow task blocks new
deliveries to that worker — the same fair-dispatch effect as
`Qos(1)`. Default (unset) is 256 credits, which would let RabbitMQ
dispatch messages round-robin regardless of how busy a worker is.

## Running

Two mains live in separate subpackages so each is its own `package
main` (avoids duplicate `main`/const issues from cramming both into
one folder):

```bash
# terminal 1: start one or more workers
go run ./worker
go run ./worker      # start a second one to see load spread across both

# terminal 2: send tasks
go run ./new_task First message.
go run ./new_task Second message..
go run ./new_task Third message...
```

Watch dots in the message decide how long each worker sleeps before
acking — with prefetch=1, a worker stuck on a long task won't be handed
the next one; it goes to whichever worker is free.
