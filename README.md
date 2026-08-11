# Rabbit-MQ-Demo

A hands-on collection of small Go examples that follow the classic RabbitMQ tutorials: Hello World (simple queue), Work Queues, Publish/Subscribe (fanout), Routing, and Topic exchanges. Each folder contains a minimal sender and receiver program so you can run experiments, learn messaging concepts, and see how different RabbitMQ patterns behave in practice.

This repo is intended for learners: it gives runnable examples, explains what each demo shows, and includes troubleshooting tips and next steps for deeper study.

## Stack
- Language(s): Go (100%)
- Runtime: Go 1.18+ (any recent Go toolchain)
- Notable libraries: Uses a RabbitMQ AMQP client fetched via `go mod` (examples use typical amqp client packages — run `go mod tidy` to fetch dependencies)

## Project layout
Top-level demo directories (each mirrors a RabbitMQ pattern):
```
hello-world/     # Simple queue: basic send & receive (single queue)
pubsub-demo/     # Publish/Subscribe (fanout exchange): emit_log + receive_logs
routing/         # Routing (direct exchange) demo
topic/           # Topic exchange demo
work_queues/     # Work queues (task distribution) demo
```

How it fits together:
- Each demo generally contains a sender (publisher) and a receiver (consumer).
- Publishers publish messages to either a queue (default exchange) or an exchange (fanout/direct/topic).
- Consumers create/declare queues and bind them to exchanges when required, then consume messages and (optionally) acknowledge them.
- The demos show variations: single queue, multiple workers, broadcast (fanout), selective routing (direct), and wildcard routing (topic).

---

## Quickstart — run the examples locally

Prerequisites:
- Go 1.18+ installed and in your PATH.
- Docker (recommended) OR a RabbitMQ server available on a host you control.
- A terminal.

1) Start RabbitMQ with the management plugin (Docker):
```bash
docker run -d --hostname rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```
- Management UI: http://localhost:15672 (default user/password: guest/guest)
- AMQP port: 5672

2) From the repository root, fetch modules:
```bash
cd /path/to/Rabbit-MQ-Demo
go mod tidy
```

3) Run the Hello World demo
- Start a receiver (consumer):
```bash
go run hello-world/receive/main.go
```
- In another terminal send a message:
```bash
go run hello-world/send/main.go "Hello World!"
```
Expected: the receiver prints/receives the message. This demo uses a single queue (named `hello`) — one publisher, one consumer pattern.

4) Run the Publish/Subscribe (pubsub-demo)
- Start a receiver:
```bash
go run pubsub-demo/cmd/receive_logs/main.go
```
- Start a publisher (send a message):
```bash
go run pubsub-demo/cmd/emit_log/main.go "info: a log message"
```
Expected: Every running consumer bound to the `logs` exchange receives a copy. This demonstrates a fanout exchange (broadcast to all bound queues).

5) Other demos
- Work Queues: run multiple consumers in `work_queues/` and send multiple messages — see how tasks are distributed to workers.
- Routing: run routing producers and consumers to see `direct` exchange behavior.
- Topic: use routing keys with patterns to see wildcard matching.

(Each demo folder contains its own `main.go` files; run the sender and receiver files as shown above — some accept command-line messages via args.)

---

## What each demo teaches (educational notes)

hello-world/
- Pattern: Simple queue.
- Goal: Show the simplest send/receive flow: declare queue, publish to default exchange (or queue), consume.
- Key file(s): `hello-world/send/main.go`, `hello-world/receive/main.go`
- Concepts: queue declaration, basic publish, synchronous consumption.

pubsub-demo/
- Pattern: Publish/Subscribe (fanout exchange).
- Goal: Broadcast messages to multiple subscribers.
- Key file(s): `pubsub-demo/cmd/emit_log/main.go`, `pubsub-demo/cmd/receive_logs/main.go`
- Concepts:
  - Exchanges vs queues: publisher sends to an exchange (not directly to queue).
  - `fanout` exchange type: forwards messages to all queues bound to it.
  - Auto-generated, exclusive queues for temporary subscriptions.
  - No routing key is used for fanout.

work_queues/
- Pattern: Work queues (task distribution).
- Goal: Distribute time-consuming tasks among a pool of workers.
- Concepts:
  - Message acknowledgments: ensure tasks aren't lost if a worker dies.
  - durable queues vs persistent messages (survive broker restart).
  - prefetch and fair dispatch.

routing/
- Pattern: Direct exchange (routing by exact key).
- Goal: Send messages selectively based on a routing key.
- Concepts:
  - `direct` exchange: bind queue with specific routing keys.

topic/
- Pattern: Topic exchange (wildcard routing).
- Goal: Complex routing matching with `*` and `#`.
- Concepts:
  - `topic` exchange: flexible, pattern-based routing.

---

## Core RabbitMQ concepts (short primer)

- Broker: the RabbitMQ server that sits between senders and receivers.
- Exchange: message routing logic. Types: `direct`, `fanout`, `topic`, `headers`.
- Queue: buffer that stores messages until consumers consume them.
- Bindings: links between exchanges and queues, often with routing keys.
- Routing key: a string used by exchanges to decide where to deliver messages (direct/topic).
- Acknowledgement (ack): consumer confirms successful processing; prevents message loss.
- Durable queue vs transient: durability relates to surviving broker restarts.
- Persistent messages: message property to request broker to persist the message to disk.
- Exclusive queue: only accessible by the connection that declared it; deleted on close.
- Auto-delete queue: deleted automatically when the last consumer unsubscribes.
- Prefetch / QoS: limits unacknowledged messages per consumer for fair dispatch.

---

## Expected outputs & examples

- Hello-world consumer prints lines like:
  - [x] Received 'Hello World!'
- Pubsub consumer prints lines like:
  - [x] Sent 'my log message'  (emit side)
  - [*] Waiting for logs. To exit press CTRL+C (receive side)
- Work queues with multiple workers: each worker will print messages it processes and you’ll observe distribution between them.

---

## Troubleshooting

- Connection refused:
  - Is RabbitMQ running at `localhost:5672`? If using Docker, ensure container is running and ports mapped.
  - Use `docker ps` to check; check container logs via `docker logs rabbitmq`.
- `guest` login refused from non-localhost:
  - By default RabbitMQ allows `guest` only from localhost. Use management UI or add another user.
- Messages not received:
  - Ensure consumer declared same queue/exchange/binding as publisher.
  - Check for missing `queue.Declare` or wrong exchange type.
- Dependencies failing to fetch:
  - Run `go mod tidy` and check network/proxy settings.

---

## Next learning steps (recommended)
- Read the official RabbitMQ tutorials: https://www.rabbitmq.com/getstarted.html
- Experiment:
  - Try marking messages persistent and queues durable; restart broker and observe behavior.
  - Implement reconnection/backoff logic in producers and consumers.
  - Add dead-letter exchanges and TTL headers for delayed/retry logic.
- Explore advanced features:
  - Consumer acknowledgements, manual vs auto-ack
  - Publisher confirms for guaranteed delivery
  - Transactions vs confirms
  - Streams plugin or Kafka-like patterns if you need log-style retention

---

## Contributing
Suggestions, improvements and additional comments are welcome. Typical contributions:
- Clearer comments and explanations inside the sample programs
- Additional examples (RPC, priority queues, dead-letter handling)
- Tests or helper scripts to automate starting RabbitMQ and running demos

Please open issues or PRs in this repository.

---

## License
Add a license file to this repo (e.g., MIT, Apache-2.0). This README assumes community-friendly licensing.

---

## Where to look in this repo
- hello-world/send/main.go — publisher (queue "hello")
- hello-world/receive/main.go — consumer (queue "hello")
- pubsub-demo/cmd/emit_log/main.go — publisher emitting to `logs` (fanout)
- pubsub-demo/cmd/receive_logs/main.go — receiver binding to `logs`
- routing/, topic/, work_queues/ — other tutorial-style examples

If anything in the code doesn't match what you expect from this README, open an issue or mention the file path and I'll help clarify the mapping.

Happy learning — run the demos, change routing keys and flags, and observe what happens!
