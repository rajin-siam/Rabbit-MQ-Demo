# RabbitMQ Pub/Sub Demo (Go, AMQP 1.0)

A minimal "logging system" that demonstrates the **publish/subscribe**
messaging pattern using RabbitMQ and the official Go AMQP 1.0 client.

One program **emits** log messages. Any number of other programs can
**receive** them — every receiver gets its own copy of every message,
live, in real time.

---

## 1. The core idea

In the simplest messaging setup, a producer sends a message straight
into a queue, and a consumer reads it out. That's fine for a to-do list,
but it doesn't model a real broadcast — like a log stream that multiple
tools might want to watch at once.

RabbitMQ solves this by never letting producers touch a queue directly.
Instead:

```
Producer  --->  Exchange  --->  Queue  --->  Consumer
```

- **Producer** — sends messages.
- **Exchange** — receives messages and decides where they go. It never
  stores anything itself.
- **Queue** — a buffer that actually holds messages.
- **Consumer** — reads messages out of a queue.

The exchange's routing behavior depends on its **type**. This project
uses a **`fanout`** exchange, which is the simplest kind: it copies
every message it receives into *every* queue that's bound to it — no
filtering, no routing keys. That's exactly "broadcast to everyone,"
which is what a logging system needs.

### System diagram

This is the shape of the system once everything is running: one
producer, one fanout exchange, and each receiver with its own private
queue bound to that exchange.


![img.png](img.png)

```mermaid
flowchart LR
    P["Producer<br/>cmd/emit_log"] -->|publish| E{{"Exchange: logs<br/>(fanout)"}}
    E -->|copy| Q1["Queue<br/>(auto-generated,<br/>exclusive)"]
    E -->|copy| Q2["Queue<br/>(auto-generated,<br/>exclusive)"]
    Q1 --> C1["Consumer 1<br/>cmd/receive_logs"]
    Q2 --> C2["Consumer 2<br/>cmd/receive_logs"]
```

A fanout exchange doesn't look at routing keys at all — it just
copies every message into every queue that's currently bound to it.
Add a third receiver and it gets its own queue and its own copy too;
remove one and its queue is deleted automatically.

## 2. What each program does

| File | Role |
|---|---|
| `cmd/emit_log/main.go` | Producer. Declares the `logs` fanout exchange and publishes one message (from command-line args) to it. |
| `cmd/receive_logs/main.go` | Consumer. Declares the same exchange, creates its **own private, temporary queue**, binds that queue to the exchange, then loops printing whatever arrives. |

The key trick is in the receiver: instead of a shared queue with a
fixed name, each receiver asks RabbitMQ to auto-generate a **new,
exclusive, auto-delete queue** just for itself:

- `IsExclusive: true` — only this program's connection can use it.
- `IsAutoDelete: true` — RabbitMQ deletes it automatically the moment
  the program disconnects, so nothing is left behind.

Because every receiver has its *own* queue, and the fanout exchange
copies each message into *every bound queue*, every receiver ends up
with its own full copy of the message stream.

### Project / setup flow

The sequence below shows what actually happens, in order, from
starting a receiver through to a message landing in its terminal.
Steps 1–3 (declare exchange, declare queue, bind) are pure setup;
step 4 onward is the live message flow.

![img_1.png](img_1.png)

```mermaid
sequenceDiagram
    participant Recv as receive_logs
    participant Broker as RabbitMQ
    participant Emit as emit_log

    Recv->>Broker: 1. Declare exchange "logs" (fanout)
    Recv->>Broker: 2. Declare auto-generated queue<br/>(exclusive, auto-delete)
    Recv->>Broker: 3. Bind queue to "logs" exchange
    Recv->>Broker: Wait for messages (Receive)

    Emit->>Broker: Declare exchange "logs" (fanout)
    Emit->>Broker: 4. Publish message to "logs"
    Broker-->>Emit: Outcome: accepted
    Broker->>Recv: 5. Deliver copy of message
    Recv->>Broker: 6. Accept (acknowledge) message
```

Notice both programs declare the exchange — that's safe and
intentional, since "declare" just means "create if missing, otherwise
confirm it already matches." Whichever program starts first is the
one that actually creates it.

## 3. Prerequisites

- **Go** 1.21+ installed (`go version` to check).
- **RabbitMQ** running locally with the AMQP 1.0 plugin enabled, on
  the default port `5672`. Easiest way with Docker:

  ```bash
  docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 \
    rabbitmq:4-management
  ```

  Then enable the AMQP 1.0 protocol plugin (it ships with RabbitMQ but
  is off by default):

  ```bash
  docker exec rabbitmq rabbitmq-plugins enable rabbitmq_amqp1_0
  ```

  (Management UI, optional, at http://localhost:15672 — user/pass
  `guest`/`guest`.)

## 4. Project setup

```bash
cd pubsub-demo

# Fetch the RabbitMQ AMQP 1.0 client and lock its version into go.mod
go get github.com/rabbitmq/rabbitmq-amqp-go-client

# Tidy up go.mod / go.sum
go mod tidy
```

## 5. Run it

**Terminal 1 — start a receiver:**

```bash
go run ./cmd/receive_logs
# [*] Waiting for logs. To exit press CTRL+C
```

**Terminal 2 — start a second receiver** (optional, to prove the
broadcast works):

```bash
go run ./cmd/receive_logs
# [*] Waiting for logs. To exit press CTRL+C
```

**Terminal 3 — emit a message:**

```bash
go run ./cmd/emit_log "Here is the first log"
# [x] Sent Here is the first log
```

You should see `[x] Here is the first log` printed in **both**
receiver terminals at once — each one got its own copy via its own
queue, both fed by the same `logs` exchange. This is exactly the
fan-out shown in the system diagram above.

Run the emitter again with a different message to see it broadcast
again. Stop a receiver with `Ctrl+C` and its private queue disappears
automatically (auto-delete).

## 6. Code walkthrough

### Connecting (`cmd/emit_log/main.go` and `cmd/receive_logs/main.go`)

```go
env := rmq.NewEnvironment(brokerURI, nil)
conn, err := env.NewConnection(ctx)
```

`NewEnvironment` sets up the client with connection details; `NewConnection`
actually opens the TCP connection to RabbitMQ. `brokerURI` encodes
user, password, host, and port: `amqp://guest:guest@localhost:5672/`.

### Declaring the exchange (both files)

```go
conn.Management().DeclareExchange(ctx, &rmq.FanOutExchangeSpecification{Name: "logs"})
```

"Declare" means *create it if it doesn't already exist, otherwise
confirm it matches*. It's safe to call this in both programs — whoever
runs first actually creates it.

### Publishing (`cmd/emit_log/main.go`)

```go
publisher, _ := conn.NewPublisher(ctx, &rmq.ExchangeAddress{Exchange: "logs", Key: ""}, nil)
publisher.Publish(ctx, rmq.NewMessage([]byte(body)))
```

The publisher is wired to always send to the `logs` exchange. `Key` is
empty because fanout exchanges don't use routing keys.

### The private queue (`cmd/receive_logs/main.go`)

```go
qInfo, _ := conn.Management().DeclareQueue(ctx, &rmq.AutoGeneratedQueueSpecification{
IsExclusive:  true,
IsAutoDelete: true,
})
```

No name is given, so RabbitMQ generates a random one. This is the
"mailbox" that will receive this particular receiver's copy of every
broadcast message.

### Binding (`cmd/receive_logs/main.go`)

```go
conn.Management().Bind(ctx, &rmq.ExchangeToQueueBindingSpecification{
SourceExchange:   "logs",
DestinationQueue: qInfo.Name(),
BindingKey:       "",
})
```

This is the step that actually links the exchange to this specific
queue — without it, messages sent to `logs` would just be discarded
(a fanout exchange with no bound queues drops everything).

### Consuming (`cmd/receive_logs/main.go`)

```go
for {
delivery, _ := consumer.Receive(ctx)
// ...print the message...
delivery.Accept(ctx)
}
```

`Receive` blocks until a message shows up. `Accept` acknowledges it —
telling RabbitMQ "I've handled this, you can remove it from my
queue."

## 7. Next steps

This is Pub/Sub — everyone gets everything. The next tutorial in the
series ("Routing") introduces the `direct` exchange type, which lets
consumers subscribe to only a *subset* of messages using routing keys
(e.g. only "error" logs instead of all logs).