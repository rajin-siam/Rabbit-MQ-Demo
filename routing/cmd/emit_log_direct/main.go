// main.go — the PRODUCER for the "routing" tutorial.
//
// This is tutorial 3's emitter with ONE conceptual change: instead of a
// FANOUT exchange (broadcast to everyone), we use a DIRECT exchange and
// tag each message with a "routing key" — here, the log's severity
// (e.g. "info", "warning", "error"). A direct exchange only delivers a
// message to queues whose binding key exactly matches that routing key,
// so receivers can now choose which severities they care about.
package main

import (
	"context"
	"log"
	"os"
	"strings"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const brokerURI = "amqp://guest:guest@localhost:5672/"

func main() {
	ctx := context.Background()

	env := rmq.NewEnvironment(brokerURI, nil)
	conn, err := env.NewConnection(ctx)
	if err != nil {
		log.Panicf("Failed to connect to RabbitMQ: %v", err)
	}
	defer func() {
		_ = env.CloseConnections(context.Background())
	}()

	// Declare a DIRECT exchange named "logs_direct" (a different name from
	// tutorial 3's "logs" fanout exchange, so both demos can coexist on
	// the same broker without clashing).
	//
	// A direct exchange routes each message to the queues whose binding
	// key exactly equals the message's routing key — no broadcasting.
	_, err = conn.Management().DeclareExchange(ctx, &rmq.DirectExchangeSpecification{Name: "logs_direct"})
	if err != nil {
		log.Panicf("Failed to declare an exchange: %v", err)
	}

	// Read severity + message text from the command line, e.g.:
	//   go run . warn "disk almost full"
	severity, body := parseArgs(os.Args)

	// Unlike the fanout publisher, the Key here is NOT empty — it's the
	// routing key ("info"/"warning"/"error") that the exchange will use
	// to decide which bound queues get this message.
	publisher, err := conn.NewPublisher(ctx, &rmq.ExchangeAddress{Exchange: "logs_direct", Key: severity}, nil)
	if err != nil {
		log.Panicf("Failed to create publisher: %v", err)
	}
	defer func() { _ = publisher.Close(context.Background()) }()

	res, err := publisher.Publish(ctx, rmq.NewMessage([]byte(body)))
	if err != nil {
		log.Panicf("Failed to publish a message: %v", err)
	}

	switch res.Outcome.(type) {
	case *rmq.StateAccepted:
		// success — nothing to do
	default:
		log.Panicf("Unexpected publish outcome: %v", res.Outcome)
	}

	log.Printf(" [x] Sent [%s] %s", severity, body)
}

// parseArgs splits the command line into (severity, message).
//
//	os.Args[0] = program name
//	os.Args[1] = severity, e.g. "info", "warning", "error" (defaults to "info")
//	os.Args[2:] = the rest is joined together as the message body
func parseArgs(args []string) (severity, body string) {
	severity = "info"
	if len(args) > 1 && args[1] != "" {
		severity = args[1]
	}
	if len(args) > 2 {
		body = strings.Join(args[2:], " ")
	} else {
		body = "hello"
	}
	return severity, body
}
