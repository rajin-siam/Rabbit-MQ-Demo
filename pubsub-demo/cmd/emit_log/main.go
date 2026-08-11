// emit_log.go — the PRODUCER.
// Its only job: connect to RabbitMQ, make sure the "logs" exchange exists,
// and publish one message to it. It doesn't know or care who (if anyone)
// is listening — that's the whole point of pub/sub.
package main

import (
	"context"
	"log"
	"os"
	"strings"

	// The official AMQP 1.0 client for RabbitMQ, aliased to "rmq" so we
	// don't have to type the long package name everywhere.
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// Connection string: user "guest", password "guest", host "localhost",
// port 5672 (RabbitMQ's default AMQP port). These are the default
// credentials RabbitMQ ships with for local/dev use only.
const brokerURI = "amqp://guest:guest@localhost:5672/"

func main() {
	ctx := context.Background()

	// An "Environment" is the client's top-level entry point — think of
	// it as the object that knows how to open connections to the broker.
	env := rmq.NewEnvironment(brokerURI, nil)

	// Open an actual network connection to RabbitMQ using that environment.
	conn, err := env.NewConnection(ctx)
	if err != nil {
		log.Panicf("Failed to connect to RabbitMQ: %v", err)
	}
	// defer = "run this when main() exits, no matter how". Here it makes
	// sure we cleanly close the connection when the program is done.
	defer func() {
		_ = env.CloseConnections(context.Background())
	}()

	// Declare (create-if-missing) an exchange named "logs" of type FANOUT.
	// A fanout exchange ignores routing keys entirely and just copies
	// every message it receives to every queue bound to it — exactly the
	// "broadcast to everyone" behavior we want for a logging system.
	_, err = conn.Management().DeclareExchange(ctx, &rmq.FanOutExchangeSpecification{Name: "logs"})
	if err != nil {
		log.Panicf("Failed to declare an exchange: %v", err)
	}

	// Create a publisher that always sends to the "logs" exchange.
	// Key: "" because fanout exchanges don't use routing keys — every
	// bound queue gets the message regardless of key.
	publisher, err := conn.NewPublisher(ctx, &rmq.ExchangeAddress{Exchange: "logs", Key: ""}, nil)
	if err != nil {
		log.Panicf("Failed to create publisher: %v", err)
	}
	defer func() { _ = publisher.Close(context.Background()) }()

	// Build the message body from whatever was typed after the program
	// name on the command line, e.g.:  go run emit_log.go "server down"
	body := bodyFrom(os.Args)

	// Actually send the message.
	res, err := publisher.Publish(ctx, rmq.NewMessage([]byte(body)))
	if err != nil {
		log.Panicf("Failed to publish a message: %v", err)
	}

	// AMQP 1.0 publishes return an "outcome" telling us whether the
	// broker accepted the message. We check it's a plain "accepted" —
	// anything else (e.g. rejected) is treated as an error here.
	switch res.Outcome.(type) {
	case *rmq.StateAccepted:
		// success — nothing to do
	default:
		log.Panicf("Unexpected publish outcome: %v", res.Outcome)
	}

	log.Printf(" [x] Sent %s", body)
}

// bodyFrom turns the command-line arguments into the message text.
// os.Args[0] is always the program name itself, so real arguments
// start at index 1. If none were given, default to "hello".
func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}
