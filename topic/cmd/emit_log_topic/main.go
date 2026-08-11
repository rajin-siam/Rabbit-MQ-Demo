// emit_log_topic.go — the PRODUCER for the "topics" tutorial.
//
// This builds on tutorial 4's direct exchange, but swaps in a TOPIC
// exchange. The routing key is no longer a single flat word like
// "warning" — it's a dot-separated multi-part key of the form
// "<facility>.<severity>", e.g. "kern.critical" or "auth.info".
// A topic exchange lets receivers match against these keys using
// wildcards, which a direct exchange can't do.
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

	// Declare a TOPIC exchange named "logs_topic" (kept separate from
	//
	// A topic exchange routes messages using pattern matching on a
	// dotted routing key, instead of the exact-match-only behavior of
	// a direct exchange.
	_, err = conn.Management().DeclareExchange(ctx, &rmq.TopicExchangeSpecification{Name: "logs_topic"})
	if err != nil {
		log.Panicf("Failed to declare an exchange: %v", err)
	}

	// Read the routing key ("facility.severity") and the message text
	// from the command line, e.g.:
	//   go run . kern.critical "disk controller failure"
	routingKey, body := parseArgs(os.Args)

	publisher, err := conn.NewPublisher(ctx, &rmq.ExchangeAddress{Exchange: "logs_topic", Key: routingKey}, nil)
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

	log.Printf(" [x] Sent [%s] %s", routingKey, body)
}

// parseArgs splits the command line into (routingKey, message).
//
//	os.Args[0] = program name
//	os.Args[1] = routing key, e.g. "kern.critical" (defaults to "anonymous.info")
//	os.Args[2:] = the rest is joined together as the message body
func parseArgs(args []string) (routingKey, body string) {
	routingKey = "anonymous.info"
	if len(args) > 1 && args[1] != "" {
		routingKey = args[1]
	}
	if len(args) > 2 {
		body = strings.Join(args[2:], " ")
	} else {
		body = "hello"
	}
	return routingKey, body
}
