package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	const brokerURI = "amqp://guest:guest@localhost:5672/"
	ctx := context.Background()
	env := rmq.NewEnvironment(brokerURI, nil)
	conn, err := env.NewConnection(ctx)
	if err != nil {
		log.Panicf("Failed to connect to RabbitMQ: %v", err)
	}
	defer func() {
		_ = env.CloseConnections(context.Background())
	}()

	_, err = conn.Management().DeclareQueue(ctx, &rmq.QuorumQueueSpecification{Name: "task_queue"})
	if err != nil {
		log.Panicf("Failed to declare a queue: %v", err)
	}

	// InitialCredits: 1 is the AMQP 1.0 equivalent of channel.Qos(prefetch=1):
	// the broker won't dispatch a new task to this worker until the current
	// one is Accept()-ed, giving fair round-robin dispatch across workers.
	consumer, err := conn.NewConsumer(ctx, "task_queue", &rmq.ConsumerOptions{InitialCredits: 1})
	if err != nil {
		log.Panicf("Failed to create consumer: %v", err)
	}
	defer func() { _ = consumer.Close(context.Background()) }()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	for {
		delivery, err := consumer.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Panicf("Failed to receive a message: %v", err)
		}
		msg := delivery.Message()
		var body []byte
		if len(msg.Data) > 0 {
			body = msg.Data[0]
		}
		log.Printf("Received a message: %s", body)
		dotCount := bytes.Count(body, []byte("."))
		time.Sleep(time.Duration(dotCount) * time.Second)
		log.Printf("Done")
		err = delivery.Accept(ctx)
		if err != nil {
			log.Panicf("Failed to accept message: %v", err)
		}
	}
}
