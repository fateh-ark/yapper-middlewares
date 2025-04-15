package main

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	var err error

	// Rabbit MQ Setup
	rabbitConn, err := amqp.Dial("amqp://reader:reader12345@rabbitmq:5672/")
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}
	defer rabbitConn.Close()

	rabbitChan, err := rabbitConn.Channel()
	if err != nil {
		log.Fatal("Failed to open a channel:", err)
	}
	defer rabbitChan.Close()

	err = rabbitChan.ExchangeDeclare(
		"service_log", // Exchange name
		"topic",       // Exchange type (topic)
		true,          // Durable
		false,         // Auto-deleted
		false,         // Internal
		false,         // No-wait
		nil,           // Arguments
	)
	if err != nil {
		log.Fatal("Failed to declare an exchange:", err)
	}

	queue, err := rabbitChan.QueueDeclare(
		"",    // Queue name (auto-generated)
		false, // Durable
		true,  // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Fatal("Failed to declare a queue:", err)
	}

	err = rabbitChan.QueueBind(
		queue.Name,    // Queue name
		"*",           // Routing key pattern (e.g., "book.created", "book.updated")
		"service_log", // Exchange
		false,         // No-wait
		nil,           // Arguments
	)
	if err != nil {
		log.Fatal("Failed to bind a queue:", err)
	}

	msgs, err := rabbitChan.Consume(
		queue.Name, // Queue
		"",         // Consumer
		true,       // Auto-ack
		false,      // Exclusive
		false,      // No-local
		false,      // No-wait
		nil,        // Args
	)
	if err != nil {
		log.Fatal("Failed to register a consumer:", err)
	}

	var forever chan struct{}

	go func() {
		for d := range msgs {
			var event map[string]interface{}
			if err := json.Unmarshal(d.Body, &event); err != nil {
				log.Println("Error unmarshalling message:", err)
				continue
			}

			prettyJSON, err := json.MarshalIndent(event, "", "  ") // "" for prefix, "  " for indent
			if err != nil {
				log.Println("Error marshaling to pretty JSON:", err)
				log.Printf("Received event (%s): %v", d.RoutingKey, event) // Fallback to default
				continue
			}
			log.Printf("Received event (%s):\n%s", d.RoutingKey, string(prettyJSON))
		}
	}()

	log.Printf(" [*] Waiting for messages")
	<-forever // run the script forever
}
