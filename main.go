package main

import (
	"github.com/index0h/go-porter/porter"
	"flag"
	"github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"net/http"
	"strconv"
	"time"
)

func main() {
	amqpDsn := flag.String("amqp-dsn", "", "AMQP DSN, example 'amqp://guest:guest@localhost:5672/'")
	routingKey := flag.String("amqp-routing-key", "", "AMQP routing key")
	port := flag.Uint("port", 8080, "HTTP port")
	ttl := flag.Uint64("ttl", 30, "HTTP request ttl (sec)")

	flag.Parse()

	log := logrus.New()
	log.Formatter = &logrus.JSONFormatter{}

	logger := logrus.NewEntry(log)

	if *amqpDsn == "" {
		logger.Fatal("amqp-dsn is required")
	} else if *routingKey == "" {
		logger.Fatal("amqp-routing-key is required")
	}

	var (
		amqpConnection *amqp.Connection
		err            error
	)

	if amqpConnection, err = amqp.Dial(*amqpDsn); err != nil {
		logger.Panicf("Failed to connect to RabbitMQ: %s", err)
	}

	defer amqpConnection.Close()

	var amqpChannel *amqp.Channel

	if amqpChannel, err = amqpConnection.Channel(); err != nil {
		logger.Panicf("Failed to open a channel: %s", err)
	}

	defer amqpChannel.Close()

	var consumeQueue amqp.Queue

	consumeQueue, err = amqpChannel.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		logger.Panicf("Failed to declare a consume queue: %s", err)
	}

	err = amqpChannel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)

	if err != nil {
		logger.Panicf("Failed to set QoS: %s", err)
	}

	var delivery <-chan amqp.Delivery

	delivery, err = amqpChannel.Consume(
		consumeQueue.Name, // queue
		"",                // consumer
		false,             // auto-ack
		true,              // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)

	if err != nil {
		logger.Panicf("Failed to register a consumer: %s", err)
	}

	logger.Warn()

	publish := make(chan amqp.Publishing)

	go func() {
		for publishing := range publish {
			publishing.ReplyTo = consumeQueue.Name

			err := amqpChannel.Publish(
				"",          // exchange
				*routingKey, // routing key
				false,       // mandatory
				false,       // immediate
				publishing,
			)

			if err != nil {
				logger.Panicf("Failed to publish a message: %s", err)
			}
		}
	}()

	porter := porter.NewPorter(publish, delivery, time.Duration(*ttl)*time.Second, logger)

	go porter.Start()

	http.ListenAndServe(":"+strconv.Itoa(int(*port)), porter)
}
