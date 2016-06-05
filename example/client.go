package main

import (
	"github.com/index0h/go-porter/message"
	"github.com/streadway/amqp"
	"log"
	"os"
)

func main() {
	amqpDsn := "amqp://guest:guest@172.19.0.2:5672/"
	rpcQueueName := "rpc"

	logger := log.New(os.Stdout, "", 0)

	var (
		amqpConnection *amqp.Connection
		err            error
	)

	if amqpConnection, err = amqp.Dial(amqpDsn); err != nil {
		logger.Panicf("Failed to connect to RabbitMQ: %s", err)
	}

	defer amqpConnection.Close()

	var amqpChannel *amqp.Channel

	if amqpChannel, err = amqpConnection.Channel(); err != nil {
		logger.Panicf("Failed to open a channel: %s", err)
	}

	defer amqpChannel.Close()

	var delivery <-chan amqp.Delivery

	delivery, err = amqpChannel.Consume(
		rpcQueueName, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)

	if err != nil {
		logger.Panicf("Failed to register a consumer: %s", err)
	}

	for request := range delivery {
		log.Printf("Request received: %s", request.Body)

		response := &message.Response{
			StatusCode: 200,
			Body:       []byte("response"),
		}

		data, _ := response.Bytes()

		err = amqpChannel.Publish(
			"",              // exchange
			request.ReplyTo, // routing key
			false,           // mandatory
			false,           // immediate
			amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: request.CorrelationId,
				Body:          data,
			},
		)

		if err != nil {
			logger.Panicf("Failed to send response: %s", err)
		}

		log.Printf("Response sent: %s", response.Body)

		request.Ack(true)
	}
}
