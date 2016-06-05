package porter

import (
	"crypto/rand"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/index0h/go-porter/message"
	"github.com/streadway/amqp"
	"net/http"
	"sync"
	"time"
)

type entry struct {
	responseWriter http.ResponseWriter
	closeRequest   chan bool
}

type Porter struct {
	lock      sync.RWMutex
	toQueue   chan<- amqp.Publishing
	fromQueue <-chan amqp.Delivery
	ttl       time.Duration
	entries   map[string]entry
	logger    *logrus.Entry
}

func NewPorter(
	toQueue chan<- amqp.Publishing,
	fromQueue <-chan amqp.Delivery,
	ttl time.Duration,
	logger *logrus.Entry,
) *Porter {
	entries := make(map[string]entry)

	return &Porter{
		toQueue:   toQueue,
		fromQueue: fromQueue,
		ttl:       ttl,
		entries:   entries,
		logger:    logger,
	}
}

func (this *Porter) correlationId() (string, error) {
	result := [16]byte{}

	if _, err := rand.Read(result[:]); err != nil {
		return "", err
	}

	result[8] = (result[8] | 0x80) & 0xBF
	result[6] = (result[6] | 0x40) & 0x4F

	return fmt.Sprintf("%X", result), nil
}

func (this *Porter) Start() {
	for delivery := range this.fromQueue {
		this.handleDelivery(delivery)
	}
}

func (this *Porter) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	logger := this.logger.WithFields(
		logrus.Fields{
			"RequestURI": request.RequestURI,
			"Method":     request.Method,
			"RemoteAddr": request.RemoteAddr,
		},
	)

	var (
		correlationId string
		err           error
		body          []byte
	)

	if correlationId, err = this.correlationId(); err != nil {
		http.Error(responseWriter, "", 500)

		logger.Errorf("Could not create correlation id: ", err)

		return
	}

	logger = logger.WithField("CorrelationId", correlationId)

	if body, err = message.NewRequestFromHTTP(*request).Bytes(); err != nil {
		http.Error(responseWriter, "", 500)

		logger.Errorf("Could not serialize request: ", err)
	}

	closeRequest := make(chan bool)

	this.lock.Lock()

	this.entries[correlationId] = entry{
		responseWriter: responseWriter,
		closeRequest:   closeRequest,
	}

	this.lock.Unlock()

	message := amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: correlationId,
		Body:          body,
	}

	go func() {
		this.toQueue <- message

		logger.Info("Request sent to AMQP")
	}()

	select {
	case <-closeRequest:
	case <-time.After(this.ttl):
		this.lock.Lock()

		if entry, entryFound := this.entries[correlationId]; entryFound {
			delete(this.entries, correlationId)

			http.Error(entry.responseWriter, "", 500)

			logger.Errorf("Request TTL expied")
		}

		this.lock.Unlock()
	}

	close(closeRequest)
}

func (this *Porter) handleDelivery(delivery amqp.Delivery) {
	delivery.Ack(true)

	logger := this.logger.WithField("CorrelationId", delivery.CorrelationId)

	this.lock.Lock()

	entry, entryFound := this.entries[delivery.CorrelationId]

	if !entryFound {
		logger.Warning("Entry not found by CorrelationId")

		this.lock.Unlock()

		return
	}

	delete(this.entries, delivery.CorrelationId)

	this.lock.Unlock()

	defer func() {
		entry.closeRequest <- true
	}()

	queueMessage, err := message.NewResponseFromAMQPDelivery(delivery)

	if err != nil {
		http.Error(entry.responseWriter, "", 500)

		logger.Errorf("Could not parse response message: %s", err)
	}

	if queueMessage.Header != nil {
		for key, values := range queueMessage.Header {
			for _, value := range values {
				entry.responseWriter.Header().Add(key, value)
			}
		}
	}

	entry.responseWriter.WriteHeader(queueMessage.StatusCode)

	fmt.Fprintln(entry.responseWriter, queueMessage.Body)

	logger.WithField("Status", queueMessage.StatusCode).Info("Response sent to HTTP")
}
