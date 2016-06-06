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

func (p *Porter) correlationID() (string, error) {
	result := [16]byte{}

	if _, err := rand.Read(result[:]); err != nil {
		return "", err
	}

	result[8] = (result[8] | 0x80) & 0xBF
	result[6] = (result[6] | 0x40) & 0x4F

	return fmt.Sprintf("%X", result), nil
}

func (p *Porter) Start() {
	for delivery := range p.fromQueue {
		p.handleDelivery(delivery)
	}
}

func (p *Porter) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	logger := p.logger.WithFields(
		logrus.Fields{
			"RequestURI": request.RequestURI,
			"Method":     request.Method,
			"RemoteAddr": request.RemoteAddr,
		},
	)

	var (
		correlationID string
		err           error
		body          []byte
	)

	if correlationID, err = p.correlationID(); err != nil {
		http.Error(responseWriter, "", 500)

		logger.Errorf("Could not create correlation id: %s", err)

		return
	}

	logger = logger.WithField("CorrelationId", correlationID)

	if body, err = message.NewRequestFromHTTP(*request).Bytes(); err != nil {
		http.Error(responseWriter, "", 500)

		logger.Errorf("Could not serialize request: %s", err)
	}

	closeRequest := make(chan bool)

	p.lock.Lock()

	p.entries[correlationID] = entry{
		responseWriter: responseWriter,
		closeRequest:   closeRequest,
	}

	p.lock.Unlock()

	message := amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: correlationID,
		Body:          body,
	}

	go func() {
		p.toQueue <- message

		logger.Info("Request sent to AMQP")
	}()

	select {
	case <-closeRequest:
	case <-time.After(p.ttl):
		p.lock.Lock()

		if entry, entryFound := p.entries[correlationID]; entryFound {
			delete(p.entries, correlationID)

			http.Error(entry.responseWriter, "", 500)

			logger.Errorf("Request TTL expied")
		}

		p.lock.Unlock()
	}

	close(closeRequest)
}

func (p *Porter) handleDelivery(delivery amqp.Delivery) {
	logger := p.logger.WithField("CorrelationId", delivery.CorrelationId)

	p.lock.Lock()

	entry, entryFound := p.entries[delivery.CorrelationId]

	if !entryFound {
		logger.Warning("Entry not found by CorrelationId")

		p.lock.Unlock()

		return
	}

	delete(p.entries, delivery.CorrelationId)

	p.lock.Unlock()

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
