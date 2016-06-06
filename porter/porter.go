package porter

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/index0h/go-porter/message"
	"github.com/streadway/amqp"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type entry struct {
	responseWriter http.ResponseWriter
	closeRequest   chan bool
}

type Porter struct {
	lock      sync.RWMutex
	counter   uint32
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

func (p *Porter) correlationID() string {
	return fmt.Sprintf("%d", atomic.AddUint32(&p.counter, 1))
}

func (p *Porter) Start() {
	for delivery := range p.fromQueue {
		go p.handleDelivery(delivery)
	}
}

func (p *Porter) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	correlationID := p.correlationID()

	logger := p.logger.WithFields(
		logrus.Fields{
			"RequestURI":    request.RequestURI,
			"Method":        request.Method,
			"RemoteAddr":    request.RemoteAddr,
			"CorrelationId": correlationID,
		},
	)

	var (
		err  error
		body []byte
	)

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
