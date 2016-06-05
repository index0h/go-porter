package message

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"net/http"
)

type Request struct {
	RequestURI string              `json:"requestURI"`
	Method     string              `json:"method"`
	RemoteAddr string              `json:"remoteAddr"`
	Header     map[string][]string `json:"header"`
	Body       []byte              `json:"body"`
}

func NewRequestFromHTTP(request http.Request) *Request {
	body := []byte{}

	request.Body.Read(body[:])

	return &Request{
		RequestURI: request.RequestURI,
		Method:     request.Method,
		RemoteAddr: request.RemoteAddr,
		Header:     request.Header,
		Body:       body,
	}
}

func NewRequestFromAMQPDelivery(delivery amqp.Delivery) (*Request, error) {
	request := &Request{}

	if err := json.Unmarshal(delivery.Body, request); err != nil {
		return nil, err
	}

	return request, nil
}

func (this *Request) Bytes() ([]byte, error) {
	return json.Marshal(this)
}
