package message

import (
	"encoding/json"
	"github.com/streadway/amqp"
)

type Response struct {
	StatusCode int                 `json:"status"`
	Header     map[string][]string `json:"header"`
	Body       []byte              `json:"body"`
}

func NewResponseFromAMQPDelivery(delivery amqp.Delivery) (*Response, error) {
	response := &Response{}

	if err := json.Unmarshal(delivery.Body, response); err != nil {
		return nil, err
	}

	return response, nil
}

func (r *Response) Bytes() ([]byte, error) {
	return json.Marshal(r)
}
