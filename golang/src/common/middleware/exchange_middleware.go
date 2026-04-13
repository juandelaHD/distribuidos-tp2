package middleware

import (
	"fmt"
	
	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeMiddleware struct {
	conn *amqp.Connection
	channel *amqp.Channel
	exchange string
	routingKeys []string
}

func newExchangeMiddleware(exchange string, keys []string, settings ConnSettings) (*exchangeMiddleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, closeConnection(conn, err)
	}

	if err := ch.ExchangeDeclare(
		exchange, // nombre del exchange
		"direct", // direct porque queremos rutear por routing key
		true, // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return nil, closeChannelAndConnection(ch, conn, err)
	}

	return &exchangeMiddleware{
		conn: conn,
		channel: ch,
		exchange: exchange,
		routingKeys: keys,
	}, nil
}

func (e *exchangeMiddleware) Send(msg Message) error {
	for _, key := range e.routingKeys {
		err := e.channel.Publish(
			e.exchange, // nombre del exchange
			key, // routing key para rutear el mensaje
			false, // mandatory
			false, // inmediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body: []byte(msg.Body),
			},
		)
		if err != nil {
			return middlewareError(err)
		}
	}
	return nil
}

func (e *exchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	q, err := e.channel.QueueDeclare(
		"", // nombre random
		false, // durable
		true, // auto-delete
		true, // exclusive
		false, // no-wait
		nil)
	if err != nil {
		return middlewareError(err)
	}

	// bindeamos la queue al exchange con cada una de las routing keys indicadas.
	for _, key := range e.routingKeys {
		if err := e.channel.QueueBind(q.Name, key, e.exchange, false, nil); err != nil {
			return middlewareError(err)
		}
	}

	deliveries, err := e.channel.Consume(q.Name, "", false, true, false, false, nil)
	if err != nil {
		return middlewareError(err)
	}

	for delivery := range deliveries {
		d := delivery
		callbackFunc(
			Message{Body: string(d.Body)},
			func() { d.Ack(false) },
			func() { d.Nack(false, true) },
		)
	}

	return ErrMessageMiddlewareDisconnected
}

func (e *exchangeMiddleware) StopConsuming() error {
	return e.channel.Close()
}

func (e *exchangeMiddleware) Close() error {
	if err := e.channel.Close(); err != nil && err != amqp.ErrClosed {
		return ErrMessageMiddlewareClose
	}
	if err := e.conn.Close(); err != nil && err != amqp.ErrClosed {
		return ErrMessageMiddlewareClose
	}
	return nil
}
