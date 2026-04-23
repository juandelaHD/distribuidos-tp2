package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type queueMiddleware struct {
	conn *amqp.Connection
	channel *amqp.Channel
	queueName string
}

func newQueueMiddleware(queueName string, connectionSettings ConnSettings) (*queueMiddleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, closeConnection(conn, err)
	}

	if _, err := ch.QueueDeclare(
		queueName,
		true, // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	); err != nil {
		return nil, closeChannelAndConnection(ch, conn, err)
	}

	return &queueMiddleware{
		conn: conn,
		channel: ch,
		queueName: queueName,
	}, nil
}

func (q *queueMiddleware) Send(msg Message) error {
	err := q.channel.Publish(
		"", // exchange default
		q.queueName, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body: []byte(msg.Body),
		},
	)
	if err != nil {
		return middlewareError(err)
	}
	return nil
}

func (q *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	deliveries, err := q.channel.Consume(
		q.queueName, // queue
		"", // random consumer tag
		false, // no-auto-ack
		false, // no-exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	if err != nil {
		return middlewareError(err)
	}

	// Iteramos sobre el channel hasta que se cierre.
	// Con autoAck=false, el broker retiene el mensaje hasta recibir el Ack.
	// Si el proceso muere sin hacer Ack, el mensaje se reencola automáticamente.
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

func (q *queueMiddleware) StopConsuming() error {
	return q.channel.Close()
}

func (q *queueMiddleware) Close() error {
	if err := q.channel.Close(); err != nil && err != amqp.ErrClosed {
		return ErrMessageMiddlewareClose
	}
	if err := q.conn.Close(); err != nil && err != amqp.ErrClosed {
		return ErrMessageMiddlewareClose
	}
	return nil
}
