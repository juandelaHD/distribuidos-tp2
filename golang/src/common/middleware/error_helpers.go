package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func middlewareError(err error) error {
	if err == amqp.ErrClosed {
		return ErrMessageMiddlewareDisconnected
	}
	return ErrMessageMiddlewareMessage
}

func closeConnection(conn *amqp.Connection, err error) error {
	_ = conn.Close()
	return middlewareError(err)
}

func closeChannelAndConnection(ch *amqp.Channel, conn *amqp.Connection, err error) error {
	_ = ch.Close()
	_ = conn.Close()
	return middlewareError(err)
}
