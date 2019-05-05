package rabbeasy

import (
	"github.com/streadway/amqp"
)

// Message wraps message delivered
type Message interface {
	MessageBody
	Acker
	Requeuer
	DeadLetterSender
	Deliveryer
}

// MessageBody returns bytes of message
type MessageBody interface {
	Body() []byte
}

// Acker confirms message with ok
type Acker interface {
	Ack()
}

// Requeuer confirms message with requeue
type Requeuer interface {
	Requeue()
}

// DeadLetterSender confirms message with sending to discard
type DeadLetterSender interface {
	DeadLetter()
}

// Deliveryer returns original received message
type Deliveryer interface {
	Delivery() *amqp.Delivery
}

// Delivery implements Message and wraps message delivered
type Delivery struct {
	dlv *amqp.Delivery
}

// NewDelivery creates message based on an amqp.Delivery
func NewDelivery(d *amqp.Delivery) *Delivery {
	message := &Delivery{dlv: d}
	return message
}

// Delivery returns underlying amqp.Delivery
func (d *Delivery) Delivery() *amqp.Delivery {
	return d.dlv
}

// Body returns original Delivery body
func (d *Delivery) Body() []byte {
	return d.dlv.Body
}

// Ack confirms message processing for server
func (d *Delivery) Ack() {
	d.dlv.Ack(false)
}

// Requeue sendes message go back to queue
func (d *Delivery) Requeue() {
	d.dlv.Nack(false, true)
}

// DeadLetter removes or send to dead-letter
func (d *Delivery) DeadLetter() {
	d.dlv.Nack(false, false)
}
