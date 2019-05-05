// +build test example !unit,!integration

package rabbeasy_test

import (
	"fmt"

	"github.com/kimprado/rabbeasy/pkg/rabbeasy"
	"github.com/streadway/amqp"
)

func ExampleHandler_basics() {
	var h rabbeasy.Handler // Consumer will invoke handler for each message

	var listenerCh = make(chan rabbeasy.MessageBody)
	h = func(m rabbeasy.Message) {
		listenerCh <- m
	}

	rabbeasy.NewConsumer(rabbeasy.ConsumerParameter{
		Handler: h, // Consumer created with handler h reference
	})

	message := <-listenerCh
	fmt.Println(string(message.Body()))
}

func ExampleHandler_channel() {
	type MySimpleMessageType interface {
		rabbeasy.MessageBody // Declared only what is used ( Body() )
		rabbeasy.Acker       // Declared only what is used ( Ack()  )
		// rabbeasy.Requeuer         // Not declared for non-use
		// rabbeasy.DeadLetterSender // Not declared for non-use
		// rabbeasy.Deliveryer       // Not declared for non-use
	}

	var listenerCh = make(chan MySimpleMessageType)
	var handler = func(m rabbeasy.Message) {
		listenerCh <- m
	}

	go produceMockMassege(handler)

	message := <-listenerCh
	fmt.Println(string(message.Body()))
	message.Ack()

	// Output:
	// hello
}

func produceMockMassege(h rabbeasy.Handler) {
	h(&mockMessage{})
}

type mockMessage struct{}

func (m *mockMessage) Body() []byte {
	return []byte("hello")
}
func (m *mockMessage) Ack()     {}
func (m *mockMessage) Requeue() {}
func (m *mockMessage) Delivery() *amqp.Delivery {
	return nil
}
func (m *mockMessage) DeadLetter() {}

func ExamplePublisher_Publish() {
	var (
		publisher *rabbeasy.Publisher
		param     rabbeasy.PublisherParameter
		conn      rabbeasy.PublisherConnection
		cfg       rabbeasy.PublisherConfig
		err       error
	)

	cfg = rabbeasy.PublisherConfig{
		DestinationConfig: rabbeasy.DestinationConfig{
			RoutingKey: "sample", // Sending to queue 'sample'
		},
		Default: amqp.Publishing{
			ContentType: "text/plain",
		},
	}

	param = rabbeasy.PublisherParameter{
		Config:     cfg,
		Connection: conn,
	}

	publisher, err = rabbeasy.NewPublisher(param)
	if err != nil {
		return
	}

	publisher.Publish([]byte("New message to send"))
}

func ExampleNotifiablePublisher_Publish() {
	var (
		publisher *rabbeasy.NotifiablePublisher
		param     rabbeasy.NotifiablePublisherParameter
		conn      rabbeasy.NotifiablePublisherConnection
		cfg       rabbeasy.PublisherConfig
		eCh       chan *amqp.Error
		err       error
	)

	cfg = rabbeasy.PublisherConfig{
		DestinationConfig: rabbeasy.DestinationConfig{
			RoutingKey: "sample", // Sending to queue 'sample'
		},
		Default: amqp.Publishing{
			ContentType: "text/plain",
		},
	}

	param = rabbeasy.NotifiablePublisherParameter{
		Config:     cfg,
		Connection: conn,
		Receiver:   eCh,
	}

	publisher, err = rabbeasy.NewNotifiablePublisher(param)
	if err != nil {
		return
	}

	publisher.Publish([]byte("New message to send"))
}
