package rabbeasy

import (
	"github.com/streadway/amqp"
)

// ClientNotifiable notifies channel status
type ClientNotifiable interface {
	NotifyClose(e *amqp.Error)
}

// MessagePublisher publishes messages on the channel
type MessagePublisher interface {
	Publish(body []byte) error
}

// PublisherClientNotifiable publishes messages on the channel
//
// Also receives channel status
type PublisherClientNotifiable interface {
	PublisherClient
	ClientNotifiable
}

// PublisherClient publishes messages on the channel
type PublisherClient interface {
	PublishIn(*amqp.Channel)
	Destination() PublisherConfig
}

// MessageConsumer processes channel messages
type MessageConsumer interface {
	Consume(Handler)
}

// ConsumerClientNotifiable processes channel messages
//
//Also receives channel status
type ConsumerClientNotifiable interface {
	ConsumerClient
	ClientNotifiable
}

// ConsumerClient processes channel messages
type ConsumerClient interface {
	From(<-chan amqp.Delivery)
	Destination() ConsumerConfig
}

// Consumer implements messaging consumer
type Consumer struct {
	config     ConsumerConfig
	connection ConsumerConnection
	handler    Handler
}

// ConsumerParameter holds creation parameters
type ConsumerParameter struct {
	Config     ConsumerConfig
	Connection ConsumerConnection
	Handler    Handler
}

// NotifiableConsumer implements messaging notifiable consumer
type NotifiableConsumer struct {
	Consumer
	connection NotifiableConsumerConnection
	receiver   chan *amqp.Error
}

// NotifiableConsumerParameter holds creation parameters
type NotifiableConsumerParameter struct {
	config     ConsumerConfig
	connection NotifiableConsumerConnection
	handler    Handler
	receiver   chan *amqp.Error
}

// NewConsumer creates messaging consumer
func NewConsumer(param ConsumerParameter) (c *Consumer, err error) {
	consumer := new(Consumer)
	consumer.config = param.Config
	consumer.connection = param.Connection
	consumer.handler = param.Handler
	err = consumer.connection.StartConsumer(consumer)
	if err != nil {
		return
	}
	c = consumer
	return
}

// NewNotifiableConsumer creates messaging notifiable consumer
func NewNotifiableConsumer(param NotifiableConsumerParameter) (c *NotifiableConsumer, err error) {
	consumer := new(NotifiableConsumer)
	consumer.config = param.config
	consumer.connection = param.connection
	consumer.handler = param.handler
	consumer.receiver = param.receiver
	err = consumer.connection.StartNotifiableConsumer(consumer)
	if err != nil {
		return
	}
	c = consumer
	return
}

// Publisher implements messaging publisher
type Publisher struct {
	config     PublisherConfig
	connection PublisherConnection
	amqpCh     *amqp.Channel
}

// PublisherParameter holds creation parameters
type PublisherParameter struct {
	Config     PublisherConfig
	Connection PublisherConnection
}

// NotifiablePublisher implements messaging notifiable publisher
type NotifiablePublisher struct {
	Publisher
	connection NotifiablePublisherConnection
	receiver   chan *amqp.Error
}

// NotifiablePublisherParameter holds creation parameters
type NotifiablePublisherParameter struct {
	Config     PublisherConfig
	Connection NotifiablePublisherConnection
	Receiver   chan *amqp.Error
}

// NewPublisher creates messaging Publisher
func NewPublisher(param PublisherParameter) (p *Publisher, err error) {
	publisher := new(Publisher)

	var (
		contentType      string
		deliveryMode     uint8
		defaulPublishing amqp.Publishing
	)
	if param.Config.Default.ContentType == "" {
		contentType = "application/json"
	} else {
		contentType = param.Config.Default.ContentType
	}
	if param.Config.Default.DeliveryMode != 2 && param.Config.Default.DeliveryMode != 1 {
		deliveryMode = amqp.Persistent
	} else {
		deliveryMode = param.Config.Default.DeliveryMode
	}
	defaulPublishing = amqp.Publishing{
		Headers:         param.Config.Default.Headers,
		ContentType:     contentType,
		ContentEncoding: param.Config.Default.ContentEncoding,
		DeliveryMode:    deliveryMode,
		Priority:        param.Config.Default.Priority,
		CorrelationId:   param.Config.Default.CorrelationId,
		ReplyTo:         param.Config.Default.ReplyTo,
		Expiration:      param.Config.Default.Expiration,
		MessageId:       param.Config.Default.MessageId,
		Timestamp:       param.Config.Default.Timestamp,
		Type:            param.Config.Default.Type,
		UserId:          param.Config.Default.UserId,
		AppId:           param.Config.Default.AppId,
	}
	publisher.config = param.Config
	publisher.config.Default = defaulPublishing
	publisher.connection = param.Connection
	err = publisher.connection.StartPublisher(publisher)
	if err != nil {
		return
	}
	p = publisher
	return
}

// NewNotifiablePublisher creates messaging notifiable publisher
func NewNotifiablePublisher(param NotifiablePublisherParameter) (p *NotifiablePublisher, err error) {
	publisher := new(NotifiablePublisher)

	var (
		contentType      string
		deliveryMode     uint8
		defaulPublishing amqp.Publishing
	)
	if param.Config.Default.ContentType == "" {
		contentType = "application/json"
	} else {
		contentType = param.Config.Default.ContentType
	}
	if param.Config.Default.DeliveryMode != 2 && param.Config.Default.DeliveryMode != 1 {
		deliveryMode = amqp.Persistent
	} else {
		deliveryMode = param.Config.Default.DeliveryMode
	}
	defaulPublishing = amqp.Publishing{
		Headers:         param.Config.Default.Headers,
		ContentType:     contentType,
		ContentEncoding: param.Config.Default.ContentEncoding,
		DeliveryMode:    deliveryMode,
		Priority:        param.Config.Default.Priority,
		CorrelationId:   param.Config.Default.CorrelationId,
		ReplyTo:         param.Config.Default.ReplyTo,
		Expiration:      param.Config.Default.Expiration,
		MessageId:       param.Config.Default.MessageId,
		Timestamp:       param.Config.Default.Timestamp,
		Type:            param.Config.Default.Type,
		UserId:          param.Config.Default.UserId,
		AppId:           param.Config.Default.AppId,
	}
	publisher.config = param.Config
	publisher.config.Default = defaulPublishing
	publisher.connection = param.Connection
	publisher.receiver = param.Receiver
	err = publisher.connection.StartNotifiablePublisher(publisher)
	if err != nil {
		return
	}
	p = publisher
	return
}

// Destination returns destination config configuration
func (p *Publisher) Destination() (d PublisherConfig) {
	d = p.config
	return
}

// PublishIn connects publisher to amqp.Channel
func (p *Publisher) PublishIn(amqpCh *amqp.Channel) {
	p.amqpCh = amqpCh
	return
}

// NotifyClose handles channel closure event
//
// Forwards message in amqp.Error form to listener receiver
func (p *NotifiablePublisher) NotifyClose(e *amqp.Error) {
	p.receiver <- e
}

// Handler called when a message is received
//
// Although you can implement business rules directly in the Handler, it
// is advisable to delegate the treatment to another function or channel.
type Handler func(d Message)

// From connects consumer to new message listener channel
func (c *Consumer) From(messages <-chan amqp.Delivery) {
	go func() {
		for d := range messages {
			c.handler(NewDelivery(&d))
		}
	}()
}

// Destination returns destination config configuration
func (c *Consumer) Destination() (d ConsumerConfig) {
	d = c.config
	return
}

// NotifyClose handles channel closure event
//
// Forwards message in amqp.Error form to listener receiver
func (c *NotifiableConsumer) NotifyClose(e *amqp.Error) {
	c.receiver <- e
}

// Publish publishes message in queue or topic
//
// Message are sent to destination defined at publisher creation
func (p *Publisher) Publish(body []byte) (err error) {
	err = p.amqpCh.Publish(
		p.config.Exchange,   // publish to an exchange
		p.config.RoutingKey, // routing to 0 or more queues
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			Headers:         p.config.Default.Headers,
			ContentType:     p.config.Default.ContentType,
			ContentEncoding: p.config.Default.ContentEncoding,
			DeliveryMode:    p.config.Default.DeliveryMode,
			Priority:        p.config.Default.Priority,
			CorrelationId:   p.config.Default.CorrelationId,
			ReplyTo:         p.config.Default.ReplyTo,
			Expiration:      p.config.Default.Expiration,
			MessageId:       p.config.Default.MessageId,
			Timestamp:       p.config.Default.Timestamp,
			Type:            p.config.Default.Type,
			UserId:          p.config.Default.UserId,
			AppId:           p.config.Default.AppId,
			Body:            body,
		},
	)
	return
}

// Publish publishes message in queue or topic
//
// Message are sent to destination defined at publisher creation
func (p *NotifiablePublisher) Publish(body []byte) (err error) {
	err = p.Publisher.Publish(body)
	return
}
