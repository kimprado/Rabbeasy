package rabbeasy

import (
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// ConsumerConnection defines consumer connection interface
type ConsumerConnection interface {
	StartConsumer(ConsumerClient) error
}

// NotifiableConsumerConnection defines consumer connection interface
type NotifiableConsumerConnection interface {
	StartNotifiableConsumer(ConsumerClientNotifiable) error
}

// PublisherConnection defines publisher connection interface
type PublisherConnection interface {
	StartPublisher(PublisherClient) error
}

// NotifiablePublisherConnection defines publisher connection interface
type NotifiablePublisherConnection interface {
	StartNotifiablePublisher(PublisherClientNotifiable) error
}

// CloserConnection defines closeable connection interface
type CloserConnection interface {
	Close() error
}

// Connection defines pub/sub connection interface
//
// Should not be used in production
type amqpConnection interface {
	ConsumerConnection
	PublisherConnection
}

// Connection manages amqp connection
type Connection struct {
	locker                     sync.Mutex
	closed                     chan interface{}
	onConnect                  chan interface{}
	onDisconnect               chan *amqp.Error
	config                     ConnectionConfig
	conn                       *amqp.Connection
	ConsumerClients            []ConsumerClient
	PublisherClients           []PublisherClient
	ConsumerClientNotifiables  []ConsumerClientNotifiable
	PublisherClientNotifiables []PublisherClientNotifiable
	exchanges                  exchangesConfig
	logger                     Logger
}

// ConnectionParameter holds creation parameters
type ConnectionParameter struct {
	Config       ConnectionConfig
	Logger       Logger
	OnConnect    chan interface{}
	OnDisconnect chan *amqp.Error
}

// NewConnection creates Connection and its underlying *amqp.Connection
//
// Initialises underlying *amqp.Connection and reconnection handle process
func NewConnection(param ConnectionParameter) (c *Connection, err error) {
	c = new(Connection)
	c.config = param.Config
	c.logger = param.Logger
	if param.OnConnect != nil {
		c.onConnect = param.OnConnect
	}
	if param.OnDisconnect != nil {
		c.onDisconnect = param.OnDisconnect
	}
	c.closed = make(chan interface{})
	c.conn, err = c.connect()
	if err != nil {
		return
	}
	c.exchanges = exchangesConfig{
		logger: c.logger,
	}
	url := c.config.GetURL()
	c.logger.Infof("Connected %q\n", url)
	c.startCloseListener()
	return
}

func (c *Connection) lock() {
	c.locker.Lock()
}

func (c *Connection) unlock() {
	c.locker.Unlock()
}

func (c *Connection) connect() (conn *amqp.Connection, err error) {
	c.logger.Debugf("Connecting\n")
	url := c.config.GetURL()
	props := c.config.GetProperties()
	conn, err = amqp.DialConfig(url, amqp.Config{Properties: props})
	if err != nil {
		return
	}
	if c.onConnect != nil {
		select {
		case c.onConnect <- struct{}{}:
		default:
		}
	}
	return
}

// Close closes amqp connection
func (c *Connection) Close() (err error) {
	c.lock()
	defer c.unlock()
	if c.closed == nil {
		return
	}
	c.closed <- true
	close(c.closed)
	return c.disconnect()
}

func (c *Connection) disconnect() (err error) {
	if c.conn != nil {
		err = c.conn.Close()
		if err != nil {
			return
		}
	}
	return
}

func (c *Connection) forceDisconnect() (es []error) {
	errors := []error{}
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			errors = append(errors, err)
			return
		}
	}
	if len(errors) > 0 {
		es = errors
	}
	return
}

// StartConsumer creates amqp.Channel and assigns to consumer parameter
func (c *Connection) StartConsumer(consumer ConsumerClient) (err error) {
	c.lock()
	defer c.unlock()
	if c.closed == nil {
		return
	}
	err = c.startConsumer(consumer)
	if err != nil {
		return
	}
	c.ConsumerClients = append(c.ConsumerClients, consumer)
	return
}

func (c *Connection) startConsumer(consumer ConsumerClient) (err error) {
	amqpCh, err := c.openConsumerChannel(consumer.Destination())
	if err != nil {
		return
	}
	var messages <-chan amqp.Delivery
	err = c.exchanges.configureDestinations(amqpCh, consumer.Destination().DestinationConfig)
	if err != nil {
		return
	}
	messages, err = c.exchanges.consumeWithDestinations(amqpCh, consumer.Destination().DestinationConfig)
	if err != nil {
		return
	}
	consumer.From(messages)
	return
}

// StartNotifiableConsumer creates amqp.Channel and assigns to notifiableConsumer parameter
func (c *Connection) StartNotifiableConsumer(consumer ConsumerClientNotifiable) (err error) {
	c.lock()
	defer c.unlock()
	if c.closed == nil {
		return
	}
	err = c.startNotifiableConsumer(consumer)
	if err != nil {
		return
	}
	c.ConsumerClientNotifiables = append(c.ConsumerClientNotifiables, consumer)
	return
}

func (c *Connection) startNotifiableConsumer(consumer ConsumerClientNotifiable) (err error) {
	amqpCh, err := c.openConsumerChannel(consumer.Destination())
	if err != nil {
		return
	}
	var messages <-chan amqp.Delivery
	err = c.exchanges.configureDestinations(amqpCh, consumer.Destination().DestinationConfig)
	if err != nil {
		return
	}
	messages, err = c.exchanges.consumeWithDestinations(amqpCh, consumer.Destination().DestinationConfig)
	if err != nil {
		return
	}
	channelClose := make(chan *amqp.Error)
	go func() {
		if c.closed == nil {
			return
		}
		for e := range channelClose {
			consumer.NotifyClose(e)
		}
	}()
	amqpCh.NotifyClose(channelClose)
	consumer.From(messages)
	return
}

// StartPublisher creates amqp.Channel and assigns to publisher parameter
func (c *Connection) StartPublisher(publisher PublisherClient) (err error) {
	c.lock()
	defer c.unlock()
	if c.closed == nil {
		return
	}
	err = c.startPublisher(publisher)
	if err != nil {
		return
	}
	c.PublisherClients = append(c.PublisherClients, publisher)
	return
}

func (c *Connection) startPublisher(publisher PublisherClient) (err error) {
	amqpCh, err := c.openPublisherChannel(publisher.Destination())
	if err != nil {
		return
	}
	err = c.exchanges.configureDestinations(amqpCh, publisher.Destination().DestinationConfig)
	if err != nil {
		return
	}
	publisher.PublishIn(amqpCh)
	return
}

// StartNotifiablePublisher creates amqp.Channel and assigns to notifiablePublisher parameter
func (c *Connection) StartNotifiablePublisher(publisher PublisherClientNotifiable) (err error) {
	c.lock()
	defer c.unlock()
	if c.closed == nil {
		return
	}
	err = c.startNotifiablePublisher(publisher)
	if err != nil {
		return
	}
	c.PublisherClientNotifiables = append(c.PublisherClientNotifiables, publisher)
	return
}

func (c *Connection) startNotifiablePublisher(publisher PublisherClientNotifiable) (err error) {
	amqpCh, err := c.openPublisherChannel(publisher.Destination())
	if err != nil {
		return
	}
	err = c.exchanges.configureDestinations(amqpCh, publisher.Destination().DestinationConfig)
	if err != nil {
		return
	}
	channelClose := make(chan *amqp.Error)
	go func() {
		if c.closed == nil {
			return
		}
		for e := range channelClose {
			publisher.NotifyClose(e)
		}
	}()
	amqpCh.NotifyClose(channelClose)
	publisher.PublishIn(amqpCh)
	return
}

func (c *Connection) reconnectClients() (err error) {
	for _, consumer := range c.ConsumerClients {
		err = c.startConsumer(consumer)
		if err != nil {
			return
		}
	}
	for _, consumer := range c.ConsumerClientNotifiables {
		err = c.startNotifiableConsumer(consumer)
		if err != nil {
			return
		}
	}
	for _, publisher := range c.PublisherClients {
		err = c.startPublisher(publisher)
		if err != nil {
			return
		}
	}
	for _, publisher := range c.PublisherClientNotifiables {
		err = c.startNotifiablePublisher(publisher)
		if err != nil {
			return
		}
	}
	return
}

func (c *Connection) openPublisherChannel(config PublisherConfig) (amqpCh *amqp.Channel, err error) {
	amqpCh, err = c.conn.Channel()
	if err != nil {
		return
	}
	err = amqpCh.Qos(
		0,
		0,
		false,
	)
	if err != nil {
		c.logger.Errorf("Erro ao configurar Qos do canal %s\n", err)
		return
	}
	return
}

func (c *Connection) openConsumerChannel(config ConsumerConfig) (amqpCh *amqp.Channel, err error) {
	amqpCh, err = c.conn.Channel()
	if err != nil {
		return
	}
	var prefetchCount int
	var prefetchSize int
	if config.PrefetchCount > 0 {
		prefetchCount = config.PrefetchCount
	} else if c.config.PrefetchCount > 0 {
		prefetchCount = c.config.PrefetchCount
	}
	if config.PrefetchSize > 0 {
		prefetchSize = config.PrefetchSize
	} else if c.config.PrefetchSize > 0 {
		prefetchSize = c.config.PrefetchSize
	}
	err = amqpCh.Qos(
		prefetchCount,
		prefetchSize,
		false,
	)
	if err != nil {
		c.logger.Errorf("Erro ao configurar Qos do canal %s\n", err)
		return
	}
	return
}

func (c *Connection) startCloseListener() {
	if c.closed == nil {
		return
	}
	channelClose := make(chan *amqp.Error)
	c.conn.NotifyClose(channelClose)
	go func() {
		if c.closed == nil {
			return
		}
		select {
		case m := <-channelClose:
			c.logger.Tracef("Close detected %s\n", m)
			if c.onDisconnect != nil {
				select {
				case c.onDisconnect <- m:
				default:
				}
			}
			c.reconnect(0)
		case <-c.closed:
		}
	}()
}

func (c *Connection) reconnect(n int) {
	if c.closed == nil {
		return
	}
	c.logger.Debugf("Reconnecting [%d]\n", n+1)
	var err error
	time.Sleep(c.config.ReconnectInterval * time.Millisecond)
	err = c.disconnect()
	if err != nil {
		c.forceDisconnect()
	}
	c.conn, err = c.connect()
	if err != nil {
		c.reconnectAgain(n)
		return
	}
	c.startCloseListener()
	err = c.reconnectClients()
	if err != nil {
		c.reconnectAgain(n)
		return
	}
}

func (c *Connection) reconnectAgain(n int) {
	times := n + 1
	go c.reconnect(times)
}
