// +build test integration

package rabbeasy

import (
	"fmt"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestValidateNotifiablePublisher(t *testing.T) {
	t.Parallel()

	var (
		c      *Connection
		err    error
		conn   string
		logger Logger
		config ConnectionConfig
		cfg    testConfig
	)
	cfg = newTestConfig()

	logger = loggerMockInfo{
		loggerf: t.Logf,
	}

	config = ConnectionConfig{
		Environment:       "integration-test",
		Domain:            "connect",
		Name:              t.Name(),
		Number:            0,
		Host:              cfg.Host,
		Port:              cfg.Port,
		User:              cfg.User,
		Password:          cfg.Password,
		ReconnectInterval: 12 * 1000,
	}
	conn = config.GetConnectionName()

	api := &apiHelper{
		logger: logger,
		url:    cfg.MngtAPIUrl,
		delay:  10 * time.Second,
		setConnNames: map[string]interface{}{
			conn: struct{}{},
		},
	}

	apiConnections, apiConnectionsMap, err := api.getConnections()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(apiConnections), "Must not exists connections")

	c, err = NewConnection(ConnectionParameter{
		Config: config,
		Logger: logger,
	})
	defer c.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c)

	time.Sleep(api.delay)

	apiConnections, apiConnectionsMap, err = api.getExpectedConnections(1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(apiConnections), "Must exists connections")

	configChannel := DestinationConfig{
		Queue:           fmt.Sprintf("%s-%s", "queue", conn),
		Exchange:        fmt.Sprintf("%s-%s", "ex", conn),
		ExchangeType:    fmt.Sprintf("%s", "topic"),
		RoutingKey:      fmt.Sprintf("%s", "rk"),
		DlxExchange:     fmt.Sprintf("%s-%s", "dlx", conn),
		DlxExchangeType: fmt.Sprintf("%s", "topic"),
		DlxRoutingKey:   fmt.Sprintf("%s", "rk"),
		Durable:         true,
	}
	var ech = make(chan *amqp.Error, 2)
	publisher,err := NewNotifiablePublisher(NotifiablePublisherParameter{
		Config:         PublisherConfig{
			DestinationConfig:configChannel,
		},
		Connection: c,
		Receiver:   ech,
	})
	assert.Nil(t, err)
	assert.NotNil(t, publisher)

	err = publisher.Publish([]byte("Msg teste envio 1"))
	assert.Nil(t, err)

	if k, ok := apiConnectionsMap[conn]; ok {
		api.killConnection(k)
	}

	select {
	case closeError := <-ech:
		assert.NotNil(t, closeError)
	case <-time.After(3 * time.Second):
		t.Error("Notify Close was not delivered.")
		return
	}

	time.Sleep(api.delay)

	err = publisher.Publish([]byte("Msg teste envio 2"))
	assert.NotNil(t, err)

	time.Sleep((config.ReconnectInterval) + (5 * time.Second))

	err = publisher.Publish([]byte("Msg teste envio 3"))
	assert.Nil(t, err)
}

func TestValidateMultipleNotificationPublisher(t *testing.T) {
	t.Parallel()

	var (
		c      *Connection
		err    error
		conn   string
		logger Logger
		config ConnectionConfig
		cfg    testConfig
	)
	cfg = newTestConfig()

	logger = loggerMockInfo{
		loggerf: t.Logf,
	}

	config = ConnectionConfig{
		Environment:       "integration-test",
		Domain:            "connect",
		Name:              t.Name(),
		Number:            0,
		Host:              cfg.Host,
		Port:              cfg.Port,
		User:              cfg.User,
		Password:          cfg.Password,
		ReconnectInterval: 12 * 1000,
	}
	conn = config.GetConnectionName()

	api := &apiHelper{
		logger: logger,
		url:    cfg.MngtAPIUrl,
		delay:  10 * time.Second,
		setConnNames: map[string]interface{}{
			conn: struct{}{},
		},
	}

	apiConnections, apiConnectionsMap, err := api.getConnections()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(apiConnections), "Must not exists connections")

	c, err = NewConnection(ConnectionParameter{
		Config: config,
		Logger: logger,
	})
	defer c.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c)

	time.Sleep(api.delay)

	apiConnections, apiConnectionsMap, err = api.getExpectedConnections(1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(apiConnections), "Must exists connections")

	configChannel := DestinationConfig{
		Queue:           fmt.Sprintf("%s-%s", "queue", conn),
		Exchange:        fmt.Sprintf("%s-%s", "ex", conn),
		ExchangeType:    fmt.Sprintf("%s", "topic"),
		RoutingKey:      fmt.Sprintf("%s", "rk"),
		DlxExchange:     fmt.Sprintf("%s-%s", "dlx", conn),
		DlxExchangeType: fmt.Sprintf("%s", "topic"),
		DlxRoutingKey:   fmt.Sprintf("%s", "rk"),
		Durable:         true,
	}
	var ech = make(chan *amqp.Error, 2)
	publisher,err := NewNotifiablePublisher(NotifiablePublisherParameter{
		Config:         PublisherConfig{
			DestinationConfig:configChannel,
		},
		Connection: c,
		Receiver:   ech,
	})
	assert.Nil(t, err)
	assert.NotNil(t, publisher)

	err = publisher.Publish([]byte("Msg teste envio 1"))
	assert.Nil(t, err)

	if k, ok := apiConnectionsMap[conn]; ok {
		api.killConnection(k)
	}

	select {
	case closeError := <-ech:
		assert.NotNil(t, closeError)
	case <-time.After(3 * time.Second):
		t.Error("Notify Close was not delivered")
		return
	}

	time.Sleep(api.delay)

	err = publisher.Publish([]byte("Msg teste envio 2"))
	assert.NotNil(t, err)

	time.Sleep((config.ReconnectInterval) + (5 * time.Second))

	err = publisher.Publish([]byte("Msg teste envio 3"))
	assert.Nil(t, err)

	apiConnections, apiConnectionsMap, err = api.getExpectedConnections(1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(apiConnections), "Must exists connections")

	if k, ok := apiConnectionsMap[conn]; ok {
		api.killConnection(k)
	}

	select {
	case closeError := <-ech:
		assert.NotNil(t, closeError)
	case <-time.After(3 * time.Second):
		t.Error("Notify Close was not delivered")
		return
	}
}

func TestValidateNotifiableConsumer(t *testing.T) {
	t.Parallel()

	var (
		c      *Connection
		err    error
		conn   string
		logger Logger
		config ConnectionConfig
		cfg    testConfig
	)
	cfg = newTestConfig()

	logger = loggerMockInfo{
		loggerf: t.Logf,
	}

	config = ConnectionConfig{
		Environment:       "integration-test",
		Domain:            "connect",
		Name:              t.Name(),
		Number:            0,
		Host:              cfg.Host,
		Port:              cfg.Port,
		User:              cfg.User,
		Password:          cfg.Password,
		ReconnectInterval: 12 * 1000,
	}
	conn = config.GetConnectionName()

	api := &apiHelper{
		logger: logger,
		url:    cfg.MngtAPIUrl,
		delay:  10 * time.Second,
		setConnNames: map[string]interface{}{
			conn: struct{}{},
		},
	}

	apiConnections, apiConnectionsMap, err := api.getConnections()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(apiConnections), "Must not exists connections")

	c, err = NewConnection(ConnectionParameter{
		Config: config,
		Logger: logger,
	})
	defer c.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c)

	time.Sleep(api.delay)

	apiConnections, apiConnectionsMap, err = api.getExpectedConnections(1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(apiConnections), "Must exists connections")

	configChannel := DestinationConfig{
		Queue:           fmt.Sprintf("%s-%s", "queue", conn),
		Exchange:        fmt.Sprintf("%s-%s", "ex", conn),
		ExchangeType:    fmt.Sprintf("%s", "topic"),
		RoutingKey:      fmt.Sprintf("%s", "rk"),
		DlxExchange:     fmt.Sprintf("%s-%s", "dlx", conn),
		DlxExchangeType: fmt.Sprintf("%s", "topic"),
		DlxRoutingKey:   fmt.Sprintf("%s", "rk"),
		Durable:         true,
	}
	var mch = make(chan Message)
	var handler = func(m Message) {
		mch <- m
	}
	var ech = make(chan *amqp.Error, 2)
	NewNotifiableConsumer(NotifiableConsumerParameter{
		config:         ConsumerConfig{
			DestinationConfig:configChannel,
		},
		connection: c,
		handler:    handler,
		receiver:   ech,
	})

	body := "Message to validate " + time.Now().String()

	pm := PublishMessageAPIType{
		Vhost:        "%2F", //  "/" html encoded
		ExchangeName: "amq.default",
		Properties: MessagePropertiesAPIType{
			DeliveryMode: 2,
		},
		RoutingKey:      configChannel.Queue,
		DeliveryMode:    "2",
		Payload:         body,
		PayloadEncoding: "string",
	}
	go api.publishMessage(pm)

	time.Sleep(5 * time.Second)

	deliveryMessage1 := <-mch
	go deliveryMessage1.Ack()
	b := deliveryMessage1.Body()
	message := string(b)
	assert.Equal(t, body, message)

	if k, ok := apiConnectionsMap[conn]; ok {
		go api.killConnection(k)
		time.Sleep(1 * time.Second)
	}

	body = "Message to validate " + time.Now().String()
	pm = PublishMessageAPIType{
		Vhost:        "%2F", //  "/" html encoded
		ExchangeName: "amq.default",
		Properties: MessagePropertiesAPIType{
			DeliveryMode: 2,
		},
		RoutingKey:      configChannel.Queue,
		DeliveryMode:    "2",
		Payload:         body,
		PayloadEncoding: "string",
	}
	go api.publishMessage(pm)
	time.Sleep(1 * time.Second)

	select {
	case deliveryMessage2 := <-mch:
		defer deliveryMessage2.Ack()
		t.Error("Connection must be closed")
		return
	default:
	}

	select {
	case closeError := <-ech:
		assert.NotNil(t, closeError)
	case <-time.After(3 * time.Second):
		t.Error("Notify Close was not delivered")
		return
	}

	time.Sleep(config.ReconnectInterval * time.Millisecond)

	select {
	case deliveryMessage3 := <-mch:
		defer deliveryMessage3.Ack()
		b := deliveryMessage3.Body()
		message := string(b)
		assert.Equal(t, body, message)
	case <-time.After((config.ReconnectInterval) + (30 * time.Second)):
		t.Error("Message was not delivered")
	}

}

func TestValidateMultipleNotificationConsumer(t *testing.T) {
	t.Parallel()

	var (
		c      *Connection
		err    error
		conn   string
		logger Logger
		config ConnectionConfig
		cfg    testConfig
	)
	cfg = newTestConfig()

	logger = loggerMockInfo{
		loggerf: t.Logf,
	}

	config = ConnectionConfig{
		Environment:       "integration-test",
		Domain:            "connect",
		Name:              t.Name(),
		Number:            0,
		Host:              cfg.Host,
		Port:              cfg.Port,
		User:              cfg.User,
		Password:          cfg.Password,
		ReconnectInterval: 12 * 1000,
	}
	conn = config.GetConnectionName()

	api := &apiHelper{
		logger: logger,
		url:    cfg.MngtAPIUrl,
		delay:  10 * time.Second,
		setConnNames: map[string]interface{}{
			conn: struct{}{},
		},
	}

	apiConnections, apiConnectionsMap, err := api.getConnections()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(apiConnections), "Must not exists connections")

	c, err = NewConnection(ConnectionParameter{
		Config: config,
		Logger: logger,
	})
	defer c.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c)

	time.Sleep(api.delay)

	apiConnections, apiConnectionsMap, err = api.getExpectedConnections(1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(apiConnections), "Must exists connections")

	configChannel := DestinationConfig{
		Queue:           fmt.Sprintf("%s-%s", "queue", conn),
		Exchange:        fmt.Sprintf("%s-%s", "ex", conn),
		ExchangeType:    fmt.Sprintf("%s", "topic"),
		RoutingKey:      fmt.Sprintf("%s", "rk"),
		DlxExchange:     fmt.Sprintf("%s-%s", "dlx", conn),
		DlxExchangeType: fmt.Sprintf("%s", "topic"),
		DlxRoutingKey:   fmt.Sprintf("%s", "rk"),
		Durable:         true,
	}
	var mch = make(chan Message)
	var handler = func(m Message) {
		mch <- m
	}
	var ech = make(chan *amqp.Error, 2)
	NewNotifiableConsumer(NotifiableConsumerParameter{
		config:         ConsumerConfig{
			DestinationConfig:configChannel,
		},
		connection: c,
		handler:    handler,
		receiver:   ech,
	})

	body := "Message to validate " + time.Now().String()

	pm := PublishMessageAPIType{
		Vhost:        "%2F", //  "/" html encoded
		ExchangeName: "amq.default",
		Properties: MessagePropertiesAPIType{
			DeliveryMode: 2,
		},
		RoutingKey:      configChannel.Queue,
		DeliveryMode:    "2",
		Payload:         body,
		PayloadEncoding: "string",
	}
	go api.publishMessage(pm)

	time.Sleep(5 * time.Second)

	deliveryMessage1 := <-mch
	go deliveryMessage1.Ack()
	b := deliveryMessage1.Body()
	message := string(b)
	assert.Equal(t, body, message)

	if k, ok := apiConnectionsMap[conn]; ok {
		go api.killConnection(k)
		time.Sleep(1 * time.Second)
	}

	select {
	case closeError := <-ech:
		assert.NotNil(t, closeError)
	case <-time.After(3 * time.Second):
		t.Error("Notify Close was not delivered")
		return
	}

	body = "Message to validate " + time.Now().String()
	pm = PublishMessageAPIType{
		Vhost:        "%2F", //  "/" html encoded
		ExchangeName: "amq.default",
		Properties: MessagePropertiesAPIType{
			DeliveryMode: 2,
		},
		RoutingKey:      configChannel.Queue,
		DeliveryMode:    "2",
		Payload:         body,
		PayloadEncoding: "string",
	}
	go api.publishMessage(pm)
	time.Sleep(1 * time.Second)

	select {
	case deliveryMessage2 := <-mch:
		defer deliveryMessage2.Ack()
		t.Error("Connection must be closed")
		return
	default:
	}

	time.Sleep(config.ReconnectInterval * time.Millisecond)

	select {
	case deliveryMessage3 := <-mch:
		defer deliveryMessage3.Ack()
		b := deliveryMessage3.Body()
		message := string(b)
		assert.Equal(t, body, message)
	case <-time.After((config.ReconnectInterval) + (30 * time.Second)):
		t.Error("Message was not delivered")
	}

	time.Sleep(api.delay)

	apiConnections, apiConnectionsMap, err = api.getExpectedConnections(1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(apiConnections), "Must exists connections")

	if k, ok := apiConnectionsMap[conn]; ok {
		api.killConnection(k)
	}

	select {
	case closeError := <-ech:
		assert.NotNil(t, closeError)
	case <-time.After(3 * time.Second):
		t.Error("Notify Close was not delivered")
		return
	}

}
