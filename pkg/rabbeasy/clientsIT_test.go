// +build test integration

package rabbeasy

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPublish(t *testing.T) {
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

	config = ConnectionConfig{
		Environment: "integration-test",
		Domain:      "clients",
		Name:        t.Name(),
		Host:        cfg.Host,
		Port:        cfg.Port,
		User:        cfg.User,
		Password:    cfg.Password,
	}
	conn = config.GetConnectionName()

	logger = loggerMockInfo{
		loggerf: t.Logf,
	}

	c, err = NewConnection(ConnectionParameter{
		Config: config,
		Logger: logger,
	})
	defer c.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c)

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

	publisher, err := NewPublisher(PublisherParameter{
		Config: PublisherConfig{
			DestinationConfig: configChannel,
		},
		Connection: c,
	})
	assert.Nil(t, err)
	assert.NotNil(t, publisher)

	err = publisher.Publish([]byte("Msg teste envio 1"))
	assert.Nil(t, err)

}

func TestConsume(t *testing.T) {
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

	expect := "Message to validate " + time.Now().String()

	config = ConnectionConfig{
		Environment: "integration-test",
		Domain:      "clients",
		Name:        t.Name(),
		Host:        cfg.Host,
		Port:        cfg.Port,
		User:        cfg.User,
		Password:    cfg.Password,
	}
	conn = config.GetConnectionName()

	logger = loggerMockInfo{
		loggerf: t.Logf,
	}

	api := &apiHelper{
		logger: logger,
		url:    cfg.MngtAPIUrl,
		delay:  10 * time.Second,
	}

	c, err = NewConnection(ConnectionParameter{
		Config: config,
		Logger: logger,
	})
	defer c.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c)

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

	NewConsumer(ConsumerParameter{
		Config: ConsumerConfig{
			DestinationConfig: configChannel,
		},
		Connection: c,
		Handler:    handler,
	})

	pm := PublishMessageAPIType{
		Vhost:        "%2F", //  "/" html encoded
		ExchangeName: "amq.default",
		Properties: MessagePropertiesAPIType{
			DeliveryMode: 2,
		},
		RoutingKey:      configChannel.Queue,
		DeliveryMode:    "2",
		Payload:         expect,
		PayloadEncoding: "string",
	}
	api.publishMessage(pm)

	deliveryMessage := <-mch
	defer deliveryMessage.Ack()

	b := deliveryMessage.Body()
	message := string(b)
	amqpDelivery := deliveryMessage.Delivery()

	assert.Equal(t, expect, message)
	assert.Equal(t, "", amqpDelivery.Exchange)
	assert.Equal(t, pm.RoutingKey, amqpDelivery.RoutingKey)
	assert.Equal(t, pm.Properties.DeliveryMode, int(amqpDelivery.DeliveryMode))

}
