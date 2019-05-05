// +build test integration

package rabbeasy

import (
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	t.Parallel()

	var (
		logger Logger
		config ConnectionConfig
		cfg    testConfig
	)
	cfg = newTestConfig()

	config = ConnectionConfig{
		Environment: "integration-test",
		Domain:      "connect",
		Name:        t.Name(),
		Host:        cfg.Host,
		Port:        cfg.Port,
		User:        cfg.User,
		Password:    cfg.Password,
	}

	logger = loggerMockInfo{
		loggerf: t.Logf,
	}

	var (
		c   *Connection
		err error
	)

	c, err = NewConnection(ConnectionParameter{
		Config: config,
		Logger: logger,
	})
	defer c.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c)

}

func TestConnectTwice(t *testing.T) {
	t.Parallel()

	var (
		logger Logger
		config ConnectionConfig
		cfg    testConfig
	)
	cfg = newTestConfig()

	config = ConnectionConfig{
		Environment: "integration-test",
		Domain:      "connect",
		Name:        t.Name(),
		Host:        cfg.Host,
		Port:        cfg.Port,
		User:        cfg.User,
		Password:    cfg.Password,
	}

	logger = loggerMockInfo{
		loggerf: t.Logf,
	}

	var (
		c1  *Connection
		c2  *Connection
		err error
	)

	c1, err = NewConnection(ConnectionParameter{
		Config: config,
		Logger: logger,
	})
	defer c1.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c1)

	c2, err = NewConnection(ConnectionParameter{
		Config: config,
		Logger: logger,
	})
	defer c2.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c2)

}

func TestConnectTwiceAndValidate(t *testing.T) {
	t.Parallel()

	var (
		c1  *Connection
		c2  *Connection
		err error

		conn1 string
		conn2 string

		logger Logger

		config1 ConnectionConfig
		config2 ConnectionConfig
		cfg     testConfig
	)
	cfg = newTestConfig()

	logger = loggerMockInfo{
		loggerf: t.Logf,
	}
	config1 = ConnectionConfig{
		Environment:       "integration-test",
		Domain:            "connect",
		Name:              t.Name(),
		Number:            1,
		Host:              cfg.Host,
		Port:              cfg.Port,
		User:              cfg.User,
		Password:          cfg.Password,
		ReconnectInterval: 12 * 1000,
	}
	config2 = ConnectionConfig{
		Environment:       "integration-test",
		Domain:            "connect",
		Name:              t.Name(),
		Number:            2,
		Host:              cfg.Host,
		Port:              cfg.Port,
		User:              cfg.User,
		Password:          cfg.Password,
		ReconnectInterval: 12 * 1000,
	}
	conn1 = config1.GetConnectionName()
	conn2 = config2.GetConnectionName()

	api := &apiHelper{
		logger: logger,
		url:    cfg.MngtAPIUrl,
		delay:  10 * time.Second,
		setConnNames: map[string]interface{}{
			conn1: struct{}{},
			conn2: struct{}{},
		},
	}

	apiConnections, apiConnectionsMap, err := api.getConnections()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(apiConnections), "Must not exists connections")

	c1, err = NewConnection(ConnectionParameter{
		Config: config1,
		Logger: logger,
	})
	defer c1.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c1)

	c2, err = NewConnection(ConnectionParameter{
		Config: config2,
		Logger: logger,
	})
	defer c2.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c2)

	time.Sleep(api.delay)

	apiConnections, apiConnectionsMap, err = api.getExpectedConnections(2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(apiConnections), "Must exists connections")

	if k1, ok := apiConnectionsMap[conn1]; ok {
		api.killConnection(k1)
	}

	if k2, ok := apiConnectionsMap[conn2]; ok {
		api.killConnection(k2)
	}

	time.Sleep(api.delay)
	apiConnections, apiConnectionsMap, err = api.getConnections()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(apiConnections), "Must not exists connections")

	time.Sleep((config1.ReconnectInterval) + (api.delay))
	apiConnections, apiConnectionsMap, err = api.getExpectedConnections(2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(apiConnections), "Must exists connections")

}

func TestNotifyReconnect(t *testing.T) {
	t.Parallel()

	var (
		c            *Connection
		err          error
		conn         string
		logger       Logger
		config       ConnectionConfig
		cfg          testConfig
		onConnect    chan interface{}
		onDisconnect chan *amqp.Error
		connCh       chan interface{}
		discCh       chan *amqp.Error
	)
	cfg = newTestConfig()
	onConnect = make(chan interface{})
	onDisconnect = make(chan *amqp.Error)
	connCh = make(chan interface{})
	discCh = make(chan *amqp.Error)

	go func() {
		var c interface{}
		c = <-onConnect
		connCh <- c
	}()
	go func() {
		var e *amqp.Error
		e = <-onDisconnect
		discCh <- e
	}()

	logger = loggerMockInfo{
		loggerf: t.Logf,
	}
	config = ConnectionConfig{
		Environment:       "integration-test",
		Domain:            "connect",
		Name:              t.Name(),
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
		Config:       config,
		Logger:       logger,
		OnConnect:    onConnect,
		OnDisconnect: onDisconnect,
	})
	defer c.Close()

	assert.Nil(t, err)
	assert.NotNil(t, c)

	time.Sleep(api.delay)

	apiConnections, apiConnectionsMap, err = api.getExpectedConnections(2)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(apiConnections), "Must exists connections")

	if k, ok := apiConnectionsMap[conn]; ok {
		api.killConnection(k)
	}

	time.Sleep(api.delay)
	apiConnections, apiConnectionsMap, err = api.getConnections()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(apiConnections), "Must not exists connections")

	time.Sleep((config.ReconnectInterval) + (api.delay))
	apiConnections, apiConnectionsMap, err = api.getExpectedConnections(2)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(apiConnections), "Must exists connections")

	select {
	case <-connCh:
	case <-time.After(1 * time.Second):
		t.Error("Connect message was not delivered")
	}

	select {
	case <-discCh:
	case <-time.After(1 * time.Second):
		t.Error("Disconnect message was not delivered")
	}
}
