package rabbeasy

import (
	"github.com/streadway/amqp"
	"strconv"
	"strings"
	"time"
)

// GlobalConsumerConfig holds connection global consumer configs
type GlobalConsumerConfig struct {
	PrefetchCount int
	PrefetchSize  int
}

// ConnectionConfig holds connection settings
type ConnectionConfig struct {
	Environment       string
	Domain            string
	Name              string
	Number            int
	Host              string
	Port              int
	User              string
	Password          string
	ReconnectInterval time.Duration
	GlobalConsumerConfig
}

// GetURL produces url string
func (c ConnectionConfig) GetURL() (u string) {
	url := "amqp://" + c.User + ":" + c.Password + "@" + c.Host + ":" + strconv.Itoa(c.Port) + "/"
	u = url
	return
}

// GetProperties produces properties table
func (c ConnectionConfig) GetProperties() (p map[string]interface{}) {
	connectionName := c.GetConnectionName()
	var props map[string]interface{}
	if len(connectionName) > 2 {
		props = map[string]interface{}{
			"connection_name": connectionName,
		}
	}
	p = props
	return
}

// GetConnectionName produces connection name
func (c ConnectionConfig) GetConnectionName() (cn string) {
	connectionNameProps := []string{
		c.Environment,
		c.Domain,
		c.Name,
	}
	connectionName := strings.Join(connectionNameProps, "_")
	if c.Number > 0 {
		connectionName += "[" + strconv.Itoa(c.Number) + "]"
	}
	connectionName = strings.ReplaceAll(connectionName, "__", "_")
	connectionName = strings.Trim(connectionName, "_")
	cn = connectionName
	return
}

// DestinationConfig holds exchange destination configs
type DestinationConfig struct {
	// ConsumerConfig
	// PublisherConfig
	Queue           string
	Exchange        string
	ExchangeType    string
	RoutingKey      string
	DlxExchange     string
	DlxExchangeType string
	DlxRoutingKey   string
	Durable         bool
}

// ConsumerConfig holds consumer configs
type ConsumerConfig struct {
	DestinationConfig
	PrefetchCount int
	PrefetchSize  int
}

// PublisherConfig holds message publishing configs
type PublisherConfig struct {
	DestinationConfig
	Default amqp.Publishing
}
