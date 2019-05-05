// +build test integration

package rabbeasy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	rabbitHost       = "RABBIT_HOST"
	rabbitPort       = "RABBIT_PORT"
	rabbitUser       = "RABBIT_USER"
	rabbitPassword   = "RABBIT_PASSWORD"
	rabbitMngtPort   = "RABBIT_MANAGEMENT_PORT"
	rabbitMngtAPI    = "RABBIT_MANAGEMENT_API"
	rabbitMngtAPIUrl = "RABBIT_MANAGEMENT_API_URL"
)

type testConfig struct {
	Host       string
	Port       int
	User       string
	Password   string
	MngtPort   int
	MngtAPI    string
	MngtAPIUrl string //Default - http://localhost:15673/api
}

func newTestConfig() (t testConfig) {

	t.Host = os.Getenv(rabbitHost)
	t.Port, _ = strconv.Atoi(os.Getenv(rabbitPort))
	t.User = os.Getenv(rabbitUser)
	t.Password = os.Getenv(rabbitPassword)
	t.MngtPort, _ = strconv.Atoi(os.Getenv(rabbitMngtPort))
	t.MngtAPI = os.Getenv(rabbitMngtAPI)
	t.MngtAPIUrl = os.Getenv(rabbitMngtAPIUrl)

	if t.Host == "" {
		t.Host = "localhost"
	}
	if t.Port < 1 {
		t.Port = 5673
	}
	if t.User == "" {
		t.User = "guest"
	}
	if t.Password == "" {
		t.Password = "guest"
	}
	if t.MngtPort < 1 {
		t.MngtPort = 15673
	}
	if t.MngtAPI == "" {
		t.MngtAPI = "api"
	}
	if t.MngtAPIUrl == "" {
		t.MngtAPIUrl = fmt.Sprintf("http://%s:%d/%s", t.Host, t.MngtPort, t.MngtAPI)
	}

	return
}

type apiHelper struct {
	logger       Logger
	url          string
	delay        time.Duration
	setConnNames map[string]interface{}
}

// return all connections opened in rabbitmq via the management API
func (a *apiHelper) getExpectedConnections(expected int) (listConns []ConnectionAPIType, mapConns map[string]ConnectionAPIType, reqErr error) {
	if len(listConns) == expected {
		reqErr = nil
	}
	for i := 3; i > 0; i-- {
		listConns, mapConns, reqErr = a.getConnections()
		if len(listConns) != expected {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if len(listConns) == expected {
		reqErr = nil
	}
	return
}

// return all connections opened in rabbitmq via the management API
func (a *apiHelper) getConnections() (listConns []ConnectionAPIType, mapConns map[string]ConnectionAPIType, reqErr error) {
	for i := 3; i > 0; i-- {
		client := &http.Client{}
		url := fmt.Sprintf("%s/connections", a.url)

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			reqErr = err
			return nil, nil, err
		}
		req.SetBasicAuth("guest", "guest")
		resp, err := client.Do(req)
		if err != nil {
			reqErr = err
			return nil, nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusInternalServerError {
			time.Sleep(time.Second)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			reqErr = fmt.Errorf("Non 200 response: %s", resp.Status)
			return
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			reqErr = err
			return
		}

		lc := make([]ConnectionAPIType, 0)
		listConnections := make([]ConnectionAPIType, 0)
		mapConnections := make(map[string]ConnectionAPIType)
		err = json.Unmarshal(b, &lc)
		if err != nil {
			return nil, nil, err
		}

		for _, cn := range lc {
			if _, ok := a.setConnNames[cn.ClientProperties.ConnectionName]; ok {
				listConnections = append(listConnections, cn)
				mapConnections[cn.ClientProperties.ConnectionName] = cn
			}
		}
		listConns = listConnections
		mapConns = mapConnections
	}
	return
}

// close one connection
func (a *apiHelper) killConnection(connection ConnectionAPIType) error {
	client := &http.Client{}
	url := fmt.Sprintf("%s/connections/%s", a.url, connection.Name)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth("guest", "guest")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("deletion of connection failed, status: %s", resp.Status)
	}
	return nil
}

// close all open connections to the rabbitmq via the management api
func (a *apiHelper) killConnections(t *testing.T) {
	conns := make(chan []ConnectionAPIType)
	go func() {
		for {
			connections, _, err := a.getConnections()
			if err != nil {
				t.Error(err)
			}
			if len(connections) >= 1 {
				conns <- connections
				break //exit the loop
			}
			//the rabbitmq api is a bit slow to update, we have to wait a bit
			time.Sleep(time.Second)
		}
	}()

	select {
	case connections := <-conns:
		for _, c := range connections {
			t.Log(c.Name)
			if err := a.killConnection(c); err != nil {
				t.Errorf("impossible to kill connection (%s): %s", c.Name, err)
			}
		}
	case <-time.After(time.Second * 10):
		t.Error("timeout for killing connection reached")
	}
}

func (a *apiHelper) publishMessage(m PublishMessageAPIType) (err error) {
	client := &http.Client{}
	url := fmt.Sprintf("%s/exchanges/%s/%s/publish", a.url, m.Vhost, m.ExchangeName)

	b, err := json.Marshal(m)
	if err != nil {
		return
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(b))
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("guest", "guest")

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("Non 200 response: %s", resp.Status)
		return
	}

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var response PublishMessageResponseAPIType
	err = json.Unmarshal(b, &response)
	if err != nil {
		return
	}

	if !response.Routed {
		err = fmt.Errorf("Message was not routed")
		return
	}

	return

}

type PublishMessageResponseAPIType struct {
	Routed bool `json:"routed"`
}

type PublishMessageAPIType struct {
	Vhost           string                   `json:"vhost"`
	ExchangeName    string                   `json:"name"`
	Properties      MessagePropertiesAPIType `json:"properties"`
	RoutingKey      string                   `json:"routing_key"`
	DeliveryMode    string                   `json:"delivery_mode"`
	Payload         string                   `json:"payload"`
	PayloadEncoding string                   `json:"payload_encoding"`
}

type MessagePropertiesAPIType struct {
	DeliveryMode int `json:"delivery_mode"`
}

type ClientPropertiesAPIType struct {
	ConnectionName string `json:"connection_name"`
}

type ConnectionAPIType struct {
	Name             string                  `json:"name"`
	ClientProperties ClientPropertiesAPIType `json:"client_properties"`
}

type loggerMockInfo struct {
	loggerf func(msg string, v ...interface{})
}

func (l loggerMockInfo) Errorf(msg string, v ...interface{}) {
}

func (l loggerMockInfo) Warnf(msg string, v ...interface{}) {
}

func (l loggerMockInfo) Infof(msg string, v ...interface{}) {
	l.loggerf(msg, v...)
}

func (l loggerMockInfo) Debugf(msg string, v ...interface{}) {
}

func (l loggerMockInfo) Tracef(msg string, v ...interface{}) {
}

type loggerMockDebug struct {
	loggerf func(msg string, v ...interface{})
}

func (l loggerMockDebug) Errorf(msg string, v ...interface{}) {
}

func (l loggerMockDebug) Warnf(msg string, v ...interface{}) {
}

func (l loggerMockDebug) Infof(msg string, v ...interface{}) {
	l.loggerf(msg, v...)
}

func (l loggerMockDebug) Debugf(msg string, v ...interface{}) {
	l.loggerf(msg, v...)
}

func (l loggerMockDebug) Tracef(msg string, v ...interface{}) {
}

type loggerMockTrace struct {
	loggerf func(msg string, v ...interface{})
}

func (l loggerMockTrace) Errorf(msg string, v ...interface{}) {
}

func (l loggerMockTrace) Warnf(msg string, v ...interface{}) {
}

func (l loggerMockTrace) Infof(msg string, v ...interface{}) {
	l.loggerf(msg, v...)
}

func (l loggerMockTrace) Debugf(msg string, v ...interface{}) {
	l.loggerf(msg, v...)
}

func (l loggerMockTrace) Tracef(msg string, v ...interface{}) {
	l.loggerf(msg, v...)
}
