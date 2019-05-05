// +build test unit

package rabbeasy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetUrlConnection(t *testing.T) {

	con1 := "host1"
	con2 := "host2"
	exp := map[string]string{
		con1: "amqp://guest:guest@host1:123/",
		con2: "amqp://admin:admin@host2:5672/",
	}

	var url string
	var info ConnectionConfig
	info = ConnectionConfig{
		Environment: "test",
		Domain:      "unit",
		Name:        "url",
		Host:        con1,
		Port:        123,
		User:        "guest",
		Password:    "guest",
	}
	url = info.GetURL()
	assert.Equal(t, exp[con1], url)

	info = ConnectionConfig{
		Environment: "test",
		Domain:      "unit",
		Name:        "url",
		Host:        con2,
		Port:        5672,
		User:        "admin",
		Password:    "admin",
	}
	url = info.GetURL()
	assert.Equal(t, exp[con2], url)

}

func TestGetProperties(t *testing.T) {

	connectionName1 := "test_unit_pub"
	connectionName2 := "test_unit_sub"
	connectionName3 := "test"
	connectionName4 := "unit_sub"
	connectionName5 := "unit"
	connectionName6 := "sub"
	exp1 := map[string]interface{}{
		"connection_name": connectionName1,
	}
	exp2 := map[string]interface{}{
		"connection_name": connectionName2,
	}
	exp3 := map[string]interface{}{
		"connection_name": connectionName3,
	}
	exp4 := map[string]interface{}{
		"connection_name": connectionName4,
	}
	exp5 := map[string]interface{}{
		"connection_name": connectionName5,
	}
	exp6 := map[string]interface{}{
		"connection_name": connectionName6,
	}

	var url map[string]interface{}
	var info ConnectionConfig
	info = ConnectionConfig{
		Environment: "test",
		Domain:      "unit",
		Name:        "pub",
	}
	url = info.GetProperties()
	assert.Equal(t, exp1, url)

	info = ConnectionConfig{
		Environment: "test",
		Domain:      "unit",
		Name:        "sub",
	}
	url = info.GetProperties()
	assert.Equal(t, exp2, url)

	info = ConnectionConfig{
		Environment: "test",
		Domain:      "",
		Name:        "",
	}
	url = info.GetProperties()
	assert.Equal(t, exp3, url)

	info = ConnectionConfig{
		Environment: "",
		Domain:      "unit",
		Name:        "sub",
	}
	url = info.GetProperties()
	assert.Equal(t, exp4, url)

	info = ConnectionConfig{
		Environment: "",
		Domain:      "unit",
		Name:        "",
	}
	url = info.GetProperties()
	assert.Equal(t, exp5, url)

	info = ConnectionConfig{
		Environment: "",
		Domain:      "",
		Name:        "sub",
	}
	url = info.GetProperties()
	assert.Equal(t, exp6, url)

}
