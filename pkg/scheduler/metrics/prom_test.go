package metrics

import (
	"fmt"
	"testing"
)

func TestGetPromData(t *testing.T) {
	promClient, err := NewPromClient("http://60.245.211.161:31178")
	if err != nil {
		fmt.Printf("new prom client failed:%v", err)
		return
	}
	requestPromDemo, err := promClient.RequestPromDemo()
	if err != nil {
		fmt.Printf("Request Prom Demo failed:%v", err)
		return
	}

	for key, value := range requestPromDemo {
		fmt.Printf("key:%v   value:%v\n", key, value)
	}
}
