package metrics

import (
	"fmt"
	"testing"
	schedulerapi "volcano.sh/volcano/pkg/scheduler/api"
)

func TestGetPromData(t *testing.T) {
	promClient, err := NewPromClient("http://60.245.211.161:32556")
	if err != nil {
		fmt.Printf("new prom client failed:%v", err)
		return
	}
	requestPromDemo, err := promClient.RequestPromMemUsage()
	if err != nil {
		fmt.Printf("Request Prom Demo failed:%v", err)
		return
	}

	for key, value := range requestPromDemo {
		fmt.Printf("key:%v   value:%v\n", key, value)
	}
}

func TestGetScore(t *testing.T) {
	var jobId schedulerapi.JobID = "test"
	Score("k8smaster1", jobId)
	for id, value := range cacheMap {
		for k, v := range value {
			fmt.Printf("id:%v  key:%v  value:%v\n", id, k, v)
		}
	}
}
