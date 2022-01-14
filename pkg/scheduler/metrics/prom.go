package metrics

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"k8s.io/klog"
	"math"
	"time"
)

type PromClient struct {
	API v1.API
}

func NewPromClient(addr string) (*PromClient, error) {
	//TODO 先写死 addr, user, pass
	//
	client, err := api.NewClient(api.Config{
		Address: addr,
		// We can use amazing github.com/prometheus/common/config helper!
		//RoundTripper: config.NewBasicAuthRoundTripper(user, config.Secret(pass), "", api.DefaultRoundTripper),
	})
	if err != nil {
		klog.Errorf("Error creating client: %v\n", err)
		return nil, err
	}

	v1api := v1.NewAPI(client)
	promDao := new(PromClient)
	promDao.API = v1api

	return promDao, nil
}

// func (promDao *PromClient) ExecPromQL(promQL string) (error, model.Value) {
func (promClient *PromClient) ExecPromQL(promQL string) (error, model.Value) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, warnings, err := promClient.API.Query(ctx, promQL, time.Now())
	if err != nil {
		klog.Errorf("Error querying Prometheus: %v", err)
		return err, nil
	}
	if len(warnings) > 0 {
		klog.Errorf("Warnings: %v", warnings)
	}

	return nil, result
}

func (promClient *PromClient) RequestPromDemo() (map[string]int64, error) {
	// d.promClient.ExecPromQL("up")
	// d.promClient.ExecPromQL(`increase(node_network_receive_bytes_total{device=~"eth0"}[30s])`)
	err, result := promClient.ExecPromQL(`max(irate(node_network_receive_bytes_total[30s])*8/1024) by (job)`)
	if err != nil {
		return nil, nil
	}
	return promClient.parsePromResultInt64(result, 1)
}

func (promClient *PromClient) parsePromResultInt64(result model.Value, base int) (map[string]int64, error) {
	vectorValue, ok := result.(model.Vector)
	if !ok {
		err := fmt.Errorf("type of result not %T, get %T", model.Vector{}, result)
		return nil, err
	}

	resMap := make(map[string]int64)
	for i := 0; i < len(vectorValue); i++ {
		tmp := vectorValue[i]
		tmpv := float64(tmp.Value)
		if base > 1 {
			resMap[string(tmp.Metric["job"])] = int64(math.Round(tmpv * float64(base)))
		} else {
			resMap[string(tmp.Metric["job"])] = int64(math.Round(tmpv))
		}
	}

	return resMap, nil
}

func (promClient *PromClient) parsePromResultFloat64(result model.Value) (map[string]float64, error) {
	vectorValue, ok := result.(model.Vector)
	if !ok {
		err := fmt.Errorf("type of result not %T, get %T", model.Vector{}, result)
		return nil, err
	}

	resMap := make(map[string]float64)
	for i := 0; i < len(vectorValue); i++ {
		tmp := vectorValue[i]
		resMap[string(tmp.Metric["job"])] = float64(tmp.Value)
	}

	return resMap, nil
}

// RequestPromUpNetIO 获取网络上传负载
// 单位 kbit/s
func (promClient *PromClient) RequestPromUpNetIO() (map[string]int64, error) {
	promQL := `max(irate(node_network_transmit_bytes_total[30s])*8/1000) by (job)`

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1)
}

// RequestPromNetIO 获取网络下载负载
func (promClient *PromClient) RequestPromDownNetIO() (map[string]int64, error) {
	promQL := `max(irate(node_network_receive_bytes_total[30s])*8/1000) by (job)`
	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1)
}

// RequestPromMaxNetIO 查询上行/下行中最大网络IO
// 单位 kbit/s
func (promClient *PromClient) RequestPromMaxNetIO() (map[string]int64, error) {
	promQL := `(max(irate(node_network_receive_bytes_total[30s])*8/1000) by (job)) > (max(irate(node_network_transmit_bytes_total[30s])*8/1024) by (job)) or (max(irate(node_network_transmit_bytes_total[30s])*8/1024) by (job))`

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1)
}

// RequestPromWriteDiskIO 查询Prom上机器的写DiskIO
// 单位byte/s 或者 B/s
func (promClient *PromClient) RequestPromWriteDiskIO(diskType string) (map[string]int64, error) {
	promQL := `max(irate(node_disk_written_bytes_total[30s])) by (job)`

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1)
}

// RequestPromWriteDiskIO 查询Prom上机器的读DiskIO
// 单位byte/s 或者 B/s
func (promClient *PromClient) RequestPromReadDiskIO(diskType string) (map[string]int64, error) {
	promQL := `max(irate(node_disk_read_bytes_total[30s])) by (job)`

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1)
}

// RequestPromMaxDiskIO 查询读/写中最大磁盘IO
func (promClient *PromClient) RequestPromMaxDiskIO() (map[string]int64, error) {
	promQL := `(max(irate(node_disk_written_bytes_total[30s])) by (job)) > (max(irate(node_disk_read_bytes_total[30s])) by (job)) or (max(irate(node_disk_read_bytes_total[30s])) by (job))`

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1)
}

// RequestPromCPUUsage 查询Prom上机器的CPU使用率
// 取4位有效数字后转换成int64，相比float64满足精度的前提下提高计算速度
// e.g.: 0.012->12 23.453453245->2345
func (promClient *PromClient) RequestPromCPUUsage() (map[string]int64, error) {
	promQL := `(1 - avg(rate(node_cpu_seconds_total{mode="idle"}[30s])) by (job))`

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 100)
}

// RequestPromMemUsage 查询Prom上机器的内存使用率
// 取4位有效数字后转换成int64，相比float64满足精度的前提下提高计算速度
// e.g.: 0.012->12 23.453453245->2345
func (promClient *PromClient) RequestPromMemUsage() (map[string]int64, error) {
	promQL := `(1 - (node_memory_MemAvailable_bytes / (node_memory_MemTotal_bytes)))`

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 100)
}
