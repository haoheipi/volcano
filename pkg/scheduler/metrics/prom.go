package metrics

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/mat"
	"k8s.io/klog"
	"math"
	"time"
	schedulerapi "volcano.sh/volcano/pkg/scheduler/api"
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

func (promClient *PromClient) RequestPromDemo() (map[string]int64, error) {
	// d.promClient.ExecPromQL("up")
	// d.promClient.ExecPromQL(`increase(node_network_receive_bytes_total{device=~"eth0"}[30s])`)
	err, result := promClient.ExecPromQL(`max(irate(node_network_receive_bytes_total[30s])*8/1024) by (job)`)
	if err != nil {
		return nil, nil
	}
	return promClient.parsePromResultInt64(result, 1)
}

// RequestPromUpNetIO 获取网络上传负载
// 单位 kB/s
func (promClient *PromClient) RequestPromUpNetIO(netType string) (map[string]int64, error) {
	promQL := fmt.Sprintf("avg(irate(node_network_transmit_bytes_total{device=\"%s\"}[30s])/1000) by (instance)", netType)

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1)
}

// RequestPromNetIO 获取网络下载负载
func (promClient *PromClient) RequestPromDownNetIO(netType string) (map[string]int64, error) {
	promQL := fmt.Sprintf("avg(irate(node_network_receive_bytes_total{device=\"%s\"}[30s])/1000) by (instance)", netType)
	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1)
}

// RequestPromWriteDiskIO 查询Prom上机器的写DiskIO
// 单位 kB/s
func (promClient *PromClient) RequestPromWriteDiskIO(diskType string) (map[string]int64, error) {
	promQL := fmt.Sprintf("avg(irate(node_disk_written_bytes_total{device=\"%s\"}[30s])/1000) by (instance)", diskType)

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1)
}

// RequestPromReadDiskIO 查询Prom上机器的读DiskIO
// 单位 kB/s
func (promClient *PromClient) RequestPromReadDiskIO(diskType string) (map[string]int64, error) {
	promQL := fmt.Sprintf("avg(irate(node_disk_read_bytes_total{device=\"%s\"}[30s])/1000) by (instance)", diskType)

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1)
}

// RequestPromCPUUsage 查询Prom上机器的CPU使用率
// 取小数点后3位有效数字后转换成int64，相比float64满足精度的前提下提高计算速度
// e.g.: 0.012->12 23.453453245->23453
func (promClient *PromClient) RequestPromCPUUsage() (map[string]int64, error) {
	promQL := `(1 - avg(irate(node_cpu_seconds_total{mode="idle"}[30s])) by (instance))`

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1000)
}

// RequestPromMemUsage 查询Prom上机器的内存使用率
// 取小数点后3位有效数字后转换成int64，相比float64满足精度的前提下提高计算速度
// e.g.: 0.012->12 23.453453245->23453
func (promClient *PromClient) RequestPromMemUsage() (map[string]int64, error) {
	promQL := `(1 - (avg(node_memory_MemAvailable_bytes) by (instance)  / avg(node_memory_MemTotal_bytes) by (instance) ) )`

	err, result := promClient.ExecPromQL(promQL)
	if err != nil {
		return nil, err
	}

	return promClient.parsePromResultInt64(result, 1000)
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
			resMap[string(tmp.Metric["instance"])] = int64(math.Round(tmpv * float64(base)))
		} else {
			resMap[string(tmp.Metric["instance"])] = int64(math.Round(tmpv))
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
		resMap[string(tmp.Metric["instance"])] = float64(tmp.Value)
	}

	return resMap, nil
}

var cacheMap = make(map[schedulerapi.JobID]map[string]int64, 1000)

func generateScore(job schedulerapi.JobID) {
	promClient, err := NewPromClient("http://60.245.211.161:32556")
	if err != nil {
		fmt.Printf("new prom client failed:%v", err)
		return
	}
	downNetIO, err := promClient.RequestPromDownNetIO("eth0")
	upNetIO, err := promClient.RequestPromUpNetIO("eth0")
	readDiskIO, err := promClient.RequestPromReadDiskIO("vdb")
	writeDiskIO, err := promClient.RequestPromWriteDiskIO("vdb")
	memUsage, err := promClient.RequestPromMemUsage()
	cpuUsage, err := promClient.RequestPromCPUUsage()

	if err != nil {
		fmt.Printf("request prom failed:%v", err)
		return
	}

	var nodeNames []string
	for k := range downNetIO {
		nodeNames = append(nodeNames, k)
	}
	matrix := generateMatrix(downNetIO, upNetIO, readDiskIO, writeDiskIO, memUsage, cpuUsage, nodeNames)
	topScore := calcTOPSIS(matrix)
	scoreMap := convertMap(nodeNames, topScore)
	cacheMap[job] = scoreMap
}

func generateMatrix(downNetIO, upNetIO, readDiskIO, writeDiskIO, memUsage, cpuUsage map[string]int64, names []string) *mat.Dense {
	getArray := func(mapData map[string]int64, names []string) []float64 {
		var array []float64
		for _, v := range names {
			array = append(array, float64(mapData[v]))
		}
		return array
	}
	downNetIOArr := getArray(downNetIO, names)
	upNetIOArr := getArray(upNetIO, names)
	readDiskIOArr := getArray(readDiskIO, names)
	writeDiskIOArr := getArray(writeDiskIO, names)
	memUsageArr := getArray(memUsage, names)
	cpuUsageArr := getArray(cpuUsage, names)

	colArr := [][]float64{downNetIOArr, upNetIOArr, readDiskIOArr, writeDiskIOArr, memUsageArr, cpuUsageArr}
	row := len(names)
	col := len(colArr)
	matrix := mat.NewDense(row, col, nil)
	for i := 0; i < col; i++ {
		matrix.SetCol(i, colArr[i])
	}
	return matrix
}

func generateScoreAndReturnNodeScore(nodeName string, job schedulerapi.JobID) int64 {
	generateScore(job)
	if value, existed := cacheMap[job]; existed {
		if score, existed := value[nodeName]; existed {
			return score
		}
	}
	return -1
}

func Score(nodeName string, job schedulerapi.JobID) int64 {
	if value, existed := cacheMap[job]; existed {
		if score, existed := value[nodeName]; existed {
			return score
		}
	}
	return generateScoreAndReturnNodeScore(nodeName, job)
}

func convertMap(nodeNames []string, scores []float64) map[string]int64 {
	num := len(nodeNames)
	if num != len(scores) {
		panic("ConvertMap num of two arrays does not equal")
	}

	res := make(map[string]int64)
	for i := 0; i < num; i++ {
		name := nodeNames[i]
		res[name] = int64(100 - math.Round(scores[i]*float64(100)))
	}
	return res
}

// CalcTOPSIS 计算TOPSIS值，输入的数组默认已经同向化
// cpu: 	[1,2,3]
// mem:		[2,3,4]
// netload:	[1,2,5]
// netcap:	[2,4,5]
// filtered: [0.0, 0.0, 0.0, 0.0]
// 如果存在列全部为0，则默认填充1
func calcTOPSIS(matrix *mat.Dense) []float64 {
	// 矩阵是否规范检查
	if IsMatrixEmpty(matrix) {
		return nil
	}
	row := matrix.RawMatrix().Rows
	col := matrix.RawMatrix().Cols
	if row == 1 {
		return []float64{1.0}
	}

	// 检查是否存在负数
	if err := CheckMatrixNegative(matrix); err != nil {
		return nil
	}
	// 如果某列为全部为0，则填充1
	ResetZeroCol(matrix, 1.0)

	// 1. 按照矩阵列正规化
	maxMinArr := make([][]float64, col)
	maxMinMatrix := mat.NewDense(col, 2, nil)
	for i := 0; i < col; i++ {
		colArr := GetDenseCol(matrix, i)
		normArr := NormArray(colArr)
		// 得到max/min
		maxMin := make([]float64, 2)
		maxMin[0] = floats.Max(normArr)
		maxMin[1] = floats.Min(normArr)
		maxMinArr[i] = maxMin
		maxMinMatrix.SetRow(i, maxMin)
		matrix.SetCol(i, normArr)
	}

	// 2. 计算每个维度的距离
	// 要考虑某一个维度是不是全部是0
	resMax := make([]float64, row)
	for i := 0; i < row; i++ {
		rowArr := matrix.RawRowView(i)
		maxSum, minSum := 0.0, 0.0
		for j := 0; j < col; j++ {
			maxSum += math.Pow(rowArr[j]-maxMinArr[j][0], 2)
			minSum += math.Pow(rowArr[j]-maxMinArr[j][1], 2)
		}
		dmax := math.Sqrt(maxSum)
		dmin := math.Sqrt(minSum)

		resMax[i] = dmin / (dmin + dmax)
	}

	return resMax
}

// ResetZeroCol 如果某列全部为0，填充为给定值
func ResetZeroCol(m *mat.Dense, dist float64) {
	/*
			cpu		mem		disk	net		netCap
		n1	0		3		2		3		2
		n2	0		2		0		0		3
		n3	0		9		34		2		12
	*/
	row := m.RawMatrix().Rows
	col := m.RawMatrix().Cols
	distArr := make([]float64, row)
	for i := range distArr {
		distArr[i] = dist
	}
	for i := 0; i < col; i++ {
		colArr := GetDenseCol(m, i)
		if IsZeroArray(colArr) {
			m.SetCol(i, distArr)
		}
	}
}

// IsZeroArray 判断数组是否全部为0，
// true全部为0
func IsZeroArray(arr []float64) bool {
	allZeroFlag := true
	for j := range arr {
		if allZeroFlag && arr[j] != 0.0 {
			allZeroFlag = false
		}
	}

	return allZeroFlag
}

func IsMatrixEmpty(m *mat.Dense) bool {
	if len(m.RawMatrix().Data) == 0 {
		return true
	}

	row := m.RawMatrix().Rows
	col := m.RawMatrix().Cols

	return row == 0 || col == 0
}

// CheckMatrixNegative 检查topsis算法输入的数组是否合法
// 某个值不能小于0
func CheckMatrixNegative(matrix *mat.Dense) error {
	var row, col int
	row = matrix.RawMatrix().Rows
	if row == 0 {
		return fmt.Errorf("empty matrix")
	}
	col = matrix.RawMatrix().Cols
	if col == 0 {
		return fmt.Errorf("empty matrix")
	}

	for i := 0; i < col; i++ {
		colArr := GetDenseCol(matrix, i)
		for j := range colArr {
			if colArr[j] < 0 {
				return fmt.Errorf("value %f of arr %v should not < 0",
					colArr[j], colArr)
			}
		}
	}

	return nil
}

// NormTOPSISArray 正规化一个数组
func NormArray(col []float64) []float64 {
	sum := 0.0
	num := len(col)
	for _, ele := range col {
		sum += math.Pow(ele, 2)
	}

	res := make([]float64, num)
	for i := range col {
		res[i] = col[i] / sum
	}

	return res
}

// GetDenseCol 获取矩阵某一列
func GetDenseCol(m *mat.Dense, c int) []float64 {
	vec := m.ColView(c)
	size := vec.Len()
	res := make([]float64, size)
	for i := range res {
		res[i] = vec.AtVec(i)
	}

	return res
}

// GetDenseCol 获取矩阵某一列
func GetDenseRow(m *mat.Dense, r int) []float64 {
	tmp := m.RawRowView(r)
	res := make([]float64, len(tmp))
	for i := range res {
		res[i] = tmp[i]
	}

	return res
}
