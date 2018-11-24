package main

import (
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docopt/docopt-go"
	"github.com/go-redis/redis"
	"github.com/influxdata/influxdb/client/v2"
	"gopkg.in/cheggaaa/pb.v2"
)

const usage = `
Redis Pressure Test Command Tool.

Usage:
	luka [--host=<host>] [--port=<port>] [--worker=<worker_number>] [--influxdb-host=<influxdb-host>] [--influxdb-port=<influxdb-port>] [--influxdb-database=<database>] [--total=<total>] [--op=<op>] [--total-key=<total-key>] [--pipeline=<pipeline>] [--data-size=<data-size>] [--total-data=<total-data>] [--need-fakedata]
	luka --help
	luka --version

Options:
	--help                                          Show this screen.
	--version                                       Show the version.
	--host=<host>                                   The redis host.
	-p <port>, --port=<port>                        The redis port.
	-w <worker_number>, --worker=<worker_number>    The number of the concurrent workers.
	--total=<total>                                 The total request count.
	--op=<op>                                       The redis op to do benchtest. Currently support: set, mset, lpush, rpush, sadd, zadd, hset, hmset, get, mget, lrange, smembers, scard, zcard, zcount, zscore, zrange, zrangebyscore, zrevrangebyscore, zrank, hget, hmget, hgetall
	--total-key=<total-key>                         Redis Unique Key count.
	--pipeline=<pipeline>                           Every pipeline contains n requests.
	--data-size=<data-size>                         The value size(KB).
	--total-data=<total-data>                       Total number of fake data, ONLY used when op is a READ operation, such as get, zrange.
	--need-fakedata                                 Need Luka to make fake data or NOT. It is useful ONLY if the op is a READ operation.
	--influxdb-host=<influxdb-host>					The influxdb host.
	--influxdb-port=<influxdb-port>					The influxdb port.
	--influxdb-database=<database>                  The influxdb database which will be written.
`
const pbFmt = "{{ red \"%s\" }} " +
	"{{ bar . | green }} " +
	"Spend: {{ etime . | cyan }} " +
	"Remain: {{ rtime . | yellow }} " +
	"Speed: {{ speed . | blue }} " +
	"Count: {{ counters . | magenta }} " +
	"Percent: {{ percent . | rndcolor }}"

var (
	wgWriteFinish sync.WaitGroup
	wgMakeFake    sync.WaitGroup
	arguments     map[string]interface{}
	benchBar      *pb.ProgressBar
	addBar        *pb.ProgressBar
)

func getRedisClient(host, port string) *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr:         net.JoinHostPort(host, port),
		Password:     "",
		DB:           0,
		PoolSize:     100,
		ReadTimeout:  time.Duration(500 * time.Millisecond),
		WriteTimeout: time.Duration(200 * time.Millisecond),
	})
	return redisClient
}

func ensureAllArguments() {
	if arguments["--host"] == nil {
		arguments["--host"] = "127.0.0.1"
	}
	if arguments["--port"] == nil {
		arguments["--port"] = "6379"
	}
	if arguments["--worker"] == nil {
		arguments["--worker"] = "20"
	}
	if arguments["--influxdb-host"] == nil {
		arguments["--influxdb-host"] = "127.0.0.1"
	}
	if arguments["--influxdb-port"] == nil {
		arguments["--influxdb-port"] = "8083"
	}
	if arguments["--influxdb-database"] == nil {
		arguments["--influxdb-database"] = "udp"
	}
	if arguments["--pipeline"] == nil {
		arguments["--pipeline"] = "0"
	}
	if arguments["--total-key"] == nil {
		arguments["--total-key"] = arguments["--total"]
	}
	if arguments["--data-size"] == nil {
		arguments["--data-size"] = "2"
	}
}

func benchOp(host, port string, total, pipeline int, op string, dataSize int) {
	var flag bool
	redisClient := getRedisClient(host, port)
	defer redisClient.Close()

	// get target function and build needed args use reflect
	redisOp := NewRedisOp(op, dataSize)
	opObj := opMapping[op]
	fc := reflect.ValueOf(redisOp).MethodByName(opObj.funcName)
	rc := make([]reflect.Value, 0)

	bench := func() {
		startTime := time.Now()
		flag = fc.Call(rc)[0].Bool()
		duration := time.Now().Sub(startTime).Seconds()
		benchBar.Add(1)
		metricsPointCh <- makeMetricsPoint(duration, op, flag)
	}

	benchPipeline := func(pipe redis.Pipeliner) {
		startTime := time.Now()
		for i := 0; i < pipeline; i++ {
			fc.Call(rc)
		}
		_, err := pipe.Exec()
		duration := time.Now().Sub(startTime).Seconds()
		if err != nil {
			flag = true
		} else {
			flag = false
		}
		benchBar.Add(pipeline)
		metricsPointCh <- makeMetricsPoint(duration, op, flag)
	}

	if pipeline > 0 { // use pipeline
		pipe := redisClient.Pipeline()
		rc = append(rc, reflect.ValueOf(pipe))
		for t := 0; t < total; t++ {
			benchPipeline(pipe)
		}
	} else { // without pipeline
		rc = append(rc, reflect.ValueOf(redisClient))
		for t := 0; t < total; t++ {
			bench()
		}
	}
}

func makeFakeData(host, port, op string, totalData, dataSize int) {
	redisClient := getRedisClient(host, port)
	defer redisClient.Close()
	defer wgMakeFake.Done()
	redisOp := NewRedisOp(op, dataSize)
	rv := redisOp.FillUpData(redisClient, totalData)
	if rv != nil {
		fmt.Printf("Failed to fill up fake redis data: %s\n", rv)
		return
	}
}

// Get benchmark operation count.
// This func returns two integer.
// 1. If `pipeline` has been given:
//		the first one is the total count of each worker need to send request
//		the second one is the pipeline value
// 2. If `pipeline` has not been given, which means send single commend:
//		the first one is the total count of each worker need to send request
//		the second one is 0
func getOpCount(worker, total, pipeline int) (int, int) {
	pipelineCount := Min(total/worker, Max(0, pipeline))
	roundCount := total / worker
	if pipelineCount != 0 {
		if roundCount%pipelineCount == 0 {
			roundCount = roundCount / pipelineCount
		} else {
			roundCount = roundCount/pipelineCount + 1
		}
	}
	return roundCount, pipelineCount
}

func main() {
	arguments, _ = docopt.Parse(usage, nil, true, "1.0.0", false)
	// Update default value for params
	ensureAllArguments()
	host := arguments["--host"].(string)
	port := arguments["--port"].(string)
	op := strings.ToLower(arguments["--op"].(string))
	worker, _ := strconv.Atoi(arguments["--worker"].(string))
	total, _ := strconv.Atoi(arguments["--total"].(string))
	pipeline, _ := strconv.Atoi(arguments["--pipeline"].(string))
	dataSize, _ := strconv.Atoi(arguments["--data-size"].(string))
	totalData, _ := strconv.Atoi(arguments["--total-data"].(string))
	totalKey, _ = strconv.Atoi(arguments["--total-key"].(string))

	roundCount, pipelineCount := getOpCount(worker, total, pipeline)
	metricsPointCh = make(chan *client.Point, total)
	totalField = totalData / totalKey

	if pipelineCount > 0 {
		fmt.Printf("Use Pipeline: %d\n", pipelineCount)
	}

	// make fake redis data
	if !opMapping[op].isWrite && arguments["--need-fakedata"].(bool) {
		fmt.Println("Start to fill up fake redis data.")
		addBar = pb.ProgressBarTemplate(fmt.Sprintf(pbFmt, "Fill up Fake Data: ")).Start(totalData)
		defer addBar.Finish()
		wgMakeFake.Add(worker)
		for x := 0; x < worker; x++ {
			go makeFakeData(host, port, op, totalData/worker, dataSize)
		}
		wgMakeFake.Wait()
		fmt.Printf(
			"Inserting fake data into Redis. Success: %d, Failure: %d.\n",
			fakeDataCount,
			fakeDataCountFail,
		)
		if fakeDataCount == 0 {
			return
		}
	}

	benchBar = pb.ProgressBarTemplate(fmt.Sprintf(pbFmt, "Benchmark")).Start(total)
	defer benchBar.Finish()
	// handle bench test
	for i := 1; i <= worker; i++ {
		go benchOp(host, port, roundCount, pipelineCount, op, dataSize)
	}
	fmt.Println("Start sending metrics...")

	// metrics coroutine
	if pipelineCount > 0 {
		wgWriteFinish.Add(total / pipelineCount / 100)
	} else {
		wgWriteFinish.Add(total / 100)
	}
	go sendMetricsPointLoop()
	wgWriteFinish.Wait()
}
