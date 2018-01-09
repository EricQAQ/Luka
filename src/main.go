package main

import (
	"fmt"
	"reflect"
	"net"
	"time"
	"strings"
	"strconv"
	"sync"

	"github.com/docopt/docopt-go"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/go-redis/redis"
)

const usage = `
Redis Pressure Test Command Tool.

Usage:
	luka [--host=<host>] [--port=<port>] [--worker=<worker_number>] [--influxdb-host=<influxdb-host>] [--influxdb-port=<influxdb-port>] [--influxdb-database=<database>] [--total=<total>] [--op=<op>] [--rand-key=<rand-key>] [--pipeline=<pipeline>]
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
	--rand-key=<rand-key>                           Redis Unique Key count.
	--pipeline=<pipeline>                           Every pipeline contains n requests.
	--influxdb-host=<influxdb-host>					The influxdb host.
	--influxdb-port=<influxdb-port>					The influxdb port.
	--influxdb-database=<database>                  The influxdb database which will be written.
`

var (
	wgWriteFinish sync.WaitGroup
	wgMakeFake sync.WaitGroup
	arguments map[string]interface{}
)

func getRedisClient(host, port string) *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     net.JoinHostPort(host, port),
		Password: "",
		DB:       0,
		PoolSize: 100,
		ReadTimeout: time.Duration(500 * time.Millisecond),
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
}

func benchOp(host, port string,
			 total, pipeline, unqKeyCount int,
			 op string) {
	redisClient := getRedisClient(host, port)
	defer redisClient.Close()

	// get target function and build needed args use reflect
	redisOp := &RedisOp{op_name: op}
	opObj := opMapping[op]
	fc := reflect.ValueOf(redisOp).MethodByName(opObj.funcName)
	rc := make([]reflect.Value, 0)
	rc = append(rc, reflect.ValueOf(redisClient))
	if opObj.isWrite {
		rc = append(rc, reflect.ValueOf(unqKeyCount), reflect.ValueOf(false))
	}

	bench := func() {
		startTime := time.Now()
		flag := fc.Call(rc)[0].Bool()
		duration := time.Now().Sub(startTime).Seconds()
		metricsPointCh <- makeMetricsPoint(duration, op, flag)
	}

	if pipeline > 0 {	// use pipeline
		for t := 0; t < total; t++ {
			for i := 0; i < pipeline; i++ { bench() }
		}
	} else {	// without pipeline
		for t := 0; t < total; t++ { bench() }
	}
}

func makeFakeData(host, port, op string, total, unqKeyCount int) {
	redisClient := getRedisClient(host, port)
	defer redisClient.Close()
	defer wgMakeFake.Done()
	redisOp := RedisOp{op_name: op}
	rv := redisOp.FillUpData(redisClient, total, unqKeyCount)
	if rv != nil {
		fmt.Printf("Failed to fill up fake redis data: %s\n", rv)
		return
	}
}

func getOpCount(worker, total, pipeline int) (int, int) {
	pipelineCount := Min(total / worker, Max(0, pipeline))
	roundCount := total / worker
	if pipelineCount != 0 {
		if roundCount % pipelineCount == 0 {
			roundCount = roundCount / pipelineCount
		} else {
			roundCount = roundCount / pipelineCount + 1
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
	unqKeyCount, err := strconv.Atoi(arguments["--rand-key"].(string))
	if err != nil {
		unqKeyCount = total
	}

	roundCount, pipelineCount := getOpCount(worker, total, pipeline)
	metricsPointCh = make(chan *client.Point, total)

	if pipelineCount > 0 {
		fmt.Printf("Use Pipeline: %d\n", pipelineCount)
	}

	// make fake redis data
	if !opMapping[op].isWrite {
		fmt.Println("Start to fill up fake redis data.")
		wgMakeFake.Add(10)
		for x := 0; x < 10; x++ {
			go makeFakeData(host, port, op, total / 10, unqKeyCount)
		}
		wgMakeFake.Wait()
		fmt.Printf("Totally inserted %d fake data into Redis.\n", fakeDataCount)
	}

	// handle bench test
	for i := 1; i <= worker; i++ {
		go benchOp(host, port, roundCount, pipelineCount, unqKeyCount, op)
	}
	fmt.Println("Start sending metrics...")

	// metrics coroutine
	wgWriteFinish.Add(total / 100)
	go sendMetricsPointLoop()
	wgWriteFinish.Wait()
}
