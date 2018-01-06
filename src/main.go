package main

import (
	"fmt"
	"reflect"
	"net"
	"time"
	"strconv"

	"github.com/docopt/docopt-go"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/go-redis/redis"
)

const usage = `
Redis Pressure Test Command Tool.

Usage:
	luka [--host=<host>] [--port=<port>] [--worker=<worker_number>] [--influxdb-host=<influxdb-host>] [--influxdb-port=<influxdb-port>] [--influxdb-database=<database>] [--total=<total>] [--op=<op>]
	luka --help
	luka --version

Options:
	--help                                          Show this screen.
	--version                                       Show the version.
	--host=<host>                                   The redis host.
	-p <port>, --port=<port>                        The redis port.
	-w <worker_number>, --worker=<worker_number>    The number of the concurrent workers.
	--total=<total>                                 The total request count.
	--op=<op>                                       The redis op to do benchtest.
	--influxdb-host=<influxdb-host>					The influxdb host.
	--influxdb-port=<influxdb-port>					The influxdb port.
	--influxdb-database=<database>                  The influxdb database which will be written.
`

var (
	isWriteFinish chan bool
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
}

func benchOp(host, port string, total int, op string) {
	redisClient := getRedisClient(host, port)
	defer redisClient.Close()
	redisOp := RedisOp{}
	fc := reflect.ValueOf(redisOp).MethodByName(op)
	rc := make([]reflect.Value, 0)
	rc = append(rc, reflect.ValueOf(redisClient))
	for t :=1; t <= total; t++ {
		startTime := time.Now()
		flag := fc.Call(rc)[0].Bool()
		duration := time.Now().Sub(startTime).Seconds()
		metricsPointCh <- makeMetricsPoint(duration, op, flag)
	}
}

func main() {
	arguments, _ = docopt.Parse(usage, nil, true, "1.0.0", false)
	// Update default value for params
	ensureAllArguments()
	host := arguments["--host"].(string)
	port := arguments["--port"].(string)
	op := arguments["--op"].(string)
	worker, _ := strconv.Atoi(arguments["--worker"].(string))
	total, _ := strconv.Atoi(arguments["--total"].(string))
	metricsPointCh = make(chan *client.Point, total)
	isWriteFinish = make(chan bool, total / 100)
	for i := 1; i <= worker; i++ {
		go benchOp(host, port, total / worker, op)
	}
	fmt.Println("Start sending metrics...")
	go sendMetricsPointLoop()
	for i:= 0; i < total / 100; i++ {
		<- isWriteFinish
	}
}
