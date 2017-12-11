package main

import (
	"fmt"
	"net"
	"time"
	"strconv"
	"math/rand"

	"github.com/docopt/docopt-go"
	"github.com/go-redis/redis"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/satori/go.uuid"
)

const usage = `
Redis Pressure Test Command Tool.

Usage:
	luka [--host=<host>] [--port=<port>] [--worker=<worker_number>] [--influxdb-host=<influxdb-host>] [--influxdb-port=<influxdb-port>] [--influxdb-database=<database>] [--total=<total>]
	luka --help
	luka --version

Options:
	--help                                          Show this screen.
	--version                                       Show the version.
	--host=<host>                                   The redis host.
	-p <port>, --port=<port>                        The redis port.
	-w <worker_number>, --worker=<worker_number>    The number of the concurrent workers.
	--total=<total>                                 The total request count.
	--influxdb-host=<influxdb-host>					The influxdb host.
	--influxdb-port=<influxdb-port>					The influxdb port.
	--influxdb-database=<database>                  The influxdb database which will be written.
`

var (
	metricsPointCh chan *client.Point
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

func sendMetricsPointLoop() {
	influxdbClient, _ := client.NewUDPClient(client.UDPConfig{
		Addr: net.JoinHostPort(arguments["--influxdb-host"].(string), arguments["--influxdb-port"].(string)),
	})
	defer influxdbClient.Close()

	var bp client.BatchPoints

	makeBatchPoint := func() client.BatchPoints {
		bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
			Database: arguments["--influxdb-database"].(string),
			Precision: "ns",
		})
		return bp
	}

	write := func(bp client.BatchPoints) {
		if len(bp.Points()) > 0 {
			influxdbClient.Write(bp)
			isWriteFinish <- true
		}
	}

	for {
		p := <-metricsPointCh
		if bp == nil {
			bp = makeBatchPoint()
		}
		bp.AddPoint(p)
		if len(bp.Points()) >= 100 {
			go write(bp)
			bp = nil
		}
	}
}

func makeMetricsPoint(duration float64, op string, err error) *client.Point {
	flag := "false"
	errInfo := ""
	if err != nil {
		flag = "true"
		errInfo =err.Error()
	}
	tags := map[string]string{
		"op": op,
		"is_exc": flag,
	}
	fields := map[string]interface{}{
		"value": duration,
		"error": errInfo,
	}
	point, _ := client.NewPoint("luka", tags, fields, time.Now())
	return point
}

func benchSetOp(host, port string, total int) {
	redisClient := getRedisClient(host, port)
	defer redisClient.Close()
	for t := 1; t <= total; t++ {
		startTime := time.Now()
		key := fmt.Sprintf("%s-set-%d", uuid.NewV4().String(), rand.Intn(10000000))
		flag := redisClient.Set(key, rand.Intn(100000), 0).Err()
		duration := time.Now().Sub(startTime).Seconds()
		metricsPointCh <- makeMetricsPoint(duration, "set", flag)
	}
}

func main() {
	arguments, _ = docopt.Parse(usage, nil, true, "1.0.0", false)
	// Update default value for params
	ensureAllArguments()
	host := arguments["--host"].(string)
	port := arguments["--port"].(string)
	worker, _ := strconv.Atoi(arguments["--worker"].(string))
	total, _ := strconv.Atoi(arguments["--total"].(string))
	metricsPointCh = make(chan *client.Point, total)
	isWriteFinish = make(chan bool, total / 100)
	for i := 1; i <= worker; i++ {
		go benchSetOp(host, port, total / worker)
	}
	fmt.Println("Start sending metrics...")
	go sendMetricsPointLoop()
	for i:= 0; i < total / 100; i++ {
		<- isWriteFinish
	}
}
