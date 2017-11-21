package main

import (
	"fmt"
	"net"
	"time"
	"strconv"
	"runtime"

	"github.com/docopt/docopt-go"
	"github.com/go-redis/redis"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/Pallinder/go-randomdata"
)
import "net/http"
import _ "net/http/pprof"

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
	isFinish chan bool
	isWriteFinish chan bool
	redisClient *redis.Client
	arguments map[string]interface{}
)

func getRedisClient(host, port string) *redis.Client {
	if redisClient == nil {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     net.JoinHostPort(host, port),
			Password: "",
			DB:       0,
			PoolSize: 1000,
			ReadTimeout: time.Duration(500 * time.Millisecond),
			WriteTimeout: time.Duration(200 * time.Millisecond),
		})
	}
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

func simpleSet(host, port string) {
	redisClient = getRedisClient(host, port)
	startTime := time.Now()
	flag := redisClient.Set(randomdata.SillyName(), randomdata.Email(), 0).Err()
	duration := time.Now().Sub(startTime).Seconds()
	metricsPointCh <- makeMetricsPoint(duration, "set", flag)
	isFinish <- true
}

func simpleStringT(host, port string, worker, total int) {
	for t := 1; t <= total / worker; t++ {
		for i := 1; i <= worker; i++ {
			go simpleSet(host, port)
		}
		for j := 1; j <= worker; j++ {
			<- isFinish
		}
	}
}

func main() {
	arguments, _ = docopt.Parse(usage, nil, true, "1.0.0", false)
	ensureAllArguments()
	runtime.GOMAXPROCS(runtime.NumCPU())
	// Update default value for params
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	host := arguments["--host"].(string)
	port := arguments["--port"].(string)
	// Generator for fake data
	worker, _ := strconv.Atoi(arguments["--worker"].(string))
	total, _ := strconv.Atoi(arguments["--total"].(string))
	metricsPointCh = make(chan *client.Point, total)
	isFinish = make(chan bool, total)
	isWriteFinish = make(chan bool, total / 100)
	go sendMetricsPointLoop()
	simpleStringT(host, port, worker, total)
	defer redisClient.Close()
	for i:= 0; i < total / 100; i++ {
		<- isWriteFinish
	}
	fmt.Println(redisClient.DBSize())
	redisClient.FlushAll()
}
