package main

import (
	"time"
	"net"
	"strconv"

	"github.com/influxdata/influxdb/client/v2"
)

var (
	metricsPointCh chan *client.Point
)

func makeMetricsPoint(duration float64, op string, isSucc bool) *client.Point {
	tags := map[string]string{
		"op": op,
		"is_exc": strconv.FormatBool(!isSucc),
	}
	fields := map[string]interface{}{
		"value": duration,
	}
	point, _ := client.NewPoint("luka", tags, fields, time.Now())
	return point
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
			wgWriteFinish.Done()
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
