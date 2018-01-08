# Luka

Luka is a command tool for redis benchmark tool implementation in Golang.

It can collect redis benchmark data and send to influxdb, for getting QPS and Response Time better.

```
Redis Pressure Test Command Tool.

Usage:
	luka [--host=<host>] [--port=<port>] [--worker=<worker_number>]
         [--influxdb-host=<influxdb-host>] [--influxdb-port=<influxdb-port>]
         [--influxdb-database=<database>] [--total=<total>]
         [--op=<op>] [--rand-key=<rand-key>] [--pipeline=<pipeline>]
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
```
