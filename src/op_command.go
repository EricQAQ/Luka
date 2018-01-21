package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"

	"github.com/go-redis/redis"
)

const MAXVALUE = 1000000
const KEYRANGE = 10000000
const FILLUPPIPELINE = 100

type OpAttr struct {
	funcName  string
	isWrite   bool
	writeFunc string
}

var (
	totalKey          int
	totalField        int
	fakeDataCount     = 0
	fakeDataCountFail = 0
	fakeKey           = 1
)

var opMapping = map[string]OpAttr{
	"set":   OpAttr{funcName: "Set", isWrite: true, writeFunc: "Set"},
	"mset":  OpAttr{funcName: "MSet", isWrite: true, writeFunc: "Set"},
	"lpush": OpAttr{funcName: "LPush", isWrite: true, writeFunc: "LPush"},
	"rpush": OpAttr{funcName: "RPush", isWrite: true, writeFunc: "LPush"},
	"sadd":  OpAttr{funcName: "SAdd", isWrite: true, writeFunc: "SAdd"},
	"zadd":  OpAttr{funcName: "ZAdd", isWrite: true, writeFunc: "ZAdd"},
	"hset":  OpAttr{funcName: "HSet", isWrite: true, writeFunc: "HSet"},
	"hmset": OpAttr{funcName: "HMSet", isWrite: true, writeFunc: "HSet"},

	"get":              OpAttr{funcName: "Get", isWrite: false, writeFunc: "Set"},
	"mget":             OpAttr{funcName: "MGet", isWrite: false, writeFunc: "Set"},
	"lrange":           OpAttr{funcName: "LRange", isWrite: false, writeFunc: "LPush"},
	"smembers":         OpAttr{funcName: "SMembers", isWrite: false, writeFunc: "Sadd"},
	"scard":            OpAttr{funcName: "SCard", isWrite: false, writeFunc: "Sadd"},
	"zcard":            OpAttr{funcName: "ZCard", isWrite: false, writeFunc: "ZAdd"},
	"zcount":           OpAttr{funcName: "ZCount", isWrite: false, writeFunc: "ZAdd"},
	"zscore":           OpAttr{funcName: "ZScore", isWrite: false, writeFunc: "ZAdd"},
	"zrange":           OpAttr{funcName: "ZRange", isWrite: false, writeFunc: "ZAdd"},
	"zrangebyscore":    OpAttr{funcName: "ZRangeByScore", isWrite: false, writeFunc: "ZAdd"},
	"zrevrangebyscore": OpAttr{funcName: "ZRevRangeByScore", isWrite: false, writeFunc: "ZAdd"},
	"zrank":            OpAttr{funcName: "ZRank", isWrite: false, writeFunc: "ZAdd"},
	"hget":             OpAttr{funcName: "HGet", isWrite: false, writeFunc: "HSet"},
	"hmget":            OpAttr{funcName: "HMGet", isWrite: false, writeFunc: "HSet"},
	"hgetall":          OpAttr{funcName: "HGetAll", isWrite: false, writeFunc: "HSet"},
}

func generator(isKey bool, op string) string {
	var key string

	if isKey {
		if fakeKey >= totalKey {
			key = fmt.Sprintf("%s-%020d", op, rand.Intn(fakeKey))
		} else {
			key = fmt.Sprintf("%s-%020d", op, fakeKey)
			fakeKey++
		}
	} else {
		key = fmt.Sprintf("%015d", rand.Intn(totalField))
	}
	return key
}

type RedisOp struct {
	opName string
}

func (op *RedisOp) Set(cmdable redis.Cmdable) bool {
	key := generator(true, op.opName)
	value := rand.Intn(MAXVALUE)
	return cmdable.Set(key, value, 0).Err() == nil
}

func (op *RedisOp) MSet(cmdable redis.Cmdable) bool {
	var kv = make([]interface{}, totalField*2, totalField*2)
	for i := 0; i < totalField*2; {
		kv[i] = generator(true, op.opName) // key
		kv[i+1] = rand.Intn(MAXVALUE)      // value
		i = i + 2
	}
	return cmdable.MSet(kv...).Err() == nil
}

func (op *RedisOp) listPush(cmdable redis.Cmdable, isLeft bool) error {
	key := generator(true, op.opName)
	var value = make([]interface{}, totalField, totalField)
	for i := 0; i < totalField; i++ {
		value[i] = generator(false, op.opName)
	}
	if isLeft {
		return cmdable.LPush(key, value...).Err()
	}
	return cmdable.RPush(key, value...).Err()
}

func (op *RedisOp) LPush(cmdable redis.Cmdable) bool {
	return op.listPush(cmdable, true) == nil
}

func (op *RedisOp) RPush(cmdable redis.Cmdable) bool {
	return op.listPush(cmdable, false) == nil
}

func (op *RedisOp) SAdd(cmdable redis.Cmdable) bool {
	key := generator(true, op.opName)
	var value = make([]interface{}, totalField, totalField)
	for i := 0; i < totalField; i++ {
		value[i] = generator(false, op.opName)
	}
	return cmdable.SAdd(key, value...).Err() == nil
}

func (op *RedisOp) ZAdd(cmdable redis.Cmdable) bool {
	key := generator(true, op.opName)
	var value = make([]redis.Z, totalField, totalField)
	for i := 0; i < totalField; i++ {
		member := generator(false, op.opName)
		value[i] = redis.Z{
			Score:  float64(rand.Intn(MAXVALUE)),
			Member: member,
		}
	}
	return cmdable.ZAdd(key, value...).Err() == nil
}

func (op *RedisOp) HSet(cmdable redis.Cmdable) bool {
	key := generator(true, op.opName)
	field := generator(false, op.opName)
	value := rand.Intn(MAXVALUE)
	return cmdable.HSet(key, field, value).Err() == nil
}

func (op *RedisOp) HMSet(cmdable redis.Cmdable) bool {
	key := generator(true, op.opName)
	var hashMap = make(map[string]interface{})
	for i := 0; i < totalField; i++ {
		field := generator(false, op.opName)
		hashMap[field] = rand.Intn(MAXVALUE)
	}
	return cmdable.HMSet(key, hashMap).Err() == nil
}

func (op *RedisOp) FillUpData(redisClient *redis.Client, totalKey int) error {
	round := totalKey/FILLUPPIPELINE + 1
	redisOp := opMapping[op.opName]
	pipe := redisClient.Pipeline()
	fc := reflect.ValueOf(op).MethodByName(redisOp.writeFunc)
	rc := make([]reflect.Value, 0)
	rc = append(rc, reflect.ValueOf(pipe))
	var rv error

	for i := 0; i < round; i++ {
		for j := 0; j < FILLUPPIPELINE; j++ {
			fc.Call(rc)
		}
		_, err := pipe.Exec()
		if err != nil {
			rv = err
			fakeDataCountFail = fakeDataCountFail + FILLUPPIPELINE
		} else {
			fakeDataCount = fakeDataCount + FILLUPPIPELINE
			addBar.Add(FILLUPPIPELINE)
		}
	}
	return rv
}

func (op *RedisOp) Get(cmdable redis.Cmdable) bool {
	return cmdable.Get(generator(true, op.opName)).Err() == nil
}

func (op *RedisOp) MGet(cmdable redis.Cmdable) bool {
	count := rand.Intn(10) + 1
	var k = make([]string, count, count)
	for i := 0; i < count; i++ {
		k[i] = generator(true, op.opName)
	}
	return cmdable.MGet(k...).Err() == nil
}

func (op *RedisOp) LRange(cmdable redis.Cmdable) bool {
	return cmdable.LRange(
		generator(true, op.opName), 0, int64(totalField),
	).Err() == nil
}

func (op *RedisOp) SMembers(cmdable redis.Cmdable) bool {
	return cmdable.SMembers(generator(true, op.opName)).Err() == nil
}

func (op *RedisOp) SCard(cmdable redis.Cmdable) bool {
	return cmdable.SCard(generator(true, op.opName)).Err() == nil
}

func (op *RedisOp) ZCard(cmdable redis.Cmdable) bool {
	return cmdable.ZCard(generator(true, op.opName)).Err() == nil
}

func (op *RedisOp) ZCount(cmdable redis.Cmdable) bool {
	rang := []int{rand.Intn(MAXVALUE), rand.Intn(MAXVALUE)}
	sort.Ints(rang)
	return cmdable.ZCount(
		generator(true, op.opName),
		strconv.Itoa(rang[0]),
		strconv.Itoa(rang[1]),
	).Err() == nil
}

func (op *RedisOp) ZScore(cmdable redis.Cmdable) bool {
	return cmdable.ZScore(
		generator(true, op.opName),
		generator(false, op.opName),
	).Err() == nil
}

func (op *RedisOp) ZRange(cmdable redis.Cmdable) bool {
	return cmdable.ZRange(
		generator(true, op.opName), 0, int64(totalField),
	).Err() == nil
}

func (op *RedisOp) ZRangeByScore(cmdable redis.Cmdable) bool {
	rang := []int{rand.Intn(MAXVALUE), rand.Intn(MAXVALUE)}
	sort.Ints(rang)
	return cmdable.ZRangeByScore(
		generator(true, op.opName),
		redis.ZRangeBy{
			Min: strconv.Itoa(rang[0]),
			Max: strconv.Itoa(rang[1]),
		},
	).Err() == nil
}

func (op *RedisOp) ZRevRangeByScore(cmdable redis.Cmdable) bool {
	rang := []int{rand.Intn(MAXVALUE), rand.Intn(MAXVALUE)}
	sort.Ints(rang)
	return cmdable.ZRevRangeByScore(
		generator(true, op.opName),
		redis.ZRangeBy{
			Max: strconv.Itoa(rang[1]),
			Min: strconv.Itoa(rang[0]),
		},
	).Err() == nil
}

func (op *RedisOp) ZRank(cmdable redis.Cmdable) bool {
	return cmdable.ZRank(
		generator(true, op.opName),
		generator(false, op.opName),
	).Err() == nil
}

func (op *RedisOp) HGet(cmdable redis.Cmdable) bool {
	return cmdable.HGet(
		generator(true, op.opName),
		generator(false, op.opName),
	).Err() == nil
}

func (op *RedisOp) HGetAll(cmdable redis.Cmdable) bool {
	return cmdable.HGetAll(generator(true, op.opName)).Err() == nil
}

func (op *RedisOp) HMGet(cmdable redis.Cmdable) bool {
	return cmdable.HMGet(
		generator(true, op.opName),
		generator(false, op.opName),
		generator(false, op.opName),
		generator(false, op.opName),
	).Err() == nil
}
