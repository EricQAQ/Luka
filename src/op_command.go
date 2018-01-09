package main

import (
	"fmt"
	"math/rand"
	"strings"
	"strconv"
	"reflect"
	"sort"

	"github.com/satori/go.uuid"
	"github.com/go-redis/redis"
)

const MAXVALUE = 1000000
const KEYRANGE = 10000000
const FILLUPPIPELINE = 1000

type OpAttr struct {
	funcName string
	isWrite bool
	writeFunc string
}

var (
	keys []string
	fakeDataCount = 0
	fakeDataCountFail = 0
)

var opMapping = map[string]OpAttr {
	"set": OpAttr{ funcName: "Set", isWrite: true, writeFunc: "Set" },
	"mset": OpAttr{ funcName: "MSet", isWrite: true, writeFunc: "Set" },
	"lpush": OpAttr{ funcName: "LPush", isWrite: true, writeFunc: "LPush" },
	"rpush": OpAttr{ funcName: "RPush", isWrite: true, writeFunc: "LPush" },
	"sadd": OpAttr{ funcName: "SAdd", isWrite: true, writeFunc: "SAdd" },
	"zadd": OpAttr{ funcName: "ZAdd", isWrite: true, writeFunc: "ZAdd" },
	"hset": OpAttr{ funcName: "HSet", isWrite: true, writeFunc: "HSet" },
	"hmset": OpAttr{ funcName: "HMSet", isWrite: true, writeFunc: "HSet" },

	"get": OpAttr{ funcName: "Get", isWrite: false, writeFunc: "Set" },
	"mget": OpAttr{ funcName: "MGet", isWrite: false, writeFunc: "Set" },
	"lrange": OpAttr{ funcName: "LRange", isWrite: false, writeFunc: "LPush" },
	"smembers": OpAttr{ funcName: "SMembers", isWrite: false, writeFunc: "Sadd"},
	"scard": OpAttr{ funcName: "SCard", isWrite: false, writeFunc: "Sadd"},
	"zcard": OpAttr{ funcName: "ZCard", isWrite: false, writeFunc: "ZAdd"},
	"zcount": OpAttr{ funcName: "ZCount", isWrite: false, writeFunc: "ZAdd"},
	"zscore": OpAttr{ funcName: "ZScore", isWrite: false, writeFunc: "ZAdd"},
	"zrange": OpAttr{ funcName: "ZRange", isWrite: false, writeFunc: "ZAdd"},
	"zrangebyscore": OpAttr{ funcName: "ZRangeByScore", isWrite: false, writeFunc: "ZAdd"},
	"zrevrangebyscore": OpAttr{ funcName: "ZRevRangeByScore", isWrite: false, writeFunc: "ZAdd"},
	"zrank": OpAttr{ funcName: "ZRank", isWrite: false, writeFunc: "ZAdd"},
	"hget": OpAttr{ funcName: "HGet", isWrite: false, writeFunc: "HGet"},
	"hmget": OpAttr{ funcName: "HMGet", isWrite: false, writeFunc: "HGet"},
	"hgetall": OpAttr{ funcName: "HGetAll", isWrite: false, writeFunc: "HGet"},
}

func genKey(op string) string {
	return fmt.Sprintf("%s-%s-%d",
					   uuid.NewV4().String(),
					   op,
					   rand.Intn(KEYRANGE))
}

func genSimpleKey(op string, totalKey int) string {
	return fmt.Sprintf("%s-%d", op, rand.Intn(totalKey))
}

func genField() string {
	return fmt.Sprintf("%s-%d", uuid.NewV4().String(), rand.Intn(MAXVALUE))
}

func randIndex() int { return rand.Intn(len(keys) - 1) }

type RedisOp struct {
	op_name string
}

func (op *RedisOp)Set(cmdable redis.Cmdable, totalKey int, needRecord bool) bool {
	key := genKey(op.op_name)
	value := rand.Intn(MAXVALUE)
	if needRecord { keys = append(keys, key) }
	return cmdable.Set(key, value, 0).Err() == nil
}

func (op *RedisOp)MSet(cmdable redis.Cmdable, totalKey int, needRecord bool) bool {
	count := (rand.Intn(10) + 1) * 2
	var kv = make([]interface{}, count, count)
	for i := 0; i < count; {
		kv[i] = genKey(op.op_name)	// key
		kv[i+1] = rand.Intn(MAXVALUE)	// value
		if needRecord {
			keys = append(keys,kv[i].(string))
		}
		i = i + 2
	}
	return cmdable.MSet(kv...).Err() == nil
}

func (op *RedisOp)listPush(cmdable redis.Cmdable,
						   isLeft bool,
						   totalKey int,
						   needRecord bool) error {
	count := rand.Intn(10) + 1
	key := genSimpleKey(op.op_name, totalKey)
	var value = make([]interface{}, count, count)
	for i := 0; i < count; i++ {
		value[i] = genField()
	}
	if needRecord { keys = append(keys, key) }
	if isLeft {
		return cmdable.LPush(key, value...).Err()
	}
	return cmdable.RPush(key, value...).Err()
}

func (op *RedisOp)LPush(cmdable redis.Cmdable, totalKey int, needRecord bool) bool {
	return op.listPush(cmdable, true, totalKey, needRecord) == nil
}

func (op *RedisOp)RPush(cmdable redis.Cmdable, totalKey int, needRecord bool) bool {
	return op.listPush(cmdable, false, totalKey, needRecord) == nil
}

func (op *RedisOp)SAdd(cmdable redis.Cmdable, totalKey int, needRecord bool) bool {
	key := genSimpleKey(op.op_name, totalKey)
	count := rand.Intn(10) + 1
	var value = make([]interface{}, count, count)
	for i := 0; i < count; i++ {
		value[i] = genField()
	}
	if needRecord { keys = append(keys, key) }
	return cmdable.SAdd(key, value...).Err() == nil
}

func (op *RedisOp)ZAdd(cmdable redis.Cmdable, totalKey int, needRecord bool) bool {
	key := genSimpleKey(op.op_name, totalKey)
	count := rand.Intn(10) + 1
	var value = make([]redis.Z, count, count)
	for i := 0; i < count; i++ {
		member := genField()
		value[i] = redis.Z{
			Score: float64(rand.Intn(MAXVALUE)),
			Member: member,
		}
		if needRecord {
			keys = append(keys, fmt.Sprintf("%s:%s", key, member))
		}
	}
	return cmdable.ZAdd(key, value...).Err() == nil
}

func (op *RedisOp)HSet(cmdable redis.Cmdable, totalKey int, needRecord bool) bool {
	key := genSimpleKey(op.op_name, totalKey)
	field := genField()
	value := rand.Intn(MAXVALUE)
	if needRecord {
		keys = append(keys, fmt.Sprintf("%s:%s", key, field))
	}
	return cmdable.HSet(key, field, value).Err() == nil
}

func (op *RedisOp)HMSet(cmdable redis.Cmdable, totalKey int, needRecord bool) bool {
	key := genSimpleKey(op.op_name, totalKey)
	count := rand.Intn(10) + 1
	var hashMap = make(map[string]interface{})
	for i := 0; i < count; i++ {
		field := genField()
		hashMap[field] = rand.Intn(MAXVALUE)
		if needRecord {
			keys = append(keys, fmt.Sprintf("%s:%s", key, field))
		}
	}
	return cmdable.HMSet(key, hashMap).Err() == nil
}

func (op *RedisOp)FillUpData(redisClient *redis.Client, totalData, totalKey int) error {
	round := totalData / FILLUPPIPELINE + 1
	redisOp := opMapping[op.op_name]
	pipe := redisClient.Pipeline()
	fc := reflect.ValueOf(op).MethodByName(redisOp.writeFunc)
	rc := make([]reflect.Value, 0)
	rc = append(
		rc,
		reflect.ValueOf(pipe),
		reflect.ValueOf(totalKey),
		reflect.ValueOf(true),
	)
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
		}
	}
	return rv
}

func (op *RedisOp)Get(cmdable redis.Cmdable) bool {
	return cmdable.Get(keys[randIndex()]).Err() == nil
}

func (op *RedisOp)MGet(cmdable redis.Cmdable) bool {
	count := rand.Intn(10) + 1
	var k = make([]string, count, count)
	for i := 0; i < count; i++ {
		k[i] = keys[randIndex()]
	}
	return cmdable.MGet(k...).Err() == nil
}

func (op *RedisOp)LRange(cmdable redis.Cmdable) bool {
	return cmdable.LRange(keys[randIndex()], 0, 20).Err() == nil
}

func (op *RedisOp)SMembers(cmdable redis.Cmdable) bool {
	return cmdable.SMembers(keys[randIndex()]).Err() == nil
}


func (op *RedisOp)SCard(cmdable redis.Cmdable) bool {
	return cmdable.SCard(keys[randIndex()]).Err() == nil
}

func (op *RedisOp)ZCard(cmdable redis.Cmdable) bool {
	km := strings.Split(keys[randIndex()], ":")
	return cmdable.ZCard(km[0]).Err() == nil
}

func (op *RedisOp)ZCount(cmdable redis.Cmdable) bool {
	km := strings.Split(keys[randIndex()], ":")
	rang := []int{rand.Intn(MAXVALUE), rand.Intn(MAXVALUE)}
	sort.Ints(rang)
	return cmdable.ZCount(
		km[0],
		strconv.Itoa(rang[0]),
		strconv.Itoa(rang[1]),
	).Err() == nil
}

func (op *RedisOp)ZScore(cmdable redis.Cmdable) bool {
	km := strings.Split(keys[randIndex()], ":")
	return cmdable.ZScore(km[0], km[1]).Err() == nil
}

func (op *RedisOp)ZRange(cmdable redis.Cmdable) bool {
	km := strings.Split(keys[randIndex()], ":")
	return cmdable.ZRange(km[0], 0, 20).Err() == nil
}

func (op *RedisOp)ZRangeByScore(cmdable redis.Cmdable) bool {
	km := strings.Split(keys[randIndex()], ":")
	rang := []int{rand.Intn(MAXVALUE), rand.Intn(MAXVALUE)}
	sort.Ints(rang)
	return cmdable.ZRangeByScore(
		km[0],
		redis.ZRangeBy{
			Min: strconv.Itoa(rang[0]),
			Max: strconv.Itoa(rang[1]),
		},
	).Err() == nil
}

func (op *RedisOp)ZRevRangeByScore(cmdable redis.Cmdable) bool {
	km := strings.Split(keys[randIndex()], ":")
	rang := []int{rand.Intn(MAXVALUE), rand.Intn(MAXVALUE)}
	sort.Ints(rang)
	return cmdable.ZRevRangeByScore(
		km[0],
		redis.ZRangeBy{
			Max: strconv.Itoa(rang[1]),
			Min: strconv.Itoa(rang[0]),
		},
	).Err() == nil
}

func (op *RedisOp)ZRank(cmdable redis.Cmdable) bool {
	km := strings.Split(keys[randIndex()], ":")
	return cmdable.ZRank(km[0], km[1]).Err() == nil
}

func (op *RedisOp)HGet(cmdable redis.Cmdable) bool {
	kf := strings.Split(keys[randIndex()], ":")
	return cmdable.HGet(kf[0], kf[1]).Err() == nil
}

func (op *RedisOp)HGetAll(cmdable redis.Cmdable) bool {
	kf := strings.Split(keys[randIndex()], ":")
	return cmdable.HGetAll(kf[0]).Err() == nil
}

func (op *RedisOp)HMGet(cmdable redis.Cmdable) bool {
	kf := strings.Split(keys[randIndex()], ":")
	return cmdable.HMGet(kf[0], kf[1], genField(), genField()).Err() == nil
}
