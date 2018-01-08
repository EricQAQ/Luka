package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"reflect"

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

var keys []string

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
}

func genKey() string {
	return fmt.Sprintf("%s-%s-%d",
					   uuid.NewV4().String(),
					   "set",
					   rand.Intn(KEYRANGE))
}

func genField() string {
	return fmt.Sprintf("%s", uuid.NewV4().String())
}

type RedisOp struct {
	op_name string
}

func (op *RedisOp)Set(cmdable redis.Cmdable) bool {
	key := genKey()
	value := rand.Intn(MAXVALUE)
	keys = append(keys, key)
	return cmdable.Set(key, value, 0).Err() == nil
}

func (op *RedisOp)MSet(cmdable redis.Cmdable) bool {
	count := (rand.Intn(10) + 1) * 2
	var kv = make([]interface{}, count, count)
	for i := 0; i < count; {
		kv[i] = genKey()	// key
		kv[i+1] = rand.Intn(MAXVALUE)	// value
		keys = append(keys,kv[i].(string))
		i = i + 2
	}
	return cmdable.MSet(kv...).Err() == nil
}

func (op *RedisOp)listPush(cmdable redis.Cmdable, isLeft bool) error {
	count := rand.Intn(10) + 1
	key := genKey()
	var value = make([]interface{}, count, count)
	for i := 0; i < count; i++ {
		value[i] = rand.Intn(MAXVALUE)
	}
	keys = append(keys, key)
	if isLeft {
		return cmdable.LPush(key, value...).Err()
	}
	return cmdable.RPush(key, value...).Err()
}

func (op *RedisOp)LPush(cmdable redis.Cmdable) bool {
	return op.listPush(cmdable, true) == nil
}

func (op *RedisOp)RPush(cmdable redis.Cmdable) bool {
	return op.listPush(cmdable, false) == nil
}

func (op *RedisOp)SAdd(cmdable redis.Cmdable) bool {
	key := genKey()
	count := rand.Intn(10) + 1
	var value = make([]interface{}, count, count)
	for i := 0; i < count; i++ {
		value[i] = rand.Intn(MAXVALUE)
	}
	keys = append(keys, key)
	return cmdable.SAdd(key, value...).Err() == nil
}

func (op *RedisOp)ZAdd(cmdable redis.Cmdable) bool {
	key := genKey()
	count := rand.Intn(10) + 1
	var value = make([]redis.Z, count, count)
	for i := 0; i < count; i++ {
		value[i] = redis.Z{
			Score: float64(rand.Intn(MAXVALUE)),
			Member: strconv.Itoa(rand.Intn(MAXVALUE)),
		}
	}
	keys = append(keys, key)
	return cmdable.ZAdd(key, value...).Err() == nil
}

func (op *RedisOp)HSet(cmdable redis.Cmdable) bool {
	key := genKey()
	field := genField()
	value := rand.Intn(MAXVALUE)
	keys = append(keys, fmt.Sprintf("%s:%s", key, field))
	return cmdable.HSet(key, field, value).Err() == nil
}

func (op *RedisOp)HMSet(cmdable redis.Cmdable) bool {
	key := genKey()
	count := rand.Intn(10) + 1
	var hashMap = make(map[string]interface{})
	for i := 0; i < count; i++ {
		field := genField()
		hashMap[field] = rand.Intn(MAXVALUE)
		keys = append(keys, fmt.Sprintf("%s:%s", key, field))
	}
	return cmdable.HMSet(key, hashMap).Err() == nil
}

func (op *RedisOp)FillUpData(redisClient *redis.Client, total int) bool {
	totalCount := total / 10
	round := totalCount / FILLUPPIPELINE + 1
	redisOp := opMapping[op.op_name]
	pipe := redisClient.Pipeline()
	fc := reflect.ValueOf(op).MethodByName(redisOp.writeFunc)
	rc := make([]reflect.Value, 0)
	rc = append(rc, reflect.ValueOf(pipe))

	for i := 0; i < round; i++ {
		for j := 0; j < FILLUPPIPELINE; j++ {
			fc.Call(rc)
		}
		_, err := pipe.Exec()
		if err != nil { return false }
	}
	return true
}

func (op *RedisOp)Get(redisClient *redis.Client) bool {
	index := rand.Intn(len(keys) - 1)
	return redisClient.Get(keys[index]).Err() == nil
}

func (op *RedisOp)MGet(redisClient *redis.Client) bool {
	count := rand.Intn(10) + 1
	var k = make([]string, count, count)
	for i := 0; i < count; i++ {
		k[i] = keys[rand.Intn(len(keys) - 1)]
	}
	return redisClient.MGet(k...).Err() == nil
}
