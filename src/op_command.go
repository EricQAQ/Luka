package main

import (
	"fmt"
	"time"
	"math/rand"
	"strconv"

	"github.com/satori/go.uuid"
	"github.com/go-redis/redis"
)

const MAXEXP = 60
const MINEXP = 10
const MAXVALUE = 1000000
const KEYRANGE = 10000000

type OpAttr struct {
	opName string
	isWrite bool
}

var opMapping = map[string]OpAttr {
	"set": OpAttr{opName: "Set", isWrite: true},
	"mset": OpAttr{opName: "MSet", isWrite: true},
	"lpush": OpAttr{opName: "LPush", isWrite: true},
	"rpush": OpAttr{opName: "RPush", isWrite: true},
	"sadd": OpAttr{opName: "SAdd", isWrite: true},
	"zadd": OpAttr{opName: "ZAdd", isWrite: true},
	"hset": OpAttr{opName: "HSet", isWrite: true},
	"hmset": OpAttr{opName: "HMSet", isWrite: true},
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

type RedisOp struct {}

func (op *RedisOp)Set(redisClient *redis.Client) bool {
	exp := time.Duration(rand.Intn(MAXEXP - MINEXP) + MINEXP) * time.Second
	key := genKey()
	value := rand.Intn(MAXVALUE)
	return redisClient.Set(key, value, exp).Err() == nil
}

func (op *RedisOp)MSet(redisClient *redis.Client) bool {
	count := (rand.Intn(10) + 1) * 2
	var kv = make([]interface{}, count, count)
	for i := 0; i < count; {
		kv[i] = genKey()
		kv[i+1] = rand.Intn(MAXVALUE)
		i = i + 2
	}
	return redisClient.MSet(kv...).Err() == nil
}

func (op *RedisOp)listPush(redisClient *redis.Client, isLeft bool) error {
	count := rand.Intn(10) + 1
	key := genKey()
	var value = make([]interface{}, count, count)
	for i := 0; i < count; i++ {
		value[i] = rand.Intn(MAXVALUE)
	}
	if isLeft {
		return redisClient.LPush(key, value...).Err()
	}
	return redisClient.RPush(key, value...).Err()
}

func (op *RedisOp)LPush(redisClient *redis.Client) bool {
	return op.listPush(redisClient, true) == nil
}

func (op *RedisOp)RPush(redisClient *redis.Client) bool {
	return op.listPush(redisClient, false) == nil
}

func (op *RedisOp)SAdd(redisClient *redis.Client) bool {
	key := genKey()
	count := rand.Intn(10) + 1
	var value = make([]interface{}, count, count)
	for i := 0; i < count; i++ {
		value[i] = rand.Intn(MAXVALUE)
	}
	return redisClient.SAdd(key, value...).Err() == nil
}

func (op *RedisOp)ZAdd(redisClient *redis.Client) bool {
	key := genKey()
	count := rand.Intn(10) + 1
	var value = make([]redis.Z, count, count)
	for i := 0; i < count; i++ {
		value[i] = redis.Z{
			Score: float64(rand.Intn(MAXVALUE)),
			Member: strconv.Itoa(rand.Intn(MAXVALUE)),
		}
	}
	return redisClient.ZAdd(key, value...).Err() == nil
}

func (op *RedisOp)HSet(redisClient *redis.Client) bool {
	key := genKey()
	field := genField()
	value := rand.Intn(MAXVALUE)
	return redisClient.HSet(key, field, value).Err() == nil
}

func (op *RedisOp)HMSet(redisClient *redis.Client) bool {
	key := genKey()
	count := rand.Intn(10) + 1
	var hashMap = make(map[string]interface{})
	for i := 0; i < count; i++ {
		hashMap[genField()] = rand.Intn(MAXVALUE)
	}
	return redisClient.HMSet(key, hashMap).Err() == nil
}
