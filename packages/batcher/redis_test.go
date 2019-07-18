package batcher

import (
	"fmt"
	"github.com/go-redis/redis"
	"os"
	"testing"
)

var (
	redisClient *redis.Client
	redisBatcher Batcher
)

func InitRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := redisClient.Ping().Result()
	fmt.Println(pong, err)
}

func InitBatcher() {
	buffer := make([]string, 0)
	redisBatcher = &RedisBatcher{redisClient, "list:fast", 2, buffer}
}

func tearDown() {
	redisClient.FlushDB()
	redisBatcher.Flush()
}

func TestMain(m *testing.M) {
	InitRedis()
	InitBatcher()

	os.Exit(m.Run())
}

func TestReadEmpty(t *testing.T) {
	tearDown()

	list := redisBatcher.Read()

	if len(list) != 0 {
		t.Errorf("len(list) not equal 0, actual: %d", len(list))
	}
}

func TestReadOne(t *testing.T) {
	tearDown()

	redisBatcher.Push("test")
	redisBatcher.Save()

	list := redisBatcher.Read()

	if len(list) != 1 {
		t.Errorf("len(list) not equal 1, actual: %d", len(list))
	}
}

func TestReadTwo(t *testing.T) {
	tearDown()

	redisBatcher.Push("test0")
	redisBatcher.Push("test1")
	redisBatcher.Save()

	list := redisBatcher.Read()

	if len(list) != 2 {
		t.Errorf("len(list) not equal 2, actual: %d", len(list))
	}
}

func TestPop(t *testing.T) {
	tearDown()

	redisBatcher.Push("test0")
	redisBatcher.Push("test1")
	redisBatcher.Save()

	list := redisBatcher.Pop()

	if len(list) != 2 {
		t.Errorf("len(list) not equal 2, actual: %d", len(list))
	}

	list = redisBatcher.Read()
	if len(list) != 0 {
		t.Errorf("len(list) not equal 0, actual: %d", len(list))
	}
}

func TestFlush(t *testing.T) {
	tearDown()

	redisBatcher.Push("test0")
	redisBatcher.Push("test1")
	redisBatcher.Save()

	list := redisBatcher.Read()
	if len(list) != 2 {
		t.Errorf("len(list) not equal 2, actual: %d", len(list))
	}

	redisBatcher.Flush()
	list = redisBatcher.Read()
	if len(list) != 0 {
		t.Errorf("len(list) not equal 0, actual: %d", len(list))
	}
}

func TestReadAllOverSize(t *testing.T) {
	tearDown()

	redisBatcher.Push("test0")
	redisBatcher.Push("test1")
	redisBatcher.Push("test2")
	redisBatcher.Save()

	list := redisBatcher.Read()
	if len(list) != 3 {
		t.Errorf("len(list) not equal 3, actual: %d", len(list))
	}
}

func TestPopOverSize(t *testing.T) {
	tearDown()

	redisBatcher.Push("test0")
	redisBatcher.Push("test1")
	redisBatcher.Push("test2")
	redisBatcher.Save()

	list := redisBatcher.Pop()
	if len(list) != 2 {
		t.Errorf("len(list) not equal 2, actual: %d", len(list))
	}
	if list[0] != "test0" {
		t.Errorf("len(list) not equal test0, actual: %s", list[0])
	}

	list = redisBatcher.Pop()
	if len(list) != 1 {
		t.Errorf("len(list) not equal 1, actual: %d", len(list))
	}

	if list[0] != "test2" {
		t.Errorf("len(list) not equal test2, actual: %s", list[0])
	}
}

func TestBufferOverSize(t *testing.T) {
	tearDown()

	for i := 0; i < 5; i++ {
		redisBatcher.Push("test0")
	}
	redisBatcher.Save()

	list := redisBatcher.Read()
	if len(list) != 5 {
		t.Errorf("len(list) not equal 5, actual: %d", len(list))
	}
}
