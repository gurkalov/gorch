package batcher

import (
	"fmt"
	"github.com/go-redis/redis"
	"os"
	"sync"
	"testing"
	"time"
)

const batcherRedisKey = "list:fast"

var (
	redisClient  *redis.Client
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
	redisBatcher = new(RedisBatcher)
	redisBatcher.Init(redisClient, batcherRedisKey)
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

	redisBatcher.SetSize(2)
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

func TestBufferRaceCondition(t *testing.T) {
	tearDown()

	for i := 0; i < 10000; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				redisBatcher.Push("test0")
			}
		}()
	}
	time.Sleep(1000 * time.Millisecond)
	redisBatcher.Save()

	list := redisBatcher.Read()
	if len(list) != 100000 {
		t.Errorf("len(list) not equal 100000, actual: %d", len(list))
	}
}

func TestBufferBatchRaceCondition(t *testing.T) {
	tearDown()

	redisBatcher.SetSize(100000)
	bufferTicker := redisBatcher.Run(1)

	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				redisBatcher.Push("test0")
			}
		}()
	}
	wg.Wait()

	time.Sleep(500 * time.Millisecond)
	bufferTicker.Stop()

	list := redisBatcher.Read()

	if len(list) != 1000000 {
		t.Errorf("len(list) not equal 1000000, actual: %d", len(list))
	}
}

func TestBatchRaceCondition(t *testing.T) {
	tearDown()

	redisBatcher.SetSize(100000)
	bufferTicker := redisBatcher.Run(100 * time.Millisecond)

	redisClient.Set("check", 0, 0)
	batchTicker := redisBatcher.Batch(10 * time.Millisecond, func(b []string) {
		c, _ := redisClient.Get("check").Int()
		redisClient.Set("check", c+len(b), 0)
	})

	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				redisBatcher.Push("test0")
			}
		}()
	}
	wg.Wait()

	time.Sleep(500 * time.Millisecond)
	bufferTicker.Stop()
	batchTicker.Stop()

	list := redisBatcher.Read()

	c, _ := redisClient.Get("check").Int()
	if c != 100000 {
		t.Errorf("c not equal 100000, actual: %d", c)
	}

	if len(list) != 0 {
		t.Errorf("len(list) not equal 0, actual: %d", len(list))
	}
}

func TestLoadBatchRaceConditionInt(t *testing.T) {
	tearDown()

	redisBatcher.SetSize(1000000)
	bufferTicker := redisBatcher.Run(1 * time.Millisecond)

	sum := 0
	batchTicker := redisBatcher.Batch(100 * time.Millisecond, func(b []string) {
		sum += len(b)
	})

	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				redisBatcher.Push("test0")
			}
		}()
	}
	wg.Wait()
	time.Sleep(500 * time.Millisecond)
	bufferTicker.Stop()
	batchTicker.Stop()

	list := redisBatcher.Read()
	if sum != 1000000 {
		t.Errorf("sum not equal 1000000, actual: %d", sum)
	}

	if len(list) != 0 {
		t.Errorf("len(list) not equal 0, actual: %d", len(list))
	}
}

func TestLoadBatchRaceConditionRedisKey(t *testing.T) {
	tearDown()

	redisBatcher.SetSize(10000)
	bufferTicker := redisBatcher.Run(1 * time.Millisecond)

	redisClient.Set("check", 0, 0)
	batchTicker := redisBatcher.Batch(1 * time.Millisecond, func(b []string) {
		c, _ := redisClient.Get("check").Int()
		redisClient.Set("check", c + len(b), 0)
	})

	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				redisBatcher.Push("test0")
			}
		}()
	}
	wg.Wait()
	time.Sleep(500 * time.Millisecond)
	bufferTicker.Stop()
	batchTicker.Stop()

	list := redisBatcher.Read()

	c, _ := redisClient.Get("check").Int()
	if c != 1000000 {
		t.Errorf("c not equal 1000000, actual: %d", c)
	}

	if len(list) != 0 {
		t.Errorf("len(list) not equal 0, actual: %d", len(list))
	}
}
