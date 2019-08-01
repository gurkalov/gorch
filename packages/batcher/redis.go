package batcher

import (
	"github.com/go-redis/redis"
	"sync"
	"time"
)

type RedisBatcher struct {
	Storage *redis.Client
	Key string
	Size uint64
	Buff []string
	Mutex *sync.Mutex
}

func (batcher *RedisBatcher) Init(period int64) error {
	go func() {
		for _ = range time.NewTicker(time.Duration(period) * time.Millisecond).C {
			//buff := batcher.Buffer()
			//fmt.Println(t)
			//fmt.Print(" ")
			//fmt.Print(len(buff))

			batcher.Save()
		}
	}()

	return nil
}

func (batcher *RedisBatcher) Batch(period int64, f func(list []string)) error {
	go func() {
		for _ = range time.NewTicker(time.Duration(period) * time.Millisecond).C {
			//batcher.Mutex.Lock()
			list := batcher.Pop()
			f(list)
			//batcher.Mutex.Unlock()

		}
	}()

	return nil
}

func (batcher *RedisBatcher) Push(item string) error {
	batcher.Mutex.Lock()
	batcher.Buff = append(batcher.Buff, item)
	batcher.Mutex.Unlock()

	return nil
}

func (batcher *RedisBatcher) Save() error {
	batcher.Mutex.Lock()
	buffer := batcher.Buffer()
	batcher.Buff = []string{}
	batcher.Mutex.Unlock()

	if err := batcher.Storage.RPush(batcher.Key, buffer).Err(); err != nil {
		return err
	}

	return nil
}

func (batcher *RedisBatcher) SetSize(s uint64) error {
	batcher.Size = s
	return nil
}

func (batcher *RedisBatcher) Buffer() []string {
	return batcher.Buff
}

func (batcher *RedisBatcher) Read() []string {
	length := int64(batcher.Storage.LLen(batcher.Key).Val())
	sliceStringList := batcher.Storage.LRange(batcher.Key, 0, int64(length)).Val()

	return sliceStringList
}

func (batcher *RedisBatcher) Pop() []string {
	length := uint64(batcher.Storage.LLen(batcher.Key).Val())
	getBatchSize := length
	if batcher.Size > 0 && getBatchSize > batcher.Size {
		getBatchSize = batcher.Size
	}

	batcher.Mutex.Lock()
	sliceStringList := batcher.Storage.LRange(batcher.Key, 0, int64(getBatchSize - 1)).Val()
	batcher.Storage.LTrim(batcher.Key, int64(getBatchSize), -1)
	batcher.Mutex.Unlock()

	return sliceStringList
}

func (batcher *RedisBatcher) Flush() error {
	batcher.Buff = []string{}
	return batcher.Storage.Del(batcher.Key).Err()
}

