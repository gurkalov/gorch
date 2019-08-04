package batcher

import (
	"github.com/go-redis/redis"
	"sync"
	"time"
)

type RedisBatcher struct {
	Storage      *redis.Client
	Key          string
	Size         uint64
	Buff         []string
	Mutex        *sync.Mutex
}

func (batcher *RedisBatcher) Init(storage *redis.Client, key string) {
	batcher.Storage = storage
	batcher.Key = key
	batcher.Size = 0

	batcher.Buff = make([]string, 0)
	batcher.Mutex = &sync.Mutex{}
}

func (batcher *RedisBatcher) Run(d time.Duration) *time.Ticker {
	ticker := time.NewTicker(d)
	go func() {
		for _ = range ticker.C {
			batcher.Save()
		}
	}()

	return ticker
}

func (batcher *RedisBatcher) Batch(d time.Duration, f func(list []string)) *time.Ticker {
	ticker := time.NewTicker(d)
	go func() {
		for _ = range ticker.C {
			f(batcher.Pop())
		}
	}()

	return ticker
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
	var sliceStringList *redis.StringSliceCmd

	for i := 0; i < 5; i++ {
		err := batcher.Storage.Watch(func(tx *redis.Tx) error {
			length := uint64(tx.LLen(batcher.Key).Val())
			getBatchSize := length
			if batcher.Size > 0 && getBatchSize > batcher.Size {
				getBatchSize = batcher.Size
			}

			_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
				sliceStringList = pipe.LRange(batcher.Key, 0, int64(getBatchSize-1))
				pipe.LTrim(batcher.Key, int64(getBatchSize), -1)
				return nil
			})

			return err
		}, batcher.Key)

		if err != redis.TxFailedErr {
			break
		}
	}

	return sliceStringList.Val()
}

func (batcher *RedisBatcher) Flush() error {
	batcher.Buff = []string{}
	if batcher.Storage == nil {
		return nil
	}
	return batcher.Storage.Del(batcher.Key).Err()
}
