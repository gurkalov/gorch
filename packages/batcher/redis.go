package batcher

import (
	"fmt"
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
	//BufferTicker *time.Ticker
	//BatchTicker  *time.Ticker
}

func (batcher *RedisBatcher) Init(period int64) *time.Ticker {
	tiker := time.NewTicker(time.Duration(period) * time.Millisecond)
	go func() {
		for _ = range tiker.C {
			batcher.Save()
		}
	}()

	return tiker
}

func (batcher *RedisBatcher) Batch(period int64, f func(list []string)) *time.Ticker {
	tiker := time.NewTicker(time.Duration(period) * time.Millisecond)
	go func() {
		for _ = range tiker.C {
			list := batcher.Pop()
			f(list)
		}
	}()

	return tiker
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
	key := batcher.Key

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
		}, key)

		if err != nil {
			fmt.Print(time.Now())
			fmt.Print(" N: ")
			fmt.Print(i)
			fmt.Print(" ")
			fmt.Println(err)
		}
		if err != redis.TxFailedErr {
			break
		}
	}

	return sliceStringList.Val()
}

func (batcher *RedisBatcher) Flush() error {
	batcher.Buff = []string{}
	return batcher.Storage.Del(batcher.Key).Err()
}
