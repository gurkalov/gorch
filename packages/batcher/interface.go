package batcher

import (
	"github.com/go-redis/redis"
	"time"
)

type Batcher interface {
	Push(item string) error
	Pop() []string
	Flush() error
	Save() error
	Init(storage *redis.Client, key string)
	Run(d time.Duration) *time.Ticker
	SetSize(s uint64) error
	Batch(d time.Duration, b func(list []string)) *time.Ticker
	Read() []string
	Buffer() []string
}
