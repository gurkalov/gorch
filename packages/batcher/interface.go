package batcher

import "time"

type Batcher interface {
	Push(item string) error
	Pop() []string
	Flush() error
	Save() error
	Init(period int64) *time.Ticker
	SetSize(s uint64) error
	Batch(period int64, b func(list []string)) *time.Ticker
	Read() []string
	Buffer() []string
}
