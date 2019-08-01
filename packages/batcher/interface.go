package batcher

type Batcher interface {
	Push(item string) error
	Pop() []string
	Flush() error
	Save() error
	Init(period int64) error
	SetSize(s uint64) error
	Batch(period int64, b func(list []string)) error
	Read() []string
	Buffer() []string
}
