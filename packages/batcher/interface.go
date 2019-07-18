package batcher

type Batcher interface {
	Push(item string) error
	Pop() []string
	Flush() error
	Save() error
	Init(period int64) error
	Read() []string
	Buffer() []string
}
