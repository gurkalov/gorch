package main

import (
	"testing"
)

func BenchmarkInit(b *testing.B) {
	InitRedis()
	InitBatcher()
}

func BenchmarkFastPush(b *testing.B) {
	fastBatcher.Flush()
	for i := 0; i < b.N; i++ {
		val := "1"
		fastBatcher.Push(val)
	}
}

func BenchmarkPushAndSave(b *testing.B) {
	fastBatcher.Flush()
	for i := 0; i < b.N; i++ {
		val := "1"
		fastBatcher.Push(val)
	}
	fastBatcher.Save()
}
