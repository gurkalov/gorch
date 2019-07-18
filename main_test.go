package main

import (
	"os"
	"testing"
)

func tearDown() {
	redisClient.FlushDB()
	fastBatcher.Flush()
}

func TestMain(m *testing.M) {
	InitRedis()

	os.Exit(m.Run())
}
