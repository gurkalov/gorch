package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/julienschmidt/httprouter"
	"gorch/packages/batcher"
	"log"
	"net/http"
	"strconv"
	"time"
)

const (
	batchSize = 10000
)

var (
	redisClient *redis.Client
	fastBatcher batcher.Batcher
	slowBatcher batcher.Batcher
)

func InitRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", //6379
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := redisClient.Ping().Result()
	fmt.Println(pong, err)
}

func InitBatcher() {
	buffer := make([]string, 100)
	fastBatcher = &batcher.RedisBatcher{redisClient,"list:fast", batchSize, buffer}
	//fastBatcher.Init(100)
}

func Index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprint(w, "Welcome!\n")
}

func Check(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprint(w, "Check")
}

func AddFast(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	val := time.Now().Nanosecond() / 1000000
	fastBatcher.Push(strconv.Itoa(val))
}

func PopFast(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprint(w, fastBatcher.Pop())
}

func ReadFast(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprint(w, fastBatcher.Read())
}

func AddSlow(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	val := time.Now().Nanosecond() / 1000000
	slowBatcher.Push(strconv.Itoa(val))
}

func BatchSlow(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprint(w, slowBatcher.Pop())
}

func main() {
	InitRedis()
	InitBatcher()

	router := httprouter.New()
	router.GET("/", Index)
	router.GET("/fast/add", AddFast)
	router.GET("/fast/pop", PopFast)
	router.GET("/fast/read", ReadFast)

	router.GET("/slow/add", AddSlow)
	router.GET("/slow/batch", BatchSlow)

	router.GET("/check", Check)

	log.Fatal(http.ListenAndServe(":7080", router))
}
