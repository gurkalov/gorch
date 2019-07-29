package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/jmoiron/sqlx"
	"github.com/julienschmidt/httprouter"
	_ "github.com/kshvakov/clickhouse"
	"gorch/models"
	"gorch/packages/batcher"
	"io/ioutil"
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
	db *sqlx.DB
	connect *sql.DB

	fastBatcher batcher.Batcher
	chBatcher *batcher.ClickhouseBatcher
	slowBatcher batcher.Batcher
)

func InitRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:16379", //6379
		Password: "",               // no password set
		DB:       0,                // use default DB
	})

	pong, err := redisClient.Ping().Result()
	fmt.Println(pong, err)
}

func InitStorage() {
	var err error
	db, err = sqlx.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	connect, err = sql.Open("clickhouse", "tcp://127.0.0.1:9000")
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

}

func InitBatcher() {
	buffer := make([]string, 0)
	fastBatcher = &batcher.RedisBatcher{redisClient, "list:fast", batchSize, buffer}
	fastBatcher.Init(1000)
}

func InitBatcherCH() {
	chBatcher = &batcher.ClickhouseBatcher{connect, fastBatcher, 100}
	chBatcher.Init(1000)
}

func Index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprint(w, "Welcome!\n")
}

func Check(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprint(w, "Check")
}

func AddFast(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	var event models.Event
	if err := json.Unmarshal(body, &event); err != nil {
		log.Fatal(err)
	}

	event.Timestamp()

	serializeEvent, err := json.Marshal(event)
	if err != nil {
		log.Fatal(err)
	}
	fastBatcher.Push(string(serializeEvent))

	//tx := db.MustBegin()
	//tx.NamedExec("INSERT INTO events VALUES (:date, :datetime, :unixtime, :user_id, :path, :value)", &event)
	//if err := tx.Commit(); err != nil {
	//	log.Fatal(err)
	//}
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
	InitStorage()
	InitBatcherCH()

	router := httprouter.New()
	router.GET("/", Index)
	router.POST("/add", AddFast)
	router.GET("/fast/pop", PopFast)
	router.GET("/fast/read", ReadFast)

	router.GET("/slow/add", AddSlow)
	router.GET("/slow/batch", BatchSlow)

	router.GET("/check", Check)

	log.Fatal(http.ListenAndServe(":7080", router))
}
