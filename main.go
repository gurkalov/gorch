package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/jmoiron/sqlx"
	"github.com/julienschmidt/httprouter"
	_ "github.com/kshvakov/clickhouse"
	"gorch/packages/batcher"
	"log"
	"net/http"
	"strconv"
	"time"
)

const (
	batchSize = 10000
)

type Event struct {
	Date     string `json:"date"`
	Time string `json:"time"`
	Unixtime uint64 `json:"unixtime"`
	UserId   uint32 `json:"user_id"`
	Path     string `json:"path"`
	Value    string `json:"value"`
}

type EventDB struct {
	Date     string `db:"date"`
	Time string `db:"time"`
	Unixtime uint64 `db:"unixtime"`
	UserId   uint32 `db:"user_id"`
	Path     string `db:"path"`
	Value    string `db:"value"`
}

var (
	redisClient *redis.Client
	db *sqlx.DB
	fastBatcher batcher.Batcher
	slowBatcher batcher.Batcher
)

func InitRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", //6379
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
}

func InitBatcher() {
	buffer := make([]string, 100)
	fastBatcher = &batcher.RedisBatcher{redisClient, "list:fast", batchSize, buffer}
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

	event := EventDB{
		Date: "2017-06-15",
		Time: "2017-06-15 23:00:00",
		Unixtime: 1,
		UserId: 1,
		Path: "rrr",
		Value: "eef",
	}

	tx := db.MustBegin()
	tx.NamedExec("INSERT INTO events VALUES (:date, :time, :unixtime, :user_id, :path, :value)", &event)
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
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
