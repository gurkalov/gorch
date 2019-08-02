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
	"sync"
	"time"
)

const (
	batchSize = 10000
)

var mutex = &sync.Mutex{}

var (
	redisClient *redis.Client
	db          *sqlx.DB
	connect     *sql.DB

	fastBatcher batcher.Batcher
	slowBatcher batcher.Batcher
)

func InitRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:16379", //6379
		Password: "",                // no password set
		DB:       0,                 // use default DB
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

	var mutex = &sync.Mutex{}
	fastBatcher = &batcher.RedisBatcher{redisClient, "list:fast", batchSize, buffer, mutex}
	fastBatcher.Init(100)

	fastBatcher.Batch(1000, func(buff []string) {
		var (
			tx, _   = connect.Begin()
			stmt, _ = tx.Prepare("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		)
		defer stmt.Close()

		var event models.Event

		for i := range buff {
			if err := json.Unmarshal([]byte(buff[i]), &event); err != nil {
				log.Fatal(err)
			}

			if _, err := stmt.Exec(
				event.Date,
				event.Datetime,
				event.Unixtime,
				event.UserId,
				event.BodyId,
				event.Service,
				event.Section,
				event.Action,
				event.Model,
				event.ModelId,
				event.Param,
				event.Value,
				event.Message,
			); err != nil {
				log.Fatal(err)
			}
		}

		if err := tx.Commit(); err != nil {
			log.Fatal(err)
		}
	})
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
	mutex.Lock()
	fastBatcher.Push(string(serializeEvent))
	mutex.Unlock()
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
	//InitBatcherCH()

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
