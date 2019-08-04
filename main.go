package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/julienschmidt/httprouter"
	_ "github.com/kshvakov/clickhouse"
	"gorch/models"
	"gorch/packages/batcher"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

const (
	batchSize = 10000
)

var mutex = &sync.Mutex{}

var (
	redisClient *redis.Client
	connect     *sql.DB

	fastBatcher batcher.Batcher
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
	connect, err = sql.Open("clickhouse", "tcp://127.0.0.1:9000")
	if err != nil {
		log.Fatal(err)
	}

	if err := connect.Ping(); err != nil {
		log.Fatal(err)
	}
}

func InitBatcher() {
	fastBatcher = new(batcher.RedisBatcher)
	fastBatcher.Init(redisClient, "list:fast")
	fastBatcher.Run(10 * time.Millisecond)

	fastBatcher.Batch(1000 * time.Millisecond, func(buff []string) {
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

func Check(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprint(w, "Check")
}

func Add(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
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

func main() {
	InitRedis()
	InitBatcher()
	InitStorage()

	router := httprouter.New()
	router.POST("/add", Add)
	router.GET("/check", Check)

	log.Fatal(http.ListenAndServe(":7080", router))
}
