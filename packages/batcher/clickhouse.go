package batcher

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"gorch/models"
	"log"
	"time"
)

type ClickhouseBatcher struct {
	DB *sql.DB
	Batcher Batcher
	Size uint64
}

func (batcher *ClickhouseBatcher) Init(period int64) error {
	f := func() {
		for t := range time.NewTicker(time.Duration(period) * time.Millisecond).C {
			//buff := batcher.Batcher.Pop()
			log.Println(t)

			batcher.Save()
		}
	}

	go f()

	return nil
}

func (batcher *ClickhouseBatcher) Save() error {
	buff := batcher.Batcher.Pop()

	fmt.Println("Write CH ")
	fmt.Print(len(buff))

	var (
		tx, _   = batcher.DB.Begin()
		stmt, _ = tx.Prepare("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)")
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
				event.Path,
				event.Value,
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	return nil
}

//func (batcher *ClickhouseBatcher) Save2() error {
//	buff := batcher.Batcher.Pop()
//
//
//	var event models.Event
//	tx := batcher.DB.MustBegin()
//	for i := range buff {
//		if err := json.Unmarshal([]byte(buff[i]), &event); err != nil {
//			log.Fatal(err)
//		}
//		//NamedQuery
//		tx.NamedQuery("INSERT INTO events VALUES (:date, :datetime, :unixtime, :user_id, :path, :value)", &event)
//		//tx.MustExec("INSERT INTO events VALUES ($1, $2, $3, $4, $5, $6)",
//		//	event.Date,
//		//	event.Datetime,
//		//	event.Unixtime,
//		//	event.UserId,
//		//	event.Path,
//		//	event.Value,
//		//)
//	}
//
//	if err := tx.Commit(); err != nil {
//		log.Fatal(err)
//	}
//
//	return nil
//}
