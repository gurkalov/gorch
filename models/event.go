package models

import (
	"encoding/json"
	"time"
)

type Event struct {
	Date     string `json:"date" db:"date"`
	Datetime string `json:"datetime" db:"datetime"`
	Unixtime uint64 `json:"unixtime" db:"unixtime"`
	UserId   uint32 `json:"user_id" db:"user_id"`
	Path     string `json:"path" db:"path"`
	Value    string `json:"value" db:"value"`
}

func (event Event) toString() string {
	result, _ := json.Marshal(event)
	return string(result)
}

func (event *Event) Timestamp() error {
	nowTime := time.Now()

	event.Date = nowTime.Format("2006-01-02")
	event.Datetime = nowTime.Format("2006-01-02 15:04:05")
	event.Unixtime = uint64(nowTime.UnixNano() / 1000)

	return nil
}
