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
	BodyId   uint32 `json:"body_id" db:"body_id"`
	Service  string `json:"service" db:"service"`
	Section  string `json:"section" db:"section"`
	Action   string `json:"action" db:"action"`
	Model    string `json:"model" db:"model"`
	ModelId  uint32 `json:"model_id" db:"model_id"`
	Param    string `json:"param" db:"param"`
	Value    string `json:"value" db:"value"`
	Message  string `json:"message" db:"message"`
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
