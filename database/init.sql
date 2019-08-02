CREATE TABLE IF NOT EXISTS events
(
    date Date,
    datetime DateTime,
    unixtime UInt64,
    user_id UInt32,
    body_id UInt32,
    service String,
    section String,
    action String,
    model String,
    model_id UInt32,
    param String,
    value String,
    message String
)
ENGINE = MergeTree
PARTITION BY date
PRIMARY KEY user_id
ORDER BY (user_id, unixtime);
