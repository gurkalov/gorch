CREATE TABLE IF NOT EXISTS events
(
    date Date,
    datetime DateTime,
    unixtime UInt64,
    user_id UInt32,
    path String,
    value String
)
ENGINE = MergeTree
PARTITION BY date
ORDER BY (unixtime, user_id);
