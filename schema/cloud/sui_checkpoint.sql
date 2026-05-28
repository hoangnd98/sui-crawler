CREATE TABLE default.sui_checkpoints (
    `sequence_number` UInt64 CODEC(Delta(8), ZSTD(1)),
    `digest` FixedString(44),
    `previous_checkpoint_digest` FixedString(44),
    `network_total_transactions` UInt32 CODEC(T64, ZSTD(1)),
    `timestamp` DateTime CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplacingMergeTree(timestamp) 
PARTITION BY toYYYYMMDD(timestamp) PRIMARY KEY sequence_number
ORDER BY sequence_number SETTINGS index_granularity = 8192