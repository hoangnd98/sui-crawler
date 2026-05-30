CREATE TABLE default.sui_transactions (
    `digest` FixedString(44),
    `checkpoint_sequence_number` UInt64 CODEC(Delta(8), ZSTD(1)),
    `timestamp` DateTime CODEC(DoubleDelta, ZSTD(1)),
    `sender` Nullable(FixedString(66)),
    `status` UInt8,
    `kind_typename` String CODEC(ZSTD(6)),
    `commands_json` String,
    `inputs_json` String,
    `events_json` String,
    `balance_changes_json` String,
    `gas_fee` Int64,
    INDEX idx_sui_txn_sender sender TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = ReplacingMergeTree(timestamp
) PARTITION BY toYYYYMM(timestamp) PRIMARY KEY digest
ORDER BY
    digest SETTINGS index_granularity = 8192
