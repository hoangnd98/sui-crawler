CREATE TABLE default.sui_transaction_objects (
    `object_id` FixedString(66),
    `version` UInt64 CODEC(Delta(8), ZSTD(1)),
    `transaction_digest` FixedString(44),
    `input_version` UInt64 CODEC(ZSTD(6)),
    `input_owner` Nullable(FixedString(66)),
    `input_digest` Nullable(FixedString(66)),
    `output_version` UInt64 CODEC(ZSTD(6)),
    `output_owner` Nullable(FixedString(66)),
    `output_digest` Nullable(FixedString(66)),
    `is_created` UInt8,
    `is_deleted` UInt8,
    `timestamp` DateTime CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplacingMergeTree( timestamp
) PARTITION BY toYYYYMM(timestamp) PRIMARY KEY (transaction_digest, object_id)
ORDER BY
    (transaction_digest, object_id, version) SETTINGS index_granularity = 8192