CREATE TABLE default.sui_objects (
    `object_id` FixedString(66),
    `version` UInt64 CODEC(Delta(8), ZSTD(1)),
    `type_` String,
    `coin_balance` UInt64,
    `display_name` Nullable(String),
    `display_image_url` Nullable(String),
    `owner_type` String,
    `owner_address` Nullable(FixedString(66)),
    `timestamp` DateTime CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplacingMergeTree(timestamp) 
PARTITION BY toYYYYMM(timestamp) PRIMARY KEY (object_id, version)
ORDER BY
    (object_id, version) SETTINGS index_granularity = 8192