CREATE TABLE IF NOT EXISTS sui_events
(
    transaction_digest          String,
    checkpoint_sequence_number  Int64,
    epoch                       Int64,
    timestamp_ms                Int64,
    event_seq                   Int64,
    package_id                  String,
    module_name                 String,
    event_type                  String,
    sender                      String,
    parsed_json                 String,
    bcs                         String
)
ENGINE = MergeTree()
ORDER BY (checkpoint_sequence_number, transaction_digest, event_seq)
PARTITION BY intDiv(checkpoint_sequence_number, 1000000)
SETTINGS index_granularity = 8192;
