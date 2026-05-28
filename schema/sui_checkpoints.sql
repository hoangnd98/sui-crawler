CREATE TABLE IF NOT EXISTS sui_checkpoints
(
    sequence_number            Int64,
    digest                     String,
    epoch                      Int64,
    timestamp_ms               Int64,
    previous_digest            String,
    network_total_transactions Int64,
    transaction_count          Int32,
    computation_cost           String,
    storage_cost               String,
    storage_rebate             String,
    non_refundable_storage_fee String,
    end_of_epoch               Bool
)
ENGINE = MergeTree()
ORDER BY (sequence_number)
PARTITION BY intDiv(sequence_number, 1000000)
SETTINGS index_granularity = 8192;
