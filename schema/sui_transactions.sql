CREATE TABLE IF NOT EXISTS sui_transactions
(
    digest                          String,
    checkpoint_sequence_number      Int64,
    epoch                           Int64,
    timestamp_ms                    Int64,
    sender                          String,
    gas_budget                      String,
    gas_price                       String,
    gas_computation_cost            String,
    gas_storage_cost                String,
    gas_storage_rebate              String,
    gas_non_refundable_storage_fee  String,
    status                          String,
    transaction_kind                String,
    inputs_json                     String,
    raw_transaction                 String,
    raw_effects                     String
)
ENGINE = MergeTree()
ORDER BY (checkpoint_sequence_number, digest)
PARTITION BY intDiv(checkpoint_sequence_number, 1000000)
SETTINGS index_granularity = 8192;
