ALTER TABLE default.sui_transactions
    ADD COLUMN IF NOT EXISTS `inputs_json` String AFTER `commands_json`;
