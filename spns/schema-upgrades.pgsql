
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS subaccount_tag BYTEA;
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS subaccount_sig BYTEA;
ALTER TABLE subscriptions DROP COLUMN IF EXISTS subkey_tag;

-- vim:ft=sql
