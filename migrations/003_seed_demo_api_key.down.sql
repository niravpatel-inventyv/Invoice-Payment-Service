-- 003_seed_demo_api_key.down.sql
-- Removes the demo API key created by migration 003.

DELETE FROM api_keys
WHERE key_prefix = 'dpk_87c5b1db992d221ada071266'
  AND key_hash = 'e77e5292a52e56f59f89c262871542670b92a874393c35591475cc6770fd098e';
