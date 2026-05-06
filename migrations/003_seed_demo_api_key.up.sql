-- 003_seed_demo_api_key.up.sql
-- Seeds one demo API key as prefix + SHA-256 hash (no plaintext storage).
-- Demo plaintext key for local testing:
--   dpk_87c5b1db992d221ada071266_7eefd21e1357f8f6d62b24333933dfe8e6a24f4d6e80cd0c10e1f6f28e60d3a1

INSERT INTO api_keys (business_id, key_prefix, key_hash)
SELECT b.id,
       'dpk_87c5b1db992d221ada071266',
       'e77e5292a52e56f59f89c262871542670b92a874393c35591475cc6770fd098e'
FROM businesses b
WHERE b.name = 'Acme Retail Pvt Ltd';
