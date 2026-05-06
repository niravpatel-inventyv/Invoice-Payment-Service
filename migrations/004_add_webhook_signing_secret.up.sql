ALTER TABLE webhook_endpoints
ADD COLUMN IF NOT EXISTS signing_secret TEXT;

UPDATE webhook_endpoints
SET signing_secret = secret_hash
WHERE signing_secret IS NULL;

ALTER TABLE webhook_endpoints
ALTER COLUMN signing_secret SET NOT NULL;