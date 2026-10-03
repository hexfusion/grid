-- Schema for grid enrollment: 0003_site_enrollment_renewal.up.sql
-- Description: A site renews its identity with its current key. The record keeps
-- the key it replaced so a renewal whose response was lost can retry. A reserved
-- name, issued by bootstrap, has no site token and gets its record on first renewal.

ALTER TABLE site_enrollments
    ADD COLUMN IF NOT EXISTS previous_public_key_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS renewed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reserved BOOLEAN NOT NULL DEFAULT FALSE,
    ALTER COLUMN site_token_id DROP NOT NULL;

-- Only a reserved name's record may lack the token that enrolled it.
DO $$
BEGIN
    ALTER TABLE site_enrollments
        ADD CONSTRAINT site_enrollments_token_or_reserved
        CHECK (site_token_id IS NOT NULL OR reserved);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$$;
