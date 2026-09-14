-- Reverses 0003_add_issued_via.up.sql, dropping the auto-issue provenance column.
ALTER TABLE enrollment_requests
    DROP COLUMN IF EXISTS issued_via;
