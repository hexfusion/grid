-- Schema for grid enrollment: 0003_add_issued_via.up.sql
-- Description: Provenance for auto-issued rows. Auto-issue removed the human who
-- would have been recorded as the approver, so the issued row names the invite
-- its redemption spent. Tracing a certificate back to the invite that authorized
-- it is what lets an operator scope the fallout of a leaked invite.

ALTER TABLE enrollment_requests
    ADD COLUMN IF NOT EXISTS issued_via UUID;
