-- Schema for grid enrollment: 0001_create_enrollment_requests.up.sql
-- Description: Requests to join a grid, and what was decided about them.
-- The record of why a site is trusted, which a hand-edited allow list cannot answer.

CREATE TABLE IF NOT EXISTS enrollment_requests (
    id                UUID PRIMARY KEY,
    site_name         TEXT NOT NULL,
    grid_network_ref  TEXT NOT NULL,
    -- Kept so approval can sign the request the submitter actually made,
    -- rather than one reconstructed later.
    csr_pem           TEXT NOT NULL,
    -- Names the key rather than the certificate, so it survives reissue.
    public_key_sha256 TEXT NOT NULL,
    phase             TEXT NOT NULL DEFAULT 'pending',
    egress            JSONB,
    capabilities      JSONB,
    certificate       TEXT,
    spiffe_id         TEXT,
    reason            TEXT,
    decided_by        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at        TIMESTAMPTZ,

    CONSTRAINT enrollment_requests_phase_check
        CHECK (phase IN ('pending', 'issued', 'denied', 'failed')),

    -- An issued row has to carry what issuing produced. Without this a bug could
    -- record a member as admitted while holding no certificate for it.
    CONSTRAINT enrollment_requests_issued_is_complete
        CHECK (phase <> 'issued'
               OR (certificate IS NOT NULL AND spiffe_id IS NOT NULL AND decided_by IS NOT NULL)),

    -- A decision has a time, and a pending request has not been decided.
    CONSTRAINT enrollment_requests_decided_at_matches_phase
        CHECK ((phase = 'pending') = (decided_at IS NULL))
);

-- One member per name, enforced by the database rather than by the process.
-- Two services sharing this database cannot both admit the same site name.
CREATE UNIQUE INDEX IF NOT EXISTS idx_enrollment_requests_issued_site
    ON enrollment_requests (site_name) WHERE phase = 'issued';

-- Listing is newest first.
CREATE INDEX IF NOT EXISTS idx_enrollment_requests_created
    ON enrollment_requests (created_at DESC);

-- Operators filter by phase to find what still needs deciding.
CREATE INDEX IF NOT EXISTS idx_enrollment_requests_phase
    ON enrollment_requests (phase);
