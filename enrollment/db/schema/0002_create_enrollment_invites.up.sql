-- Schema for grid enrollment: 0002_create_enrollment_invites.up.sql
-- Description: Invitations to enrol. An operator issues one before a provider
-- can ask, so a pending request is something expected rather than something
-- discovered.

CREATE TABLE IF NOT EXISTS enrollment_invites (
    id           UUID PRIMARY KEY,
    -- The token is kept as a digest. A timing difference reveals nothing about a
    -- valid token, and the table is not a list of usable credentials at rest.
    token_sha256 TEXT NOT NULL,
    -- When set, the invited provider may only ask for this name, so the operator
    -- chose it before the provider ever spoke.
    site_name    TEXT,
    grid_network_ref TEXT NOT NULL,
    issued_by    TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL,
    -- Set when redeemed. An invite buys one request, not a standing right to ask.
    redeemed_at  TIMESTAMPTZ,
    redeemed_by  UUID,

    CONSTRAINT enrollment_invites_redeemed_together
        CHECK ((redeemed_at IS NULL) = (redeemed_by IS NULL))
);

-- Redemption looks a token up by digest on every submission.
CREATE UNIQUE INDEX IF NOT EXISTS idx_enrollment_invites_token
    ON enrollment_invites (token_sha256);

-- Operators list what is outstanding.
CREATE INDEX IF NOT EXISTS idx_enrollment_invites_created
    ON enrollment_invites (created_at DESC);
