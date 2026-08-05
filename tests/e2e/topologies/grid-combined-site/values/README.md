# Helm Values

This directory owns demo-specific values for the Grid operator and the
consumer and provider Praxis gateway releases at each site.

Each site should render three releases from structured values:

- one Grid operator;
- one consumer gateway without provider credentials;
- one provider gateway with only its local provider credentials.

Image references must remain swappable through the existing
`GRID_XTASK_*_IMAGE` environment variables. Published quickstart values should
use a mutually compatible, immutable image set.
