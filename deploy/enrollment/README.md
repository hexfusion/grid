# Grid enrollment service

A standalone service that lets an inference provider join a grid. A provider
submits a certificate signing request, an operator approves it, and the grid
signs a certificate under a grid-assigned name. The service is Kubernetes-free
by default. It holds the grid certificate authority and signs requests directly
rather than through the Kubernetes CSR API, so a provider with no cluster of its
own can use it.

The API surface is defined in `api/enrollment-v1alpha1.yaml`.

## Build

The binary:

```
cargo build --release -p enrollment --bin enrollment
```

The container image. The build context is the repository root:

```
podman build -f deploy/enrollment/Containerfile -t enrollment .
```

## Run

The service reads its configuration from the environment.

Certificate authority, required. The service holds the grid CA and signs with
it. `ENROLLMENT_CA_CERT` and `ENROLLMENT_CA_KEY` are filesystem paths to the CA
certificate and its private key. Treat the key as the secret the grid is defined
by. `ENROLLMENT_CA_COMMON_NAME` names the CA and defaults to `grid-ca`.
`ENROLLMENT_CERT_LIFETIME_SECS` bounds an issued certificate and defaults to the
built-in site lifetime.

Store. `DB_CONNECTION_URL` is a Postgres connection string, named to match MaaS
so a deployment beside it points at the database already there. When it is unset
the records are kept in memory and lost on restart, which suits a local trial
and nothing else.

Listen address. `ENROLLMENT_LISTEN_ADDR` defaults to `0.0.0.0:8080`. The service
serves plaintext HTTP and expects a TLS-terminating front, so do not expose the
port directly.

Operator authorization. `ENROLLMENT_AUTHZ` selects who may approve or deny.
`local`, the default, reads an operator token table from
`ENROLLMENT_OPERATOR_TOKENS`, one `name:token` line per operator, where an empty
table admits nobody and is the safe direction. `kube` defers to Kubernetes RBAC
through TokenReview and SubjectAccessReview and needs a build with
`--features sar`. An unknown value, or a backend this binary was not built with,
fails closed rather than falling back to the token table.

Joining kit. On approval the service hands the member its certificate, its
grid-assigned SPIFFE id, the CA bundle, and the grid-wide gossip transport key
from `ENROLLMENT_GOSSIP_KEY` with the seed peers from `ENROLLMENT_GOSSIP_SEEDS`.
The gossip key is a shared secret, so the join step proves possession of the
enrolled key before the kit is returned.
```
