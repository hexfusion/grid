# Site Enrollment

A site joins a grid by redeeming a one-time token at the enrollment service, which returns a certificate carrying a grid-assigned identity.

## Prerequisites

- Kubernetes 1.26 or later, Helm 3.12 or later
- etcd encryption at rest, since the CA signing key is stored in a Secret
- Enrollment images built with `--features sar,bootstrap`

## Install

```bash
helm install grid-enrollment ./charts/grid-enrollment \
  --namespace grid --create-namespace \
  --set route.host=enrollment.apps.example.com \
  --set db.type=external \
  --set db.external.connectionUrlSecretRef=grid-enrollment-db
```

On OpenShift the chart renders a passthrough Route for `route.host`, so remote sites can reach enrollment. `route.enabled` defaults to `auto`, which renders the Route only when the cluster serves `route.openshift.io/v1`. `true` or `false` forces it. Elsewhere, omit `route.host` and front the Service with your own ingress. For GitOps renders with `helm template`, pass `--api-versions route.openshift.io/v1` on OpenShift, or `auto` renders no Route.

A pre-install Job runs `enrollment bootstrap` to create the grid CA and the serving certificates for enrollment and its database. It is idempotent: on upgrade it keeps the CA and re-issues a serving certificate only when a requested name, such as a new `route.host`, is missing. `ca.forceRegenerate` replaces the CA and invalidates every certificate it signed.

## Configure

| Value | Decide |
|---|---|
| `route.host` | Required when a Route renders. Added to the issued serving certificate. A BYO certificate (`serving.existingSecretRef`) must already carry it. |
| `route.tls.termination` | Keep `passthrough`. `edge` and `reencrypt` terminate at the router and break the site's grid-CA pin. |
| `db.type` | `builtin` runs Postgres in the chart. `external` reads the connection URL from the Secret in `db.external.connectionUrlSecretRef`. |
| `enrollment.authz` | `kube` (default) authorizes callers with Kubernetes RBAC on `enrollmenttokens`. `local` uses a grid-admin token table. |
| `image.repository`, `image.tag` | The enrollment image. The tag defaults to the chart's `appVersion`. |

The chart README and `values.yaml` cover the remaining values.

## Verify

Mint a token with the grid CA bundle:

```bash
kubectl -n grid get secret grid-ca-bundle -o jsonpath='{.data.ca\.crt}' | base64 -d > grid-ca-bundle.crt

curl -s -X POST https://enrollment.apps.example.com/v1alpha1/enrollmenttokens \
  --cacert grid-ca-bundle.crt \
  -H "Authorization: Bearer $GRID_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"siteName": "east2", "gridNetworkRef": "my-grid"}'
```

The response returns `tokenId`, a single-use `token`, and `expiresAt`. Give `token` and `grid-ca-bundle.crt` to the site out of band. Revoke an unused token with `DELETE /v1alpha1/enrollmenttokens/$TOKEN_ID`.

On the site, create a key and CSR, then redeem the token:

```bash
openssl ecparam -genkey -name prime256v1 -noout -out site.key
openssl req -new -key site.key -subj "/CN=east2" -out site.csr

jq -n --rawfile csr site.csr '{csr: $csr}' \
  | curl -s -X POST https://enrollment.apps.example.com/v1alpha1/enrollments \
      --cacert grid-ca-bundle.crt \
      -H "Authorization: Bearer $SITE_TOKEN" \
      -H "Content-Type: application/json" \
      -d @-
```

The response returns `certificate`, issued for the site name the token pinned (SANs in the CSR are ignored), `caCertificate`, and `spiffeId`, the identity the site presents to its peers.

## Troubleshooting

- **`route.host is required`**: a passthrough Route is rendering without a host. Set `route.host` to `<name>.apps.<cluster-domain>`, or set `route.enabled=false`. Under an umbrella chart, prefix both with the subchart name.
- **CA bootstrap Job fails with `built without --features bootstrap`**: rebuild the enrollment image with `--features sar,bootstrap`.
- **`TokenReview` or `SubjectAccessReview` calls fail**: `enrollment.authz=kube` needs the `sar` feature and `enrollment.serviceAccount.create=true`, which binds the pod to `system:auth-delegator`.
