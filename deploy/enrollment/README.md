# Grid enrollment service

Runs beside MaaS and uses the Postgres already deployed there.

## What it needs

Three secrets in `models-as-a-service`:

`grid-ca` holds the grid certificate authority as `ca.crt` and `tls.key`. This
is the authority the grid is defined by, so treat the key accordingly.

`grid-enrollment-operators` holds `operators.txt`, one `name:token` line per
operator allowed to decide. An empty file means nobody can approve, which is the
safe direction.

`maas-db-config` already exists and carries `DB_CONNECTION_URL`. Nothing needs
creating.

Optionally `grid-swim-key` holds the gossip transport key as `key`, handed to a
member along with its certificate so it does not reach a new member out of band.

## Applying

```
kubectl apply -k deploy/enrollment
```

## It holds no cluster permissions

The service account exists to run the pod and nothing else. The service issues
certificates and records decisions; it never creates Kubernetes resources.
`gridctl enrollment export` prints what a cluster should act on, and an operator
applies it.

That keeps the service usable by a provider with no cluster of its own, which is
the case enrollment exists for.
