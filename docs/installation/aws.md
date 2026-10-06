# Installing on AWS

A grid of three single-node OpenShift clusters on AWS, one acting as the hub. Written from
a build on 2026-10-05, so every value below is one that was actually used rather than one
that should work.

What AWS changes, in one sentence each: gossip needs a network load balancer because the
default carries no UDP, a peer arrives from its cluster's egress address rather than from a
private CIDR, and the enrollment endpoint has to be a DNS name.

## Clusters

Single-node OpenShift, one control-plane replica and no workers, which carries both roles.
Pin each cluster to **one** availability zone, and give each cluster a different one.
Spanning zones costs a NAT gateway and an Elastic IP per zone, and three clusters at the
installer's three-zone default ask for twelve Elastic IPs against a default regional limit
of sixteen, most of which an existing cluster is usually holding. Pinning takes it to two
per cluster.

```yaml
controlPlane:
  replicas: 1
  platform:
    aws:
      zones: ["us-west-2b"]
compute:
  - replicas: 0
    platform:
      aws:
        zones: ["us-west-2b"]
publish: External
```

`publish: External` needs an existing public Route53 hosted zone for the base domain. The
installer checks the zone exists, not that its delegation is live, so a zone created
minutes earlier is enough to install and the delegation only has to work before anything
dials the clusters by name.

One thing to decide at install time rather than later: the installer gives every cluster
`10.0.0.0/16` by default, so three clusters cannot be peered without renumbering. Set a
distinct `machineNetwork` per cluster if private cross-site traffic matters. Otherwise
gossip and signals cross the public internet between NAT addresses, authenticated but
neither private nor free.

## Endpoints

`publish: External` puts the API on an internet-facing load balancer and the console,
OAuth endpoint and `kubeadmin` password behind an internet-facing router. Restrict both
before installing anything.

The API load balancer carries its own security group, named `<infraID>-apiserver-lb`, and
that group is **not** tagged with the cluster, so a scan that filters only on
`kubernetes.io/cluster/<infraID>` misses it. Find groups by that tag **or** by the
`<infraID>` name prefix, and enumerate every CIDR on a port rather than the first.

Authorise the allowlist before revoking the world-open rule, so a failure between the two
leaves the cluster reachable rather than locked out:

```bash
aws ec2 authorize-security-group-ingress --group-id <sg> --protocol tcp --port 6443 --cidr <you>/32
aws ec2 revoke-security-group-ingress    --group-id <sg> --protocol tcp --port 6443 --cidr 0.0.0.0/0
```

Leave ICMP types 3 and 4 alone; revoking them breaks path MTU discovery.

The hub's API also has to accept each site, because the klusterlet and the enrollment
client dial it from the site's own egress address. Add each cluster's NAT gateway public
address, not its VPC CIDR:

```bash
aws ec2 describe-nat-gateways --filter Name=vpc-id,Values=<vpc> \
  --query 'NatGateways[].NatGatewayAddresses[].PublicIp' --output text
```

## The grid

Two values carry everything AWS-specific.

`platform: aws` asks for a network load balancer on the grid's Services. The default on AWS
is a Classic load balancer, which carries no UDP, so gossip would get a listener that
cannot work.

`peers` takes the other sites' addresses, separated by spaces or commas. Each becomes a
SWIM seed at the SWIM port, and each bare IPv4 address becomes a host route in the
Services' source ranges, which are the same addresses written twice. One flag needs
neither braces nor escaped commas, which `--set` otherwise requires for a list.

Enrollment on the hub, whose namespace must exist first because the bootstrap Job writes
the hub's identity into it:

```bash
kubectl create namespace grid

helm install grid charts/grid-enrollment -n grid-enroll --create-namespace \
  --set host=enrollment.apps.hub.example.com \
  --set route.host=enrollment.apps.hub.example.com \
  --set hubSite.name=hub \
  --set image.digest=sha256:<enrollment digest>
```

`host` joins the serving certificate's DNS names, and only its DNS names, so an endpoint
addressed by IP cannot pass TLS validation. There is no address-based shortcut: the
enrollment endpoint is a DNS name in every topology.

Invite the sites, which mints a one-time token per site into `grid-invite-<site>`:

```bash
helm upgrade grid charts/grid-enrollment -n grid-enroll --reuse-values \
  --set invites.site-1.network=grid \
  --set invites.site-2.network=grid
```

Operator on each cluster, with the site's own name and the other two as peers:

```bash
helm install grid-operator charts/grid-operator -n grid-system --create-namespace \
  --set platform=aws \
  --set peers='<site-2 egress> <hub egress>' \
  --set swim.siteName=site-1 \
  --set grid.id=aws \
  --set enrollment.enabled=true \
  --set enrollment.url=https://enrollment.apps.hub.example.com \
  --set signals.enabled=true \
  --set swim.service.enabled=true \
  --set swim.service.type=LoadBalancer \
  --set image.digest=sha256:<operator digest>
```

On the hub add `--set enrollment.enabled=false`: it holds an identity the bootstrap Job
issued and has nothing to enroll against.

Deliver each site's `grid-invite-<site>` Secret and the grid CA bundle's `ca.crt` to that
cluster's operator namespace before the operator runs, by hand or with a policy engine.

## Why signals have their own Service

Gossip is UDP and signals are TCP. A Service carrying both is refused outright on AWS:

```
Error syncing load balancer: failed to ensure load balancer:
  mixed protocol is not supported for LoadBalancer
```

No load balancer is created at all, and the `aws-load-balancer-type` annotation is ignored,
so the failure is a Service with no address rather than a port that silently does not
work. The chart therefore renders a separate TCP Service for signals, with its own type,
annotations and source ranges.

**Known gap.** The operator still derives a peer's signals endpoint from the SWIM Service,
by looking for a port named `signals` on it, and falls back to the SWIM host at the signals
port when a peer gossips no address. With the Services split, that fallback points at a
host with nothing listening on 9091, so each site must advertise its signals endpoint
explicitly with `signals.advertiseAddress` until the operator reads the signals Service.
On AWS the load balancer's hostname is not known until it is provisioned, so that is a
second `helm upgrade` after the Service has an address.

## Order, and what to check at each step

1. Clusters installed and `Available`.
2. Ports restricted, verified by listing every CIDR on 6443, 443 and 80 rather than by the
   call returning.
3. Enrollment installed, both sites invited, tokens delivered.
4. Operators installed, both sites enrolled and holding identities.
5. Gossip and signals reachable: a datagram to 7946/UDP, a connection to 9091/TCP, in both
   directions between every pair. UDP has no handshake, so this needs an actual datagram
   rather than a connect.
6. `oc get gridsites` on the hub listing all three.

Step 5 is where AWS differs from a lab and it is worth proving before step 6 rather than
diagnosing a grid that has not converged.
