# Installing on AWS

Three single-node OpenShift clusters, one acting as the hub. Written from a build, so the
values are ones that were used.

AWS changes three things: gossip needs a network load balancer, because the default carries
no UDP; a peer arrives from its cluster's egress address, not a private CIDR; and the
enrollment endpoint must be a DNS name.

## Clusters

One control-plane replica, no workers, both roles on the one node. Pin each cluster to a
single availability zone and give each a different one. Spanning zones costs a NAT gateway
and an Elastic IP per zone, so three clusters at the installer's three-zone default ask for
twelve against a regional default of sixteen. Pinning takes it to two per cluster.

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

`publish: External` needs a public Route53 zone for the base domain. The installer checks
the zone exists, not that its delegation is live, so the delegation only has to work before
anything dials the clusters by name.

Decide `machineNetwork` at install time: the default is `10.0.0.0/16` for every cluster, so
three cannot be peered without renumbering, and gossip then crosses the internet between NAT
addresses, authenticated but neither private nor free.

## Endpoints

`publish: External` puts the API on an internet-facing load balancer, and the console, OAuth
endpoint and `kubeadmin` password behind an internet-facing router. Restrict both before
installing anything.

The API load balancer's own group, `<infraID>-apiserver-lb`, is **not** tagged with the
cluster, so a scan filtering only on `kubernetes.io/cluster/<infraID>` misses it. Match on
that tag **or** the `<infraID>` name prefix, and read every CIDR on a port, not the first.

Authorise the allowlist before revoking the world-open rule, so a failure between the two
leaves the cluster reachable rather than locked out:

```bash
aws ec2 authorize-security-group-ingress --group-id <sg> --protocol tcp --port 6443 --cidr <you>/32
aws ec2 revoke-security-group-ingress    --group-id <sg> --protocol tcp --port 6443 --cidr 0.0.0.0/0
```

Leave ICMP types 3 and 4 alone; revoking them breaks path MTU discovery.

The hub's API must also accept each site, which dials it from that site's egress address.
Add each cluster's NAT gateway address, not its VPC CIDR:

```bash
aws ec2 describe-nat-gateways --filter Name=vpc-id,Values=<vpc> \
  --query 'NatGateways[].NatGatewayAddresses[].PublicIp' --output text
```

## The grid

Two values carry everything AWS-specific. `platform: aws` asks for a network load balancer,
since the default carries no UDP. `peers` takes the other sites' addresses, separated by
spaces or commas, and derives both the SWIM seeds and the Services' source ranges; one flag
needs neither braces nor escaped commas, which `--set` otherwise requires for a list.

Enrollment on the hub, whose namespace must exist first because the bootstrap Job writes the
hub's identity into it:

```bash
kubectl create namespace grid

helm install grid charts/grid-enrollment -n grid-enroll --create-namespace \
  --set host=enrollment.apps.hub.example.com \
  --set route.host=enrollment.apps.hub.example.com \
  --set hubSite.name=hub \
  --set image.digest=sha256:<enrollment digest>
```

`host` joins the serving certificate's DNS names only, so an IP-addressed endpoint cannot
pass TLS validation. There is no address-based shortcut.

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

Gossip is UDP, signals are TCP, and a Service carrying both is refused outright:

```
Error syncing load balancer: failed to ensure load balancer:
  mixed protocol is not supported for LoadBalancer
```

No load balancer is created at all and the annotation is ignored, so the failure is a
Service with no address rather than a port that does not work. The chart renders a separate
TCP Service for signals, with its own type, annotations and source ranges, and the operator
reads it through `GRID_SIGNALS_SERVICE_NAME`.

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

Step 5 is where AWS differs from a lab. Prove it before step 6 rather than diagnosing a grid
that has not converged.

## Troubleshooting

**`unauthorized` pulling the enrollment or operator image.** A digest pin keeps the chart's
`image.repository` default, which points at a registry with no public repositories. Set both
`image.repository` and `image.digest`.

**`Error: Api(Status { code: 401, message: "Unauthorized" })` from the CA bootstrap Job.**
The Job's ServiceAccount is gone. Its RBAC carries
`hook-delete-policy: before-hook-creation,hook-succeeded,hook-failed`, so a first attempt
that fails for any reason takes the ServiceAccount, Role and RoleBinding with it, and the
retry runs with no identity. The 401 names authentication rather than the missing account.
`helm upgrade` re-runs the Job after the delete policy has already removed what it needs, so
recover with `helm uninstall` and install again. Delete any `grid-ca-*`,
`enrollment-serving-tls` and `grid-site-identity` Secrets left behind first, in both the
release namespace and `hubSite.namespace`, or the next bootstrap refuses to overwrite them.

**`mixed protocol is not supported for LoadBalancer`.** A Service is carrying UDP and TCP
together. Upgrade to a chart that renders signals separately.

**A site never reaches `Available`, or enrollment times out.** The hub's API or router is not
accepting that site's egress address. Check the NAT gateway address of the calling cluster
against the hub's allowlist on 6443 and 443.

**Gossip never converges but signals poll fine.** The SWIM Service got a Classic load
balancer, which carries no UDP. Confirm `platform: aws`, or the
`service.beta.kubernetes.io/aws-load-balancer-type: nlb` annotation, on that Service.
