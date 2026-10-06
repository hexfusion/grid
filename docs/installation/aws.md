# Installing on AWS

AWS needs three settings the grid does not need elsewhere:

- `platform: aws`, because gossip is UDP and the default load balancer carries none.
- `peers`, the other sites' NAT addresses, which become the SWIM seeds and both Services'
  source ranges.
- `host` as a DNS name, since it joins the serving certificate's DNS names only.

## Before you start

A public Route53 zone for the base domain, and each cluster's NAT address:

```bash
aws ec2 describe-nat-gateways --filter Name=vpc-id,Values=<vpc> \
  --query 'NatGateways[].NatGatewayAddresses[].PublicIp' --output text
```

Give each cluster a distinct `machineNetwork`, or clusters sharing `10.0.0.0/16` cannot be
peered and cross-site traffic takes the internet. Pin each to one availability zone: every
zone costs a NAT gateway and an Elastic IP, and three clusters at the three-zone default
ask for twelve against a regional default of sixteen.

## Restrict the endpoints

`publish: External` exposes 6443, and 443 with the console, OAuth endpoint and `kubeadmin`
password. Close both to everything but your address and the clusters' NAT addresses, before
installing anything.

```bash
aws ec2 authorize-security-group-ingress --group-id <sg> --protocol tcp --port 6443 --cidr <allowed>/32
aws ec2 revoke-security-group-ingress    --group-id <sg> --protocol tcp --port 6443 --cidr 0.0.0.0/0
```

Authorise before revoking. Match groups on `kubernetes.io/cluster/<infraID>` **or** the
`<infraID>` name prefix, since `<infraID>-apiserver-lb` carries no cluster tag. Leave ICMP
types 3 and 4, or path MTU discovery breaks.

## Install

Enrollment on the hub. `hubSite.namespace` must exist first:

```bash
kubectl create namespace grid

helm install grid charts/grid-enrollment -n grid-enroll --create-namespace \
  --set image.repository=<registry>/grid-enrollment \
  --set image.digest=sha256:<digest> \
  --set host=enrollment.apps.hub.example.com \
  --set route.host=enrollment.apps.hub.example.com \
  --set hubSite.name=hub \
  --set invites.site-1.network=grid \
  --set invites.site-2.network=grid
```

Copy each `grid-invite-<site>` Secret, and `ca.crt` from `grid-ca-bundle`, to that site's
operator namespace. Then the operator on each cluster:

```bash
helm install grid-operator charts/grid-operator -n grid-system --create-namespace \
  --set platform=aws \
  --set peers='<other site> <hub>' \
  --set swim.siteName=site-1 \
  --set grid.id=aws \
  --set enrollment.enabled=true \
  --set enrollment.url=https://enrollment.apps.hub.example.com \
  --set signals.enabled=true \
  --set swim.service.enabled=true \
  --set swim.service.type=LoadBalancer \
  --set image.repository=<registry>/grid-operator \
  --set image.digest=sha256:<digest>
```

The hub adds `--set enrollment.enabled=false`; its identity comes from the bootstrap Job.

## Verify

```bash
oc get gridsites                 # every site, on the hub
oc get svc -n grid-system        # swim UDP and signals TCP, each with an address
```

Then send a datagram to 7946/UDP and open 9091/TCP between every pair. UDP has no
handshake, so a connect proves nothing.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `unauthorized` pulling an image | A digest pin keeps the chart's `image.repository`. Set both. |
| `mixed protocol is not supported for LoadBalancer` | One Service carrying UDP and TCP. Upgrade to a chart that splits signals out. |
| CA bootstrap Job gives `401 Unauthorized` | Its ServiceAccount is gone, removed with the hook RBAC after a failed attempt. Uninstall, delete leftover `grid-ca-*`, `enrollment-serving-tls` and `grid-site-identity` Secrets in both namespaces, install again. |
| A site never reaches `Available` | The hub is not accepting that site's NAT address on 6443 or 443. |
| Signals poll but gossip never converges | The SWIM Service got a Classic load balancer, which carries no UDP. Check `platform: aws`. |
| Peers unreachable despite correct addresses | Source ranges list VPC CIDRs rather than NAT addresses. |
