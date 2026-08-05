apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: ${ROUTE_NAME}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: grid-route53-edge-entry
    app.kubernetes.io/part-of: grid
spec:
  host: ${ROUTE_HOSTNAME}
  to:
    kind: Service
    name: ${SERVICE_NAME}
    weight: 100
  port:
    targetPort: ${SERVICE_PORT}
  tls:
    termination: ${TLS_TERMINATION}
    insecureEdgeTerminationPolicy: Redirect
