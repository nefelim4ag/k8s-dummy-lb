# k8s-dummy-lb

# TL;DR
```
helm repo add k8s-dummy-lb https://nefelim4ag.github.io/k8s-dummy-lb/
helm install k8s-dummy-lb k8s-dummy-lb/k8s-dummy-lb
```

# Description
Simple "LB" which publishes nodes ExternalIP addresses as a LB addresses

Basically it allows to use any Ingress/Gateway API controller in the same mode as Ingress-Nginx with HostNetwork.

For example:
```
apiVersion: v1
kind: Service
metadata:
  name: contour-envoy
  namespace: contour
status:
  loadBalancer:
    ingress: # Nodes' external IPs of Envoy/Contour nodes will be set there.
    - ip: 1.1.1.1
      ipMode: VIP
spec:
  loadBalancerClass: controller.io/node-ip
  ports:
  - name: http
    protocol: TCP
    port: 80
    targetPort: http
    nodePort: 30357
  - name: https
    protocol: TCP
    port: 443
    targetPort: https
    nodePort: 31705
  selector:
    app.kubernetes.io/component: envoy
  type: LoadBalancer
  sessionAffinity: None
  externalTrafficPolicy: Local
  healthCheckNodePort: 31131
  ipFamilies:
  - IPv4
  ipFamilyPolicy: SingleStack
  allocateLoadBalancerNodePorts: true
```
