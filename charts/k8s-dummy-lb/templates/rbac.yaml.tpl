---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ .Release.Name }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ .Release.Namespace }}-{{ .Release.Name }}
rules:
- apiGroups: [""]
  resources: ["services", "nodes", "endpointslices"]
  verbs: ["get", "watch", "list"]
- apiGroup: [""]
  resources: ["services/status"]
  verbs: ["patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ .Release.Namespace }}-{{ .Release.Name }}
subjects:
- kind: ServiceAccount
  name: {{ .Release.Name }}
  apiGroup: ""
  namespace: {{ .Release.Namespace }}
roleRef:
  kind: ClusterRole
  name: {{ .Release.Namespace }}-{{ .Release.Name }}
  apiGroup: ""
