---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ .Release.Name }}
  annotations: {{ .Values.serviceAccount.annotations | toJson }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ .Release.Namespace }}-{{ .Release.Name }}
rules:
- apiGroups: [""]
  resources: ["services", "nodes"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["discovery.k8s.io"]
  resources: ["endpointslices"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
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
  namespace: {{ .Release.Namespace }}
roleRef:
  kind: ClusterRole
  name: {{ .Release.Namespace }}-{{ .Release.Name }}
  apiGroup: rbac.authorization.k8s.io
