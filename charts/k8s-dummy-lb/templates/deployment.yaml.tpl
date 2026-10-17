---
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    app: {{ .Release.Name }}
  name: {{ .Release.Name }}
spec:
  replicas: {{ .Values.replicas }}
  strategy: {{ .Values.strategy | toJson }}
  selector:
    matchLabels:
      app:  {{ .Release.Name }}
  template:
    metadata:
      labels:
        app:  {{ .Release.Name }}
    spec:
      serviceAccountName: {{ .Release.Name }}
      serviceAccount: {{ .Release.Name }}
      containers:
      - image: {{ .Values.image.repository }}:{{ .Values.image.tag }}
        imagePullPolicy: {{ .Values.image.pullPolicy }}
        name: k8s-dummy-lb
        env:
        {{- with .Values.env }}
        {{- range $key, $value := . }}
        - name: {{ $key }}
        {{- if kindIs "string" $value }}
          value: {{ tpl (toYaml $value) $ }}
        {{- else }}
          {{- tpl (toYaml $value) $ | nindent 12 }}
        {{- end }}
        {{- end }}
        {{- end }}
      hostNetwork: {{ .Values.hostNetwork }}
      {{- with .Values.dnsConfig }}
      dnsConfig: {{ . | toJson }}
      {{- end }}
      terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
      imagePullSecrets: {{ .Values.image.imagePullSecrets | toJson }}
{{- with .Values.topologySpreadConstraints }}
      topologySpreadConstraints:
        {{- tpl (toYaml .) $ | nindent 8 }}
{{- end }}
{{- with .Values.affinity }}
      affinity:
        {{- tpl (toYaml .) $ | nindent 8 }}
{{- end }}
{{- with $.Values.tolerations }}
      tolerations:
        {{- tpl (toYaml .) $ | nindent 8 }}
{{- end }}
