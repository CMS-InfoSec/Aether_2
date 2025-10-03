{{- define "aether-platform.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "aether-platform.serviceName" -}}
{{- $name := .nameOverride | default (printf "%s-service" .name) -}}
{{- $release := $.Release.Name -}}
{{- printf "%s-%s" $release $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "aether-platform.labels" -}}
app.kubernetes.io/name: {{ .name }}
{{- with .Release }}
app.kubernetes.io/instance: {{ .Name }}
{{- end }}
app.kubernetes.io/part-of: aether-platform
{{- end -}}

{{- define "aether-platform.selectorLabels" -}}
app: {{ .name }}
{{- end -}}

{{- define "aether-platform.image" -}}
{{- printf "%s:%s" .repository (default "latest" .tag) -}}
{{- end -}}
