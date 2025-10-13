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

{{- /* Compute the primary service name for the bundled PostgreSQL dependency. */ -}}
{{- define "aether-platform.postgresqlFullname" -}}
{{- $pg := .Values.dependencies.postgresql | default (dict) -}}
{{- $fullname := $pg.fullnameOverride | default (printf "%s-postgresql" .Release.Name) -}}
{{- $fullname -}}
{{- end -}}

{{- define "aether-platform.postgresqlHost" -}}
{{- $conn := .connection | default (dict) -}}
{{- if $conn.host }}
{{- $conn.host -}}
{{- else if (.Values.dependencies.postgresql.enabled | default false) -}}
{{- printf "%s.%s.svc.cluster.local" (include "aether-platform.postgresqlFullname" .) .Release.Namespace -}}
{{- else -}}
{{- fail (printf "managed secret %s requires a PostgreSQL host" (.name | default "")) -}}
{{- end -}}
{{- end -}}

{{- define "aether-platform.postgresqlPort" -}}
{{- $conn := .connection | default (dict) -}}
{{- $port := $conn.port | default 5432 -}}
{{- $port -}}
{{- end -}}

{{- define "aether-platform.postgresqlQuery" -}}
{{- $conn := .connection | default (dict) -}}
{{- $sslmode := $conn.sslmode | default "require" -}}
{{- $params := dict "sslmode" $sslmode -}}
{{- range $key, $val := $conn.parameters | default (dict) -}}
{{- $_ := set $params $key $val -}}
{{- end -}}
{{- $pairs := list -}}
{{- range $key, $val := $params -}}
{{- $pairs = append $pairs (printf "%s=%s" $key (urlquery (printf "%v" $val))) -}}
{{- end -}}
{{- join "&" $pairs -}}
{{- end -}}

{{- define "aether-platform.postgresqlDsn" -}}
{{- $conn := .connection | default (dict) -}}
{{- $host := include "aether-platform.postgresqlHost" . -}}
{{- $port := include "aether-platform.postgresqlPort" . -}}
{{- $database := required (printf "managed secret %s requires a database name" (.name | default "")) $conn.database -}}
{{- $username := required (printf "managed secret %s requires a username" (.name | default "")) $conn.username -}}
{{- $password := required (printf "managed secret %s requires a password" (.name | default "")) $conn.password -}}
{{- $query := include "aether-platform.postgresqlQuery" . -}}
{{- printf "postgresql://%s:%s@%s:%v/%s?%s" (urlquery $username) (urlquery $password) $host $port $database $query -}}
{{- end -}}

{{- define "aether-platform.redisServiceName" -}}
{{- $redis := .Values.dependencies.redis | default (dict) -}}
{{- $fullname := $redis.fullnameOverride | default (printf "%s-redis" .Release.Name) -}}
{{- $arch := lower ($redis.architecture | default "standalone") -}}
{{- if eq $arch "replication" -}}
{{- printf "%s-master" $fullname -}}
{{- else -}}
{{- $fullname -}}
{{- end -}}
{{- end -}}

{{- define "aether-platform.redisUrl" -}}
{{- $session := .Values.global.sessionStore | default (dict) -}}
{{- if $session.redisUrl -}}
{{- $session.redisUrl -}}
{{- else if (.Values.dependencies.redis.enabled | default false) -}}
{{- $redis := .Values.dependencies.redis -}}
{{- $service := include "aether-platform.redisServiceName" . -}}
{{- $password := $redis.auth.password | default "" -}}
{{- $redisPort := $redis.master.service.ports.redis | default 6379 -}}
{{- if and ($redis.auth.enabled | default false) (ne $password "") -}}
{{- printf "redis://:%s@%s.%s.svc.cluster.local:%v/0" (urlquery $password) $service .Release.Namespace $redisPort -}}
{{- else -}}
{{- printf "redis://%s.%s.svc.cluster.local:%v/0" $service .Release.Namespace $redisPort -}}
{{- end -}}
{{- else -}}
{{- $session.redisUrl | default "" -}}
{{- end -}}
{{- end -}}

{{- define "aether-platform.postgresqlInitdbConfigMapName" -}}
{{- default (printf "%s-postgresql-init" .Release.Name) .Values.dependencies.postgresql.primary.initdbScriptsConfigMap -}}
{{- end -}}

{{- define "aether-platform.escapeSqlLiteral" -}}
{{- replace . "'" "''" -}}
{{- end -}}
