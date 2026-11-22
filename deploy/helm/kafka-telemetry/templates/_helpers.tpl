{{- define "kt.fullname" -}}
{{- printf "%s" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{- define "kt.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "kt.labels" -}}
app.kubernetes.io/name: kafka-telemetry
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "kt.selector" -}}
app.kubernetes.io/name: kafka-telemetry
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "kt.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "kt.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Variáveis comuns de Kafka e registry, com TLS e credenciais vindas de Secrets existentes. */}}
{{- define "kt.kafkaEnv" -}}
- name: KAFKA_BROKERS
  value: {{ .Values.kafka.brokers | quote }}
- name: SCHEMA_REGISTRY_URL
  value: {{ .Values.schemaRegistry.url | quote }}
{{- if .Values.kafka.security.enabled }}
- name: KAFKA_TLS_CA_FILE
  value: /etc/kafka-secure/ca.crt
- name: KAFKA_TLS_CERT_FILE
  value: /etc/kafka-secure/tls.crt
- name: KAFKA_TLS_KEY_FILE
  value: /etc/kafka-secure/tls.key
- name: KAFKA_SASL_MECHANISM
  value: {{ .Values.kafka.security.mechanism | quote }}
- name: KAFKA_SASL_USER
  value: {{ .Values.kafka.security.user | quote }}
- name: KAFKA_SASL_PASSWORD_FILE
  value: /etc/kafka-secure/{{ .Values.kafka.security.saslPasswordKey }}
{{- end }}
{{- if .Values.schemaRegistry.auth.enabled }}
- name: SCHEMA_REGISTRY_CA_FILE
  value: /etc/registry-secure/ca.crt
- name: SCHEMA_REGISTRY_USER
  value: {{ .Values.schemaRegistry.auth.user | quote }}
- name: SCHEMA_REGISTRY_PASSWORD_FILE
  value: /etc/registry-secure/{{ .Values.schemaRegistry.auth.passwordKey }}
{{- end }}
{{- end -}}

{{- define "kt.secureVolumeMounts" -}}
{{- if .Values.kafka.security.enabled }}
- name: kafka-secure
  mountPath: /etc/kafka-secure
  readOnly: true
{{- end }}
{{- if .Values.schemaRegistry.auth.enabled }}
- name: registry-secure
  mountPath: /etc/registry-secure
  readOnly: true
{{- end }}
{{- end -}}

{{- define "kt.secureVolumes" -}}
{{- if .Values.kafka.security.enabled }}
- name: kafka-secure
  secret:
    secretName: {{ required "kafka.security.secretName é obrigatório com kafka.security.enabled" .Values.kafka.security.secretName }}
{{- end }}
{{- if .Values.schemaRegistry.auth.enabled }}
- name: registry-secure
  secret:
    secretName: {{ required "schemaRegistry.auth.secretName é obrigatório com schemaRegistry.auth.enabled" .Values.schemaRegistry.auth.secretName }}
{{- end }}
{{- end -}}
