{{- define "k8s-alert.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "k8s-alert.fullname" -}}
{{- default (printf "%s" .Release.Name) .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "k8s-alert.sa" -}}
{{- default (include "k8s-alert.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}
{{- define "k8s-alert.labels" -}}
app.kubernetes.io/name: {{ include "k8s-alert.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
{{- define "k8s-alert.config" -}}
{{- $config := deepCopy .Values.config -}}
{{- $_ := set $config "destinations" .Values.destinations -}}
{{- $_ := set $config "listen" ":8080" -}}
{{- $_ := set $config "persistence" (dict "enabled" .Values.persistence.enabled "name" (printf "%s-state" (include "k8s-alert.fullname" .)) "namespace" .Release.Namespace) -}}
{{ toYaml $config }}
{{- end -}}
