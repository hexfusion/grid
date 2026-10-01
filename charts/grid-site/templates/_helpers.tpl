{{/*
Chart name, truncated to 63 characters.
*/}}
{{- define "grid-site.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name. Uses fullnameOverride if set, otherwise combines
release name and chart name (deduplicating when the release name already
contains the chart name).
*/}}
{{- define "grid-site.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart label value: name-version.
*/}}
{{- define "grid-site.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Standard Kubernetes labels applied to every resource.
*/}}
{{- define "grid-site.labels" -}}
helm.sh/chart: {{ include "grid-site.chart" . }}
app.kubernetes.io/name: {{ include "grid-site.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Normalize values once per render, in place and idempotently. Listing peers selects the
enrolled multi-cluster default: the TLS Secrets the operator writes. Declared peers need
no discovery mode, since manual discovery fills them from SWIM. inferenceProviders keyed by name become the list the template reads.
*/}}
{{- define "grid-site.localHostSelector" -}}
{{- with .Values.gridSite.name }}
matchLabels:
  grid.praxis-proxy.io/provider-site: {{ $.Values.gridSite.providerSiteLabel | default . | quote }}
{{- end }}
{{- end }}

{{- define "grid-site.normalize" -}}
{{- $v := .Values }}
{{- $net := $v.gridNetwork }}
{{- if $v.peers }}
{{- if not $net.tls }}
{{- $ns := .Release.Namespace }}
{{- $_ := set $net "tls" (dict
  "siteSecretRef" (dict "name" "grid-site-identity" "namespace" $ns)
  "caSecretRef" (dict "name" "grid-ca" "namespace" $ns)
  "swimKeyRef" (dict "name" "grid-swim-key" "namespace" $ns)) }}
{{- end }}
{{- end }}
{{- if kindIs "map" $v.inferenceProviders }}
{{- $list := list }}
{{- range $name := keys $v.inferenceProviders | sortAlpha }}
{{- $p := deepCopy (get $v.inferenceProviders $name | default dict) }}
{{- if not (hasKey $p "hostSelector") }}
{{- $_ := set $p "hostSelector" (include "grid-site.localHostSelector" $ | fromYaml) }}
{{- end }}
{{- $model := $p.model | default $name }}
{{- $_ := unset $p "model" }}
{{- $list = append $list (merge $p (dict
  "name" $name
  "gridNetworkRef" $net.name
  "providerKind" "openAi"
  "backendKind" "local"
  "models" (list (dict "name" $model "capabilities" (list "text_generation"))))) }}
{{- end }}
{{- $_ := set $v "inferenceProviders" $list }}
{{- end }}
{{- end }}
