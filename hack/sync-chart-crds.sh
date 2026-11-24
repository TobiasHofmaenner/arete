#!/usr/bin/env bash
# Regenerates the Helm chart's CRD templates from the CRDs controller-gen
# writes to config/crd/bases, so the two cannot drift apart.
#
# The chart copy differs from the generated file only by a Helm wrapper:
# an enable switch, the chart labels and the optional "keep" annotation.
set -euo pipefail

src="config/crd/bases"
dst="dist/chart/templates/crd"

for f in "$src"/*.yaml; do
  # Helm would try to render any "{{" inside the CRD (for example in a field
  # description). Refuse instead of producing a chart that fails to install.
  if grep -q '{{' "$f"; then
    echo "error: $f contains '{{', which Helm would interpret; escape it or reword the comment" >&2
    exit 1
  fi
  out="$dst/$(basename "$f")"
  {
    echo '{{- if .Values.crd.enable }}'
    awk '
      /^metadata:$/ && !m {
        print
        print "  labels:"
        print "    {{- include \"chart.labels\" . | nindent 4 }}"
        m = 1
        next
      }
      /^  annotations:$/ && !a {
        print
        print "    {{- if .Values.crd.keep }}"
        print "    \"helm.sh/resource-policy\": keep"
        print "    {{- end }}"
        a = 1
        next
      }
      { print }
    ' "$f"
    echo '{{- end -}}'
  } > "$out"
done

# A CRD removed from the code must not linger in the chart.
for out in "$dst"/*.yaml; do
  [ -f "$src/$(basename "$out")" ] || rm -f "$out"
done
