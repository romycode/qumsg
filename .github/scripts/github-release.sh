#!/usr/bin/env bash
# Crea la release de GitHub de una etiqueta vX.Y.Z con su sección de
# CHANGELOG.md como notas. Si la release ya existe, no hace nada.
# Uso: github-release.sh vX.Y.Z
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Uso: $0 vX.Y.Z" >&2
  exit 2
fi

tag="$1"
version="${tag#v}"

# Notas: la sección de esta versión en CHANGELOG.md, hasta la siguiente.
notes=$(awk -v v="$version" '
  index($0, "## [" v "]") == 1 { f = 1; next }
  f && /^## \[/ { exit }
  f' CHANGELOG.md)

if gh release view "$tag" >/dev/null 2>&1; then
  echo "La release $tag ya existe"
  exit 0
fi

# Solo la etiqueta core más alta es Latest; las antiguas no lo reclaman.
highest=$(git tag -l 'v*.*.*' --sort=-v:refname | head -1)
latest=false
if [ "$tag" = "$highest" ]; then
  latest=true
fi

gh release create "$tag" --verify-tag --title "$tag" \
  --notes "${notes:-Ver CHANGELOG.md}" --latest="$latest"
