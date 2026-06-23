#!/usr/bin/env bash
# scripts/setup-windows-ssl.sh
#
# For demonstration purposes only.
#
# One-time setup for Docker builds behind a corporate SSL-intercepting proxy
# (Netskope, Zscaler, etc.). Activates the *.windows-ssl.yaml.example
# templates by renaming them to docker-compose.override.yaml so compose
# auto-loads them.
#
# Usage:
#   bash scripts/setup-windows-ssl.sh [/path/to/corp-ca.pem]
#   CORP_SSL_CERT=/path/to/corp-ca.pem bash scripts/setup-windows-ssl.sh
#   bash scripts/setup-windows-ssl.sh --refresh      # overwrite overrides from templates
#   bash scripts/setup-windows-ssl.sh --uninstall    # remove activated overrides
#
# Idempotent: safe to re-run. Use --refresh after pulling repo updates to pick
# up template changes (the override files themselves are gitignored).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

uninstall() {
  local removed=0
  while IFS= read -r -d '' active; do
    local sibling="${active%docker-compose.override.yaml}docker-compose.windows-ssl.yaml.example"
    if [ -e "$sibling" ]; then
      rm -f -- "$active"
      echo "  removed: ${active#$REPO_ROOT/}"
      removed=$((removed + 1))
    fi
  done < <(find . -name "docker-compose.override.yaml" -not -path "./.git/*" -print0)
  echo ""
  echo "Removed $removed activated override file(s)."
  echo "certs/corp-ca.pem left in place (delete manually if you want it gone)."
}

refresh() {
  local updated=0
  while IFS= read -r -d '' tmpl; do
    local target="${tmpl%docker-compose.windows-ssl.yaml.example}docker-compose.override.yaml"
    if [ -e "$target" ] && ! cmp -s "$tmpl" "$target"; then
      cp -- "$tmpl" "$target"
      echo "  refreshed: ${target#$REPO_ROOT/}"
      updated=$((updated + 1))
    fi
  done < <(find . -name "docker-compose.windows-ssl.yaml.example" -not -path "./.git/*" -print0)
  echo ""
  echo "Refreshed $updated override file(s) from latest templates."
}

if [ "${1:-}" = "--uninstall" ] || [ "${1:-}" = "-u" ]; then
  uninstall
  exit 0
fi

if [ "${1:-}" = "--refresh" ] || [ "${1:-}" = "-r" ]; then
  refresh
  exit 0
fi

CERT_SRC="${1:-${CORP_SSL_CERT:-}}"

if [ -z "$CERT_SRC" ]; then
  cat <<EOF
Usage: bash scripts/setup-windows-ssl.sh [/path/to/corp-ca.pem]
   or: CORP_SSL_CERT=/path/to/corp-ca.pem bash scripts/setup-windows-ssl.sh
   or: bash scripts/setup-windows-ssl.sh --uninstall

If you're not behind a corporate SSL-intercepting proxy, you do not need
this script.

What it does:
  1. Copies your corporate root CA into certs/corp-ca.pem (gitignored).
  2. Renames each docker-compose.windows-ssl.yaml.example template to
     docker-compose.override.yaml so docker compose auto-loads it.

Dockerfiles already trust this cert via a BuildKit secret mount; the step
no-ops when the secret is absent, so non-corp environments are unaffected.
EOF
  exit 1
fi

if [ ! -s "$CERT_SRC" ]; then
  echo "Error: cert not found or empty: $CERT_SRC" >&2
  exit 1
fi

# Install cert
mkdir -p certs
cp -f -- "$CERT_SRC" certs/corp-ca.pem
echo "  installed: certs/corp-ca.pem  <-  $CERT_SRC"

# Activate templates
activated=0
skipped=0
while IFS= read -r -d '' tmpl; do
  target="${tmpl%docker-compose.windows-ssl.yaml.example}docker-compose.override.yaml"
  if [ -e "$target" ]; then
    echo "  exists  : ${target#$REPO_ROOT/}"
    skipped=$((skipped + 1))
  else
    cp -- "$tmpl" "$target"
    echo "  activated: ${target#$REPO_ROOT/}"
    activated=$((activated + 1))
  fi
done < <(find . -name "docker-compose.windows-ssl.yaml.example" -not -path "./.git/*" -print0)

echo ""
echo "Done. Activated $activated, skipped $skipped (already present)."
echo "Stack/engine builds will now mount the corp cert as a BuildKit secret."
echo "To revert: bash scripts/setup-windows-ssl.sh --uninstall"
