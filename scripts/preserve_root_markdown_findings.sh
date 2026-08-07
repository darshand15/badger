#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NOTES_FILE="${ROOT_DIR}/NOTES.md"

usage() {
  cat <<'EOF'
Usage: scripts/preserve_root_markdown_findings.sh <root-file.md> [<root-file2.md> ...]

Appends each root markdown file's content into NOTES.md with metadata,
then deletes the original file.

This is intended for pre-cleanup archival of non-paper notes so findings are
not lost when root-level markdown files are removed.
EOF
}

if [[ "$#" -lt 1 ]]; then
  usage
  exit 1
fi

for f in "$@"; do
  src="${ROOT_DIR}/${f}"
  if [[ ! -f "${src}" ]]; then
    echo "Skip (not found): ${f}" >&2
    continue
  fi
  if [[ "${f}" != *.md ]]; then
    echo "Skip (not markdown): ${f}" >&2
    continue
  fi

  {
    echo ""
    echo "## Archived from ${f}"
    echo "- archived_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- source_path: ${f}"
    echo ""
    cat "${src}"
    echo ""
    echo "---"
  } >> "${NOTES_FILE}"

  rm -f "${src}"
  echo "Archived and removed: ${f}"
done
