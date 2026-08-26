#!/usr/bin/env bash
# Copy the versioned hook into .git/hooks, which git does not track.
# Run once after cloning. The README says so.
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
src="$root/scripts/pre-commit"
dst="$root/.git/hooks/pre-commit"

[ -f "$src" ] || { echo "missing $src" >&2; exit 1; }

cp "$src" "$dst"
chmod +x "$dst"
echo "installed $dst"
