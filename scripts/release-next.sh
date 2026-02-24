#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PROJECT_FILE="CodeWorks.SimpleSql.csproj"
SOLUTION_FILE="CodeWorks.SimpleSql.sln"
REMOTE_NAME="origin"

if ! command -v git >/dev/null 2>&1; then
  echo "git is required."
  exit 1
fi

if ! command -v dotnet >/dev/null 2>&1; then
  echo "dotnet is required."
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required."
  exit 1
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "This folder is not a git repository."
  exit 1
fi

if ! git remote get-url "$REMOTE_NAME" >/dev/null 2>&1; then
  echo "Git remote '$REMOTE_NAME' is not configured."
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Working tree must be clean before release. Commit or stash changes first."
  exit 1
fi

current_version="$(python3 - <<'PY'
import re
from pathlib import Path
text = Path('CodeWorks.SimpleSql.csproj').read_text(encoding='utf-8')
match = re.search(r'<Version>([^<]+)</Version>', text)
if not match:
    raise SystemExit('Could not find <Version> in csproj')
print(match.group(1).strip())
PY
)"

IFS='.' read -r major minor patch <<<"$current_version"
if [[ -z "${major:-}" || -z "${minor:-}" || -z "${patch:-}" ]]; then
  echo "Version '$current_version' is not in expected semver format X.Y.Z"
  exit 1
fi

if ! [[ "$major" =~ ^[0-9]+$ && "$minor" =~ ^[0-9]+$ && "$patch" =~ ^[0-9]+$ ]]; then
  echo "Version '$current_version' must be numeric X.Y.Z"
  exit 1
fi

next_version="$major.$minor.$((patch + 1))"
next_tag="v$next_version"

if git rev-parse "$next_tag" >/dev/null 2>&1; then
  echo "Tag '$next_tag' already exists."
  exit 1
fi

python3 - <<PY
import re
from pathlib import Path
path = Path('$PROJECT_FILE')
text = path.read_text(encoding='utf-8')
updated, count = re.subn(r'<Version>[^<]+</Version>', '<Version>$next_version</Version>', text, count=1)
if count != 1:
    raise SystemExit('Failed to update <Version> in csproj')
path.write_text(updated, encoding='utf-8')
PY

echo "Running tests..."
dotnet test "$SOLUTION_FILE"

git add "$PROJECT_FILE"
git commit -m "chore(release): bump version to $next_version"
git tag "$next_tag"

echo "Pushing commit and tag..."
git push "$REMOTE_NAME"
git push "$REMOTE_NAME" "$next_tag"

echo "Release prepared and published trigger sent: $next_tag"
