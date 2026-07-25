#!/usr/bin/env bash
# Commit this run's news DB + dashboard and push, surviving a concurrent push.
#
# A plain `git push` here fails with "! [rejected] main -> main (fetch first)"
# whenever another run pushed first, and neither force-push nor rebase is safe:
# seen.sqlite is binary, so one side's rows always lose, and a lost row makes an
# already-sent article look new and get sent to Telegram again. So on rejection
# we reset onto the remote tip, union our rows into the remote DB with `merge-db`,
# regenerate the dashboard from the merged data, and commit that instead.
set -euo pipefail

BRANCH="${GITHUB_REF_NAME:-main}"
MAX_TRIES="${PUSH_MAX_TRIES:-5}"
TMP_DIR="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
PYTHON="${PYTHON:-python}"
mkdir -p "$TMP_DIR"

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

# Stages the generated files; returns 1 when there is nothing to commit.
commit_generated() {
  git add -f seen.sqlite
  git add docs/index.html docs/.nojekyll 2>/dev/null || true
  if git diff --cached --quiet; then
    return 1
  fi
  git commit -m "chore: update news db and dashboard [skip ci]"
}

if ! commit_generated; then
  echo "Nothing changed, skipping commit"
  exit 0
fi

for try in $(seq 1 "$MAX_TRIES"); do
  if git push origin "HEAD:${BRANCH}"; then
    echo "Pushed on attempt ${try}"
    exit 0
  fi

  if [ "$try" -eq "$MAX_TRIES" ]; then
    break
  fi

  echo "Push rejected on attempt ${try}; merging the remote DB and retrying"
  sleep $((try * 3))
  git fetch origin "$BRANCH"
  cp seen.sqlite "${TMP_DIR}/ours.sqlite"
  git reset --hard "origin/${BRANCH}"
  "$PYTHON" bc_news_update.py merge-db "${TMP_DIR}/ours.sqlite"
  rm -f "${TMP_DIR}/ours.sqlite"
  "$PYTHON" bc_news_update.py dashboard
  if ! commit_generated; then
    echo "Remote already contains our rows, nothing left to push"
    exit 0
  fi
done

echo "::error::failed to push after ${MAX_TRIES} attempts"
exit 1
