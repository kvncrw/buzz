#!/usr/bin/env bash
# Rebuild the `pagoda` integration branch: upstream block/buzz main plus every
# patch in .pagoda/patches.txt. Runs identically on a dev box and in CI.
#
# Steps:
#   1. ensure an `upstream` remote (block/buzz) and fetch its main
#   2. verify the fork's main can fast-forward to upstream main (never diverges)
#   3. merge upstream/main into pagoda (--no-edit)
#   4. merge each patch not already an ancestor of pagoda
#   5. print changed=true|false (did the pagoda sha move) plus the shas
#
# Any merge conflict aborts the merge, leaves the branch where it was, and exits 2.
# Nothing is pushed; the caller pushes main and pagoda.
#
# Env:
#   PAGODA_FORK_REMOTE  remote that holds the fork branches (default: `fork` if it
#                       exists, else `origin`)
#   PAGODA_UPSTREAM_URL upstream git URL (default https://github.com/block/buzz.git)
#   GITHUB_OUTPUT       if set, the result lines are appended there too
set -euo pipefail

UPSTREAM_URL="${PAGODA_UPSTREAM_URL:-https://github.com/block/buzz.git}"
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"
PATCHES="$ROOT/.pagoda/patches.txt"

die() { echo "sync: error: $*" >&2; exit "${2:-1}"; }

emit() {
  echo "$1"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then echo "$1" >>"$GITHUB_OUTPUT"; fi
}

# --- remotes -----------------------------------------------------------------
if [[ -n "${PAGODA_FORK_REMOTE:-}" ]]; then
  FORK_REMOTE="$PAGODA_FORK_REMOTE"
elif git remote get-url fork >/dev/null 2>&1; then
  FORK_REMOTE=fork
else
  FORK_REMOTE=origin
fi

if ! git remote get-url upstream >/dev/null 2>&1; then
  git remote add upstream "$UPSTREAM_URL"
fi
echo "sync: fork remote=$FORK_REMOTE upstream=$(git remote get-url upstream)"

git fetch --no-tags --quiet upstream main
git fetch --no-tags --quiet "$FORK_REMOTE" main pagoda 2>/dev/null || git fetch --no-tags --quiet "$FORK_REMOTE" main
UPSTREAM_SHA="$(git rev-parse --verify upstream/main)"

# --- main: fast-forward mirror of upstream --------------------------------------
FORK_MAIN="$(git rev-parse --verify -q "refs/remotes/$FORK_REMOTE/main" || true)"
if [[ -n "$FORK_MAIN" ]] && ! git merge-base --is-ancestor "$FORK_MAIN" "$UPSTREAM_SHA"; then
  die "fork main ($FORK_MAIN) is not an ancestor of upstream main; main must stay a pure mirror" 3
fi
# Update the local main branch when we can. `git branch -f` refuses when main is
# checked out in another worktree; that is fine locally, the push uses the sha.
if git show-ref --verify -q refs/heads/main; then
  if ! git branch -f main "$UPSTREAM_SHA" 2>/dev/null; then
    echo "sync: note: local main is checked out elsewhere, not moved (push uses the sha)"
  fi
else
  git branch main "$UPSTREAM_SHA"
fi

# --- pagoda: make sure it is checked out ----------------------------------------
CURRENT="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$CURRENT" != "pagoda" ]]; then
  if git show-ref --verify -q refs/heads/pagoda; then
    git switch --quiet pagoda
  elif git show-ref --verify -q "refs/remotes/$FORK_REMOTE/pagoda"; then
    git switch --quiet -c pagoda "$FORK_REMOTE/pagoda"
  else
    die "no pagoda branch locally or on $FORK_REMOTE; create it from upstream main first"
  fi
fi
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  die "working tree has uncommitted changes; refusing to merge"
fi
BEFORE="$(git rev-parse HEAD)"

merge_into_pagoda() {
  # $1 = sha, $2 = label
  if git merge-base --is-ancestor "$1" HEAD; then
    echo "sync: $2 already merged ($(git rev-parse --short "$1"))"
    return 0
  fi
  echo "sync: merging $2 ($(git rev-parse --short "$1"))"
  if ! git merge --no-edit --no-ff --signoff -m "merge(pagoda): $2 @ $(git rev-parse --short "$1")" "$1"; then
    git merge --abort || true
    die "conflict merging $2 ($1) into pagoda; resolve by hand on the patch branch" 2
  fi
}

merge_into_pagoda "$UPSTREAM_SHA" "upstream block/buzz main"

# --- patches ----------------------------------------------------------------------
[[ -f "$PATCHES" ]] || die "missing $PATCHES"
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="${line//[[:space:]]/}"
  [[ -z "$line" ]] && continue
  kind="${line%%:*}"
  ref="${line#*:}"
  case "$kind" in
    branch)
      git fetch --no-tags --quiet "$FORK_REMOTE" "refs/heads/$ref"
      sha="$(git rev-parse --verify FETCH_HEAD)"
      merge_into_pagoda "$sha" "branch $ref"
      ;;
    upstream-pr)
      [[ "$ref" =~ ^[0-9]+$ ]] || die "bad PR number in '$line'"
      git fetch --no-tags --quiet upstream "refs/pull/$ref/head"
      sha="$(git rev-parse --verify FETCH_HEAD)"
      merge_into_pagoda "$sha" "upstream PR #$ref"
      ;;
    *)
      die "unknown patch kind in '$line' (want branch:<name> or upstream-pr:<N>)"
      ;;
  esac
done <"$PATCHES"

AFTER="$(git rev-parse HEAD)"
if [[ "$BEFORE" != "$AFTER" ]]; then CHANGED=true; else CHANGED=false; fi
emit "changed=$CHANGED"
emit "pagoda_sha=$AFTER"
emit "main_sha=$UPSTREAM_SHA"
emit "upstream_sha=$UPSTREAM_SHA"
