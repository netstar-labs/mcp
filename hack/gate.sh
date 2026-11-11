#!/usr/bin/env bash
# The commit gate: one command, run before every commit. CI runs the same checks as
# separate steps; keep the tool pins below in step with .github/workflows/ci.yml.
# CI also runs govulncheck, which the gate does not: it reads a live vulnerability
# database, so it can go red with no change in the repo, which a commit gate must not do.
#
# Fail-closed: a tool that is missing or a step that cannot run is red, never a skip.
set -euo pipefail
cd "$(dirname "$0")/.."

export GOWORK=off
# Build, vet and test run on the toolchain go.mod declares, so every one of them runs on
# the same Go. The analysis tools below need a newer Go than the library does (staticcheck
# v0.8.2 needs 1.26; this module stays on 1.25 so its importers are not forced up), so
# they run with GOTOOLCHAIN=auto: the local Go if it suffices, else the one they ask for.
toolchain=$(sed -n 's/^toolchain //p' go.mod)
[ -n "$toolchain" ] || toolchain="go$(sed -n 's/^go //p' go.mod)"
case "$toolchain" in go[0-9]*.[0-9]*.[0-9]*) ;; go[0-9]*.[0-9]*) toolchain="$toolchain.0" ;; esac
export GOTOOLCHAIN="$toolchain"
# Pinned, not @latest: a gate that starts failing with no change in the repo is not a gate.
STATICCHECK=v0.8.2
DEADCODE=v0.52.0
: "${FUZZTIME:=20s}"   # FUZZTIME=0 skips the fuzz step; the gate then says so, in red letters, and
                       # ends PARTIAL, not OK. The exit status stays 0 so it is still a quick local run.
fuzz_skipped=0

step() { printf '==> %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

step "no dependencies"
! grep -q '^require' go.mod || fail "go.mod has a require line; this module is standard library only"

step gofmt
unformatted=$(gofmt -l .)
[ -z "$unformatted" ] || fail "gofmt -w needed:"$'\n'"$unformatted"

step "go list"
list_err=$(mktemp); trap 'rm -f "$list_err"' EXIT
pkgs=$(go list ./... 2>"$list_err") || { cat "$list_err" >&2; fail "go list ./... failed; refusing to skip vet/build/test"; }
[ -n "$pkgs" ] || fail "no Go packages"

step "go vet";        go vet ./...
step "go build";      go build ./...
# peercred_linux.go and umask_unix.go build only on their own platforms: vet both, so a
# macOS committer sees a Linux-only error and the reverse.
for goos in linux darwin; do step "go vet (GOOS=$goos)"; GOOS=$goos go vet ./...; done
step "go test -race"; go test -race -count=1 ./...
step "staticcheck";   GOTOOLCHAIN=auto go run honnef.co/go/tools/cmd/staticcheck@"$STATICCHECK" ./...
step "deadcode"
dead=$(GOTOOLCHAIN=auto go run golang.org/x/tools/cmd/deadcode@"$DEADCODE" -test ./...)
[ -z "$dead" ] || fail "dead code:"$'\n'"$dead"
if [ "$FUZZTIME" = 0 ]; then
	fuzz_skipped=1
	printf '\033[31mfuzz SKIPPED (FUZZTIME=0): this is not a green gate\033[0m\n'
else
	step "fuzz ($FUZZTIME per target)"; FUZZTIME="$FUZZTIME" hack/fuzz.sh
fi

step "go mod tidy (no diff)"
go mod tidy
git diff --quiet -- go.mod go.sum || { git --no-pager diff -- go.mod go.sum; fail "go.mod/go.sum not tidy"; }

if [ "$fuzz_skipped" = 1 ]; then
	printf '\033[31mPARTIAL: fuzz skipped (FUZZTIME=0), not a green gate\033[0m\n'
else
	echo "OK: gate green"
fi
