#!/usr/bin/env bash
# Run every fuzz target in the module for FUZZTIME each (default 20s, decision 70).
# Used by hack/gate.sh and CI. A crash is never retried; the fuzz engine's own
# "context deadline exceeded" with no failing input is retried once. Fail-closed: a package
# whose targets cannot be listed (it does not compile) and a run in which no target ran at
# all are both red, never a silent skip.
set -euo pipefail
cd "$(dirname "$0")/.."
export GOWORK=off
: "${FUZZTIME:=20s}"
log=$(mktemp); trap 'rm -f "$log"' EXIT
status=0
ran=0
pkgs=$(go list ./...)   # under errexit, so a module that cannot be listed is red too
for pkg in $pkgs; do
	# The status of the listing is checked on its own: `grep` finding no target is normal, a
	# failed `go test -list` (a compile error) is not.
	if ! listing=$(go test -list '^Fuzz' "$pkg"); then
		echo "FAIL: $pkg: go test -list failed, its fuzz targets were not run" >&2
		status=1
		continue
	fi
	for target in $(printf '%s\n' "$listing" | grep '^Fuzz' || true); do
		ran=$((ran + 1))
		ok=0
		for attempt in 1 2; do
			printf '==> fuzz %s %s (%s)\n' "$pkg" "$target" "$FUZZTIME"
			if go test -run '^$' -fuzz "^${target}\$" -fuzztime "$FUZZTIME" "$pkg" >"$log" 2>&1; then
				ok=1; break
			fi
			cat "$log"
			if grep -q 'Failing input written to' "$log" || ! grep -q 'context deadline exceeded' "$log"; then
				break
			fi
			echo "warning: $target: fuzz engine deadline error, retrying once"
		done
		[ "$ok" = 1 ] || status=1
	done
done
if [ "$ran" = 0 ]; then
	echo "FAIL: no fuzz target found in the module" >&2
	status=1
fi
exit $status
