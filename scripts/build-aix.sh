#!/bin/sh
# build-aix.sh -- produce the AIX/ppc64 deployment artefact set.
#
# Vol 4 section 8's deployment topology names the artefacts: "ONE static binary
# + config.json + deps.txt golden list". Gate G-AIX-1 adds the cross-compiled
# test binaries, because five of its assertions can only be made by code
# running on the target.
#
# Runs on the BUILD host (windows/amd64 or linux/amd64). Nothing here needs AIX.
#
# This supersedes poc/build_aix.sh, which is the opposite of what the contract
# now asks for: it sets CGO_ENABLED=1, needs liblzo2 headers and a C compiler,
# and has to run ON an AIX machine. That requirement disappeared when LZO1Z
# decompression became a pure-Go port; CLAUDE.md already records the script as
# stale.
#
# POSIX sh on purpose. It runs under Git Bash on Windows, under any Linux CI,
# and -- for the parts that matter -- could be read by someone on AIX's ksh.

set -eu

OUT=${OUT:-out/aix-ppc64}
MISSING_CONFIG=0
GOOS=aix
GOARCH=ppc64
CGO_ENABLED=0
export GOOS GOARCH CGO_ENABLED

# The version stamp. Vol 4 section 8 specifies -trimpath and
# -ldflags "-X main.version=...", and P1-11 added the variable for it to set.
# A detached or dirty tree still produces something, because an unstamped
# artefact is the one you cannot match to a commit later.
VERSION=${VERSION:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}
if ! git diff --quiet 2>/dev/null; then
	VERSION="${VERSION}-dirty"
fi

# -trimpath so the binary does not embed build-host absolute paths. On a host
# whose only debugging tool is a panic stack, a stack full of
# C:\Users\...\worktrees\... is noise at best and an information leak at worst.
LDFLAGS="-X main.version=${VERSION}"

echo "building for ${GOOS}/${GOARCH}, version ${VERSION}"
mkdir -p "$OUT"

echo "+ the binary"
go build -trimpath -ldflags "$LDFLAGS" -o "$OUT/feedvalve" ./cmd/feedvalve

# The test binaries. Gate G-AIX-1: "cross-compile the test binaries
# (go test -c -o ... ./internal/...) with CGO_ENABLED=0 GOOS=aix GOARCH=ppc64
# and run them ON the LPAR -- five assertions no amd64 run can make".
#
# Every package is shipped, not just the ones with on-target assertions today.
# The source-scanning gates skip themselves when the source tree is absent
# (see internal/app/testenv_test.go), so a package whose gates cannot run on
# the target reports skips rather than failures -- which is what makes the whole
# suite safe to run there.
echo "+ the test binaries"
for pkg in model wire lzo decode policy mirror writer health obs framing destinations config oradb app; do
	[ -d "internal/$pkg" ] || continue
	go test -c -o "$OUT/${pkg}.test" "./internal/$pkg"
	# A package with no test files produces NO binary and still exits 0. Report
	# what exists rather than what was attempted: a listing that claims
	# wire.test when there is no wire.test is the kind of small lie that costs
	# somebody on the far end of an scp an afternoon.
	if [ -f "$OUT/${pkg}.test" ]; then
		echo "  ${pkg}.test"
	else
		echo "  ${pkg}: no test files, nothing to ship"
	fi
done

# The golden dependency list. Vol 4 section 8.2 asks for it committed so that
# "the dependency set cannot change without somebody noticing" -- the
# golden-list form of G-DEP-1, and the thing that catches a new dependency that
# happens to be pure Go and so passes G-DEP-2 and G-DEP-3.
#
# Generated with the HOST toolchain settings deliberately unset for GOOS/GOARCH:
# the list is about what the module requires, not about one target.
echo "+ deps.txt"
GOOS= GOARCH= go list -deps ./cmd/feedvalve > "$OUT/deps.txt"
echo "  $(wc -l < "$OUT/deps.txt" | tr -d ' ') packages"

# The configuration travels with the binary and carries NO password -- that
# comes only from FEEDVALVE_DB_PASSWORD in the process environment, never a
# flag (visible in ps) and never a file (committed and copied between
# environments).
if [ -f config.example.json ]; then
	echo "+ config.example.json -> config.json (EDIT BEFORE DEPLOYING)"
	cp config.example.json "$OUT/config.json"
else
	# Loud, because config.json is one of the three artefacts Vol 4 section 8
	# names and the smoke test's very first step reads it. Silently shipping a
	# directory without it means the far end discovers the gap after the copy.
	echo "+ config.json: NOT PRODUCED -- no config.example.json in the tree"
	MISSING_CONFIG=1
fi

echo "+ the smoke script"
cp scripts/lpar-smoke.sh "$OUT/"

cat <<EOF

artefacts in $OUT:
$(ls -1 "$OUT" | sed 's/^/  /')

next, on the LPAR:
  1. copy $OUT to the target
  2. edit config.json for that environment
  3. export FEEDVALVE_DB_PASSWORD=...
  4. sh ./lpar-smoke.sh

The binary has NOT been executed by this script. It cannot be: the host is not
AIX. Everything above is a cross-compile.
EOF

if [ "$MISSING_CONFIG" -ne 0 ]; then
	cat <<'EOF'

WARNING: no config.json in the artefact set.

config.json is one of the three artefacts Vol 4 section 8 names, it must carry
NO password, and it has to be versioned with the binary. Step 1 of the smoke
test reads it. Supply one before copying this directory anywhere.
EOF
	exit 1
fi

