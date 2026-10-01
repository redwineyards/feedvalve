#!/bin/sh
# Builds the poc relay natively on an AIX machine, producing a binary meant
# to run on that same AIX machine (Go only supports aix/ppc64 — there is no
# cross-compilation target for AIX, so this must be run ON an AIX host).
#
# Requires on the AIX build host:
#   - Go with aix/ppc64 support (Go 1.12+)
#   - CGO_ENABLED=1: a C compiler (gcc from the AIX Toolbox for Linux
#     Applications, or IBM XL C/C++) and liblzo2 headers ("lzo/lzo1z.h",
#     "lzo/lzoconf.h") + library available for linking
#
# Usage:
#   ./build_aix.sh [output_path]
#
# Environment overrides:
#   CC             C compiler to use (default: cc, falling back to gcc)
#   LZO_INCLUDE_DIR  directory containing lzo/lzo1z.h, lzo/lzoconf.h
#                     (e.g. /opt/freeware/include); added via -I
#   LZO_LIB_DIR       directory containing liblzo2 (e.g. /opt/freeware/lib);
#                      added via -L
#
# Example:
#   LZO_INCLUDE_DIR=/opt/freeware/include LZO_LIB_DIR=/opt/freeware/lib \
#       ./build_aix.sh ./gobrdclnt

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
OUT=${1:-"$SCRIPT_DIR/gobrdclnt"}

if [ "$(uname -s 2>/dev/null || echo unknown)" != "AIX" ]; then
	echo "WARNING: not running on AIX (uname -s != AIX); continuing anyway" >&2
fi

if ! command -v go >/dev/null 2>&1; then
	echo "ERROR: go not found on PATH" >&2
	exit 1
fi

CC=${CC:-}
if [ -z "$CC" ]; then
	if command -v gcc >/dev/null 2>&1; then
		CC=gcc
	elif command -v cc >/dev/null 2>&1; then
		CC=cc
	else
		echo "ERROR: no C compiler found (looked for gcc, cc); set CC explicitly" >&2
		exit 1
	fi
fi
echo "Using C compiler: $CC"

CGO_CFLAGS=${CGO_CFLAGS:-}
CGO_LDFLAGS=${CGO_LDFLAGS:-}
if [ -n "${LZO_INCLUDE_DIR:-}" ]; then
	CGO_CFLAGS="$CGO_CFLAGS -I$LZO_INCLUDE_DIR"
fi
if [ -n "${LZO_LIB_DIR:-}" ]; then
	CGO_LDFLAGS="$CGO_LDFLAGS -L$LZO_LIB_DIR"
fi

export CC
export CGO_ENABLED=1
export CGO_CFLAGS
export CGO_LDFLAGS
export GOOS=aix
export GOARCH=ppc64

echo "Building $OUT (GOOS=$GOOS GOARCH=$GOARCH CGO_ENABLED=$CGO_ENABLED)"
( cd "$SCRIPT_DIR" && go build -o "$OUT" . )

echo "Built: $OUT"
