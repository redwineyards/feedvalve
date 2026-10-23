# CI requirement (G-RACE-1, G-RACE-2)

No CI configuration exists in this repository. This file is the requirement a CI system must
implement; Vol 4 s7.4 is the source. There is no race detector on `aix/ppc64`, so these runs on
`linux/amd64` (or any host with `-race`) are the only place race correctness is established.

## On every commit (G-RACE-1)

```
go build ./...
go vet ./...
go test ./...
go test -race ./...
CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go vet ./...
```

`-race` needs cgo and a C compiler on the CI host; that is a property of the CI host only, the
shipped binary stays `CGO_ENABLED=0`.

Known red: `go test -race ./internal/app/` is not green because of the open `obs.Emit` concurrent
race (predates P2-08, tracked separately). Expect that package to fail under `-race` until it is
fixed; every other package should be green. Do not treat it as a regression from your change.

Any `t.Skip` that applies under `-race` must carry a comment naming the reason. Today none does:
every existing skip is for a missing source tree, toolchain or database (each states its reason at
the call), not for the race detector.

## Before merging a scheduling-sensitive change (G-RACE-2)

One package at a time, because a single run proves very little:

```
go test -race -count=20 -run TestG_ ./internal/mirror/   # G-FLUSH, G-EPOCH, G-INFL, G-ROW
go test -race -count=20 -run TestG_ ./internal/writer/   # G-ROW, G-LIFE-4 (writer half)
go test -race -count=20 -run TestG_CFG_3 ./internal/app/
go test -race -count=20 -run TestG_LIFE_4 ./internal/app/
go test -race -count=20 -run TestG_ROW_3 ./internal/app/
```

The 60 s stress (G-RACE-3) is run with `-tags feedvalve_assert` and a raised `-timeout`; see the
header of `internal/app/stress_test.go`.

## Toolchain notes

On the Windows dev host `CC` must be the full clang path (`/c/msys64/clang64/bin/clang.exe`) with
`/c/msys64/clang64/bin` on `PATH`; `gcc` is broken there.

## On the LPAR (not CI)

`scripts/build-aix.sh` produces the artefact set; `scripts/lpar-smoke.sh` runs on the target
(G-AIX-1 assertions 1-5, G-ASSERT-1, and the SIGHUP step for OQ-L7/ASM-A9). Results are recorded in
the phase as-built notes.
