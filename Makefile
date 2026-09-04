GO ?= go
GOEXPERIMENT ?= simd
export GOEXPERIMENT
PACKAGES ?= ./...
TEST_FLAGS ?=
BENCH_PACKAGES ?= ./distribution ./internal/rangesplit
BENCH ?= ^Benchmark(DocumentPointProgram|PartitionRowsOnePass|TranslateTailEntryMove)$
BENCHTIME ?= 250ms
COUNT ?= 1

.PHONY: build test bench vet info
build:
	$(GO) build $(PACKAGES)

test:
	$(GO) test $(value TEST_FLAGS) $(PACKAGES)

bench: info
	$(GO) test -run '^$$' -bench '$(value BENCH)' -benchmem -benchtime=$(BENCHTIME) -count=$(COUNT) -cpu=1 $(BENCH_PACKAGES)

vet:
	$(GO) vet $(PACKAGES)

info:
	$(GO) version
	$(GO) env -json GOEXPERIMENT GOOS GOARCH GOAMD64
