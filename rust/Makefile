# If `make check` passes, the tree is shippable.

.PHONY: check test clippy fmt race bench regold go all

all: check go

check: fmt clippy test

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

# The on-disk format is the system of record. Regenerating puts the byte diff
# in the review, which is where it belongs.
regold:
	SENTINEL_REGOLD=1 cargo test -p sentinel-journal --test golden

bench:
	cargo run --release -p sentinel-journal --example group_commit

# Go reads the same journal bytes. A format change that passes here and fails
# there is a format change that shipped half-done.
go:
	cd ../go && gofmt -l . && go vet ./... && go test ./...
