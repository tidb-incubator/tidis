.PHONY: default check unit-test integration-test test all debug release

default: check debug

check:
	cargo check --all --all-targets --all-features
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D clippy::all

debug:
	cargo build

release:
	cargo build --release

unit-test:
	cargo test --all

integration-test:
	@echo "start tikv-service manually"
	python3 test/test_helper.py -p 6666

test: unit-test integration-test

all: check test
