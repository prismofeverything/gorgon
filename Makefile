# gorgon developer tasks.
#
# `make setup` installs the *system* build dependencies (Cargo fetches the Rust
# crates itself). The native deps come from the `remote` feature: on Linux the
# `pipewire` crate links libpipewire-0.3 and runs bindgen (which needs clang).
# See the README "Requirements" section for details.

.PHONY: help setup build check test run

.DEFAULT_GOAL := help

UNAME_S := $(shell uname -s)

help:
	@echo "gorgon make targets:"
	@echo "  setup   install system build deps for this OS"
	@echo "  build   cargo build --release"
	@echo "  check   cargo check --all-targets"
	@echo "  test    cargo test"
	@echo "  run     run gorgon, e.g. make run ARGS=\"stream --list-devices\""

# Install system build dependencies for the current OS. Rust itself (rustup) and
# Tailscale are assumed already installed.
setup:
ifeq ($(UNAME_S),Linux)
	@echo "Installing Linux build deps via apt…"
	sudo apt-get update
	sudo apt-get install -y build-essential pkg-config clang libasound2-dev libpipewire-0.3-dev
else ifeq ($(UNAME_S),Darwin)
	@echo "macOS needs no build-time system libraries (CoreAudio is built in)."
	@echo "For the 'remote' feature, install the BlackHole audio driver:"
	@echo "    brew install blackhole-16ch   # or https://existential.audio/blackhole/"
else
	@echo "Unsupported OS '$(UNAME_S)'. See the README 'Requirements' section."
	@exit 1
endif

build:
	cargo build --release

check:
	cargo check --all-targets

test:
	cargo test

# Forward args to the binary: make run ARGS="stream --list-devices"
run:
	cargo run --release -- $(ARGS)
