#!/bin/bash

set -e

cargo fmt --all -- --check
# List of clippy lint options here: https://rust-lang.github.io/rust-clippy/master/index.html
cargo clippy --workspace --all-targets -- -D warnings
export RUSTDOCFLAGS="-D warnings"
cargo doc --workspace --no-deps
