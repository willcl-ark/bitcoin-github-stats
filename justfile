[private]
default:
    just --list

# Build the flake output
[group('nix')]
build:
    nix build

# Run flake checks
[group('nix')]
check:
    nix flake check

# Run the flake default app
[group('nix')]
run:
    nix run

# Format with treefmt via nix
[group('nix')]
fmt:
    nix fmt

# Compile with cargo directly
[group('rust')]
buildc:
    cargo build

# Lint and test: check, clippy, then tests
[group('rust')]
checkc:
    #!/usr/bin/env bash
    cargo check
    cargo clippy
    cargo test

# Run via cargo directly
[group('rust')]
runc:
    cargo run

# Sync to remove stats.fish.foo
[group('admin')]
sync:
    rsync ./report.html seedbox-root:/var/www/stats/index.html
