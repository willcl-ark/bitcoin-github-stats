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

# Generate site/data.json from DB
[group('admin')]
generate:
    python3 analyze.py

# Serve site locally
[group('admin')]
serve:
    python3 -m http.server 8000 -d site

# Generate and serve
[group('admin')]
dev: generate serve

# Sync to remote stats.fish.foo
[group('admin')]
sync: generate
    rsync -a site/ seedbox-root:/var/www/stats/
