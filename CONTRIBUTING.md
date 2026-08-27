# Contributing

Mosaic is currently a research prototype. Keep the control model explicit and small: avoid abstractions that silently assume low latency, fixed cluster membership, or replay-safe side effects.

Before submitting changes:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Design changes that alter Task/Object/Resource semantics, lease behavior, retry guarantees, or topology assumptions should include an ADR under `docs/adr/`.
