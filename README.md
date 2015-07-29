# wal

A work-in-progress write ahead logging library for Rust.

## Roadmap

[*] file based segment
[*] benchmarks
[ ] mmap based segment
[ ] higher level wal abstraction
[ ] asynchronous appends
[ ] compression

## Running Benchmarks

```bash
cargo run --example bench --release -- append --help
```

