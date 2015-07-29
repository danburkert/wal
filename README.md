# wal

A work-in-progress write ahead logging library for Rust.

## Roadmap

- [x] file based segment
- [x] benchmarks
- [ ] mmap based segment
- [ ] higher level wal type
- [ ] asynchronous appends
- [ ] compression

## Running Benchmarks

```bash
cargo run --example bench --release -- append --help
```

