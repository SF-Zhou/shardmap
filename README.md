# shardmap

[![Rust](https://github.com/SF-Zhou/shardmap/actions/workflows/rust.yml/badge.svg)](https://github.com/SF-Zhou/shardmap/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/SF-Zhou/shardmap/graph/badge.svg?token=7U9JFC64U4)](https://codecov.io/gh/SF-Zhou/shardmap)
[![Crates.io](https://img.shields.io/crates/v/shardmap.svg)](https://crates.io/crates/shardmap)
[![Documentation](https://docs.rs/shardmap/badge.svg)](https://docs.rs/shardmap)

A sharded hashmap optimized for concurrent writes (via per-shard mutexes) and lock-free immutable snapshots for high-performance reads after bulk export.
