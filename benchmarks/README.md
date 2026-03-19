# ARCX Benchmarks

## Methodology

### System

- **OS**: Windows 11 Pro (10.0.26200)
- **CPU**: Intel Core (22 logical cores)
- **Runs**: 3 per measurement, median reported

### Datasets

All datasets are procedurally generated to represent common real-world workloads:

| Dataset | Files | Input Size | Description |
|---------|------:|----------:|-------------|
| Node.js project | 19,001 | 42.6 MB | Deep `node_modules` tree with `.js`, `.d.ts`, `.json` |
| Python ML project | 206 | 63.8 MB | Mix of small `.py` files and large binary model weights |
| Build artifacts | 581 | 180.9 MB | Compiled `.o` files, binaries, and build metadata |
| Log archive | 1,008 | 30.4 MB | Structured log files organized by date |
| Source code repo | 389 | 1.9 MB | Go project with tests, docs, configs |

### Formats Compared

- **ARCX** -- Rust implementation (`arcx pack` / `arcx get`)
- **TAR+ZSTD** -- Python `tarfile` + `zstandard` (in-memory tar, then zstd compress)
- **TAR+GZ** -- Python `tarfile` with gzip compression
- **ZIP** -- Python `zipfile` with deflate compression

### Measurements

- **Pack time**: Time to create the archive from source files
- **Extract all**: Time to extract every file from the archive
- **Selective extract**: Time to extract a single named file from the archive
- **Bytes read**: Data touched during selective extraction (ARCX: manifest + block; TAR: full archive; ZIP: central directory + local entry)

## Results

### Compression Ratios

| Dataset | ARCX | TAR+ZSTD | ZIP |
|---------|-----:|---------:|----:|
| Node.js project | 44.0% | 40.8% | 50.9% |
| Python ML project | 98.9% | 98.9% | 98.9% |
| Build artifacts | 77.6% | 77.6% | 77.7% |
| Log archive | 16.5% | 16.6% | 16.8% |
| Source code repo | 34.3% | 33.0% | 36.3% |

ARCX and TAR+ZSTD achieve nearly identical compression ratios because both compress files together (cross-file compression). ZIP compresses each file independently, resulting in slightly worse ratios for compressible workloads.

### Pack Speed

| Dataset | ARCX | TAR+ZSTD | ZIP |
|---------|-----:|---------:|----:|
| Node.js project | 6.84 s | 3.33 s | 4.53 s |
| Python ML project | 281.7 ms | 399.9 ms | 1.75 s |
| Build artifacts | 724.6 ms | 935.4 ms | 3.87 s |
| Log archive | 439.9 ms | 526.2 ms | 689.2 ms |
| Source code repo | 87.9 ms | 74.6 ms | 103.8 ms |

ARCX pack speed is competitive with TAR+ZSTD and consistently faster than ZIP, except on the 19K-file Node.js dataset where per-file manifest overhead dominates. The Node.js pack time is an area for optimization.

### Selective Extraction Speed

| Dataset | ARCX | TAR+ZSTD | TAR+GZ | ZIP |
|---------|-----:|---------:|-------:|----:|
| Node.js project | 131.3 ms | 840.9 ms | 1.26 s | 62.5 ms |
| Python ML project | 2.8 ms | 53.4 ms | 180.0 ms | 863 us |
| Build artifacts | 7.9 ms | 147.7 ms | 409.3 ms | 1.7 ms |
| Log archive | 7.3 ms | 99.5 ms | 238.4 ms | 3.0 ms |
| Source code repo | 3.5 ms | 19.3 ms | 45.3 ms | 1.2 ms |

### Bytes Read for Selective Extraction

| Dataset | Target Size | ARCX | TAR+ZSTD | ZIP |
|---------|----------:|-----:|---------:|----:|
| Node.js project | 1.5 KB | 1.3 MB | 17.4 MB | 632 B |
| Python ML project | 6.9 KB | 326.1 KB | 63.1 MB | 2.1 KB |
| Build artifacts | 36.4 KB | 713.8 KB | 140.4 MB | 36.5 KB |
| Log archive | 35.8 KB | 365.8 KB | 5.0 MB | 6.1 KB |
| Source code repo | 4.5 KB | 373.8 KB | 656.1 KB | 1.9 KB |

## Known Limitations

1. **Synthetic data**: Procedurally generated datasets. Real-world data may have different entropy profiles.
2. **Mixed language comparison**: ARCX is Rust; TAR/ZIP benchmarks use Python. Absolute times are not directly comparable for pack/extract-all. Selective extraction comparison is fair because it is I/O-bound.
3. **In-memory TAR+ZSTD**: The benchmark builds the tar in memory then compresses, which may differ from a streaming pipeline.
4. **No network simulation**: All I/O is local disk. Selective extraction advantages would be amplified over network/cloud storage.
5. **Cold cache**: No effort to warm OS page cache.
6. **ARCX selective bytes-read is estimated**: Includes manifest overhead plus the blocks needed for the target file.
7. **ZIP random access**: ZIP natively supports random access via its central directory. The comparison is most meaningful against TAR-based formats.

## Raw Data

See [results.json](results.json) for machine-readable benchmark data.
