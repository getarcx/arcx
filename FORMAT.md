# ARCX Format Specification

Version: 0.1

This document describes the binary layout of `.arcx` archive files.

## Overview

An ARCX archive consists of four regions laid out sequentially:

```
+----------+-------------------+----------+--------+
|  Header  |   Data Blocks     | Manifest | Footer |
| (80 B)   |   (variable)      | (var.)   | (40 B) |
+----------+-------------------+----------+--------+
```

All multi-byte integers are little-endian unless otherwise noted.

## Header (80 bytes)

| Offset | Size | Field | Description |
|-------:|-----:|-------|-------------|
| 0 | 8 | `magic` | `ARCX0001` (ASCII) |
| 8 | 4 | `version_major` | Format major version (u32) |
| 12 | 4 | `version_minor` | Format minor version (u32) |
| 16 | 8 | `manifest_offset` | Byte offset to the manifest region (u64) |
| 24 | 8 | `data_offset` | Byte offset to the first data block (u64) |
| 32 | 8 | `index_offset` | Byte offset to the manifest index within the manifest (u64) |
| 40 | 8 | `flags` | Bitfield for format flags (u64, currently 0) |
| 48 | 8 | `creation_timestamp` | Unix timestamp in seconds (u64) |
| 56 | 24 | `reserved` | Reserved for future use (zero-filled) |

The `magic` field identifies the file as an ARCX archive and encodes the format generation. Readers should reject files with unrecognized magic bytes.

## Data Blocks

Data blocks begin at `data_offset` (typically byte 80) and are stored contiguously. Each block is a length-prefixed compressed payload:

```
+------------------+---------------------------+
| compressed_size  |    compressed payload      |
|    (4 bytes LE)  |    (compressed_size bytes) |
+------------------+---------------------------+
```

- **compressed_size**: u32 little-endian. The number of bytes in the compressed payload that follows.
- **compressed payload**: The block data compressed with zstd. When decompressed, contains the raw bytes of one or more file chunks concatenated together.

Blocks are written sequentially. The manifest records the byte offset and compressed size of each block, so readers can seek directly to any block.

### Block Contents

Each block contains one or more chunks. A chunk is a contiguous range of bytes from a single file. Small files fit entirely within one chunk (and therefore one block). Large files are split across multiple chunks in consecutive blocks.

The mapping from files to chunks to blocks is recorded in the manifest.

### Block Size

The default target block size before compression is 256 KB. The packer groups files into blocks up to this threshold. Files larger than the block size are split into multiple chunks.

## Manifest

The manifest is a binary structure stored between the last data block and the footer. It contains all metadata needed to locate and extract any file in the archive.

### Manifest Layout

The manifest begins with a 4-byte magic: `MFv2` (ASCII).

The manifest is zstd-compressed. After decompression, it contains the following sections in order:

#### 1. Config Section

Archive-level configuration encoded as MessagePack:

| Field | Type | Description |
|-------|------|-------------|
| `chunk_size` | u64 | Target block size in bytes (default: 262144) |
| `small_file_threshold` | u64 | Files below this size are packed together |
| `codec_default` | string | Default compression codec (`"zstd"`) |
| `zstd_level` | u64 | Zstd compression level used |
| `compression_mode` | string | Mode used during packing (`"fast"`, `"balanced"`) |

#### 2. File Table

An array of file entries. Each entry contains:

| Field | Type | Description |
|-------|------|-------------|
| `file_id` | varint | Unique file identifier (0-indexed, sequential) |
| `path` | string | File path relative to archive root (forward slashes) |
| `size` | varint | Original uncompressed file size in bytes |
| `sha256` | 32 bytes | SHA-256 hash of the original file contents |
| `chunk_refs` | varint[] | List of chunk IDs that make up this file, in order |

#### 3. Chunk Table

An array of chunk entries mapping file segments to blocks:

| Field | Type | Description |
|-------|------|-------------|
| `chunk_id` | varint | Unique chunk identifier |
| `file_id` | varint | ID of the file this chunk belongs to |
| `file_offset` | varint | Byte offset within the original file |
| `size` | varint | Size of this chunk in uncompressed bytes |
| `block_id` | varint | ID of the block containing this chunk |

#### 4. Block Table

An array of block entries providing the physical location of each block:

| Field | Type | Description |
|-------|------|-------------|
| `block_id` | varint | Unique block identifier |
| `codec` | string | Compression codec used (`"zstd"`) |
| `offset` | varint | Byte offset from start of file to the block's size prefix |
| `compressed_size` | varint | Size of the compressed block payload (excluding the 4-byte prefix) |
| `decompressed_size` | varint | Size of the block after decompression |
| `checksum` | u32 | CRC32 checksum of the compressed payload |

### Encoding Optimizations

The manifest uses several techniques to minimize its size:

- **String table**: File paths share a deduplicated string table. Each path is stored once and referenced by index.
- **Varint encoding**: Integer fields use variable-length encoding (smaller values use fewer bytes).
- **Delta encoding**: Block offsets are stored as deltas from the previous block, reducing magnitude.
- **Zstd compression**: The entire serialized manifest is zstd-compressed before writing.

## Footer (40 bytes)

The footer is always the last 40 bytes of the archive. It provides a fixed-location entry point for readers.

| Offset | Size | Field | Description |
|-------:|-----:|-------|-------------|
| 0 | 8 | `magic` | `ARCXEND1` (ASCII) |
| 8 | 8 | `manifest_offset` | Byte offset to the start of the manifest region (u64) |
| 16 | 8 | `manifest_size` | Size of the manifest region in bytes (u64) |
| 24 | 4 | `total_files` | Total number of files in the archive (u32) |
| 28 | 4 | `checksum` | CRC32 checksum of the manifest (u32) |
| 32 | 8 | `reserved` | Reserved (zero-filled) |

### Reading Strategy

To open an ARCX archive:

1. Seek to the last 40 bytes and read the footer
2. Verify the footer magic is `ARCXEND1`
3. Seek to `manifest_offset` and read `manifest_size` bytes
4. Verify the manifest CRC32 matches `checksum`
5. Decompress and parse the manifest
6. The archive is now ready for queries

This makes opening an archive a constant-time operation regardless of archive size: read footer, read manifest, done.

## Version Compatibility

- **Major version**: Incremented for breaking changes. Readers must reject archives with an unsupported major version.
- **Minor version**: Incremented for backward-compatible additions. Readers should accept archives with a higher minor version than they support, ignoring unknown fields.

Current version: `0.1`

The format is pre-1.0 and may change. Stability guarantees begin at version `1.0`.
