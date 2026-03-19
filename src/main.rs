use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use memmap2::Mmap;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Read as _, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[cfg(feature = "fuse")]
mod fuse_mount;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAGIC: &[u8; 8] = b"ARCX0001";
const FOOTER_MAGIC: &[u8; 8] = b"ARCXEND1";
const HEADER_SIZE: usize = 80;
const FOOTER_SIZE: usize = 40;
const MANIFEST_MAGIC: &[u8; 4] = b"MFv2";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "arcx", about = "ARCX archive tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Extract a single file from the archive (local or remote s3:// / https://)
    Get {
        /// Path or URL to the .arcx archive
        archive: String,
        /// Path of the file inside the archive
        file_path: String,
        /// Output file (default: stdout-like, writes to filename in current dir)
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Print detailed timing breakdown
        #[arg(long)]
        time: bool,
    },
    /// List all files in the archive
    List {
        /// Path or URL to the .arcx archive
        archive: String,
    },
    /// Extract all files from the archive
    Extract {
        /// Path or URL to the .arcx archive
        archive: String,
        /// Output directory (default: current directory)
        output_dir: Option<PathBuf>,
    },
    /// Show archive metadata
    Info {
        /// Path or URL to the .arcx archive
        archive: String,
    },
    /// Pack a directory into an .arcx archive
    Pack {
        /// Input directory to pack
        input_dir: PathBuf,
        /// Output .arcx archive path
        output: PathBuf,
    },
    /// Mount an archive as a read-only FUSE filesystem
    #[cfg(feature = "fuse")]
    Mount {
        /// Path to the .arcx archive
        archive: String,
        /// Mountpoint directory
        mountpoint: PathBuf,
        /// Max number of decompressed blocks to cache (default: 64)
        #[arg(long, default_value = "64")]
        cache_size: usize,
    },
}

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct Header {
    manifest_offset: u64,
    data_offset: u64,
    index_offset: u64,
    flags: u64,
    creation_timestamp: u64,
    version_major: u32,
    version_minor: u32,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct Config {
    chunk_size: u64,
    small_file_threshold: u64,
    codec_default: String,
    zstd_level: u64,
    compression_mode: String,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct FileEntry {
    pub(crate) file_id: u64,
    pub(crate) path: String,
    pub(crate) size: u64,
    pub(crate) mode: u64,
    pub(crate) mtime_ns: u64,
    pub(crate) sha256: String,
    pub(crate) chunk_refs: Vec<u64>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ChunkEntry {
    pub(crate) chunk_id: u64,
    pub(crate) file_id: u64,
    pub(crate) file_offset: u64,
    pub(crate) size: u64,
    pub(crate) block_id: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct BlockEntry {
    pub(crate) block_id: u64,
    pub(crate) codec: String,
    pub(crate) offset: u64,
    pub(crate) compressed_size: u64,
    pub(crate) uncompressed_size: u64,
    pub(crate) checksum: String,
}

#[derive(Debug)]
pub(crate) struct Manifest {
    pub(crate) config: Config,
    pub(crate) files: Vec<FileEntry>,
    pub(crate) chunks: Vec<ChunkEntry>,
    pub(crate) blocks: Vec<BlockEntry>,
}

/// Partially-parsed manifest: only config, string table, and file entries are
/// parsed eagerly. Chunk and block sections are kept as raw byte ranges and
/// parsed on-demand when a file is actually extracted.
struct LazyManifest {
    config: Config,
    files: Vec<FileEntry>,
    /// Decompressed manifest payload — kept alive for on-demand chunk/block parsing.
    payload: Vec<u8>,
    /// Byte offset within `payload` where the chunk section starts.
    chunks_offset: usize,
    /// Byte offset within `payload` where the block section starts.
    blocks_offset: usize,
    /// Lazily-populated chunk and block data.
    chunks: Option<Vec<ChunkEntry>>,
    blocks: Option<Vec<BlockEntry>>,
}

/// Timing breakdown for a single `get` operation.
#[derive(Default)]
pub struct GetTimings {
    pub manifest_parse_us: u128,
    pub file_lookup_us: u128,
    pub block_read_us: u128,
    pub decompress_us: u128,
    pub total_us: u128,
}

// ---------------------------------------------------------------------------
// Varint decoding (unsigned LEB128)
// ---------------------------------------------------------------------------

fn decode_varint(data: &[u8], offset: usize) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    let mut pos = offset;
    loop {
        if pos >= data.len() {
            bail!("Unexpected end of data reading varint at offset {}", offset);
        }
        let b = data[pos];
        result |= ((b & 0x7F) as u64) << shift;
        pos += 1;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            bail!("Varint too large at offset {}", offset);
        }
    }
    Ok((result, pos))
}

fn decode_signed_varint(data: &[u8], offset: usize) -> Result<(i64, usize)> {
    let (zigzag, new_offset) = decode_varint(data, offset)?;
    let value = if zigzag & 1 == 0 {
        (zigzag >> 1) as i64
    } else {
        -((zigzag >> 1) as i64) - 1
    };
    Ok((value, new_offset))
}

// ---------------------------------------------------------------------------
// Header / Footer parsing
// ---------------------------------------------------------------------------

fn read_header(mmap: &[u8]) -> Result<Header> {
    if mmap.len() < HEADER_SIZE {
        bail!("File too short for ARCX header");
    }
    let magic = &mmap[0..8];
    if magic != MAGIC {
        bail!("Invalid ARCX magic: {:?}", magic);
    }

    let version_major = u32::from_le_bytes(mmap[8..12].try_into()?);
    let version_minor = u32::from_le_bytes(mmap[12..16].try_into()?);
    let manifest_offset = u64::from_le_bytes(mmap[16..24].try_into()?);
    let data_offset = u64::from_le_bytes(mmap[24..32].try_into()?);
    let index_offset = u64::from_le_bytes(mmap[32..40].try_into()?);
    let flags = u64::from_le_bytes(mmap[40..48].try_into()?);
    let creation_timestamp = u64::from_le_bytes(mmap[48..56].try_into()?);

    Ok(Header {
        manifest_offset,
        data_offset,
        index_offset,
        flags,
        creation_timestamp,
        version_major,
        version_minor,
    })
}

fn validate_footer(mmap: &[u8]) -> Result<()> {
    if mmap.len() < FOOTER_SIZE {
        bail!("File too short for ARCX footer");
    }
    let footer_start = mmap.len() - FOOTER_SIZE;
    let magic = &mmap[footer_start..footer_start + 8];
    if magic != FOOTER_MAGIC {
        bail!("Invalid ARCX footer magic: {:?}", magic);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Manifest deserialization
// ---------------------------------------------------------------------------

fn decode_string_table(data: &[u8], offset: usize) -> Result<(Vec<String>, usize)> {
    let (count, mut pos) = decode_varint(data, offset)?;
    let mut strings = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (slen, new_pos) = decode_varint(data, pos)?;
        pos = new_pos;
        let end = pos + slen as usize;
        if end > data.len() {
            bail!("String table entry extends beyond data");
        }
        let s = std::str::from_utf8(&data[pos..end])
            .context("Invalid UTF-8 in string table")?
            .to_string();
        pos = end;
        strings.push(s);
    }
    Ok((strings, pos))
}

fn decode_config(data: &[u8], offset: usize) -> Result<(Config, usize)> {
    let (chunk_size, mut pos) = decode_varint(data, offset)?;
    let (small_file_threshold, new_pos) = decode_varint(data, pos)?;
    pos = new_pos;

    let codec_byte = data[pos];
    pos += 1;
    let codec_default = if codec_byte == 1 { "zstd" } else { "store" }.to_string();

    let (zstd_level, new_pos) = decode_varint(data, pos)?;
    pos = new_pos;

    let mode_byte = data[pos];
    pos += 1;
    let compression_mode = if mode_byte == 1 { "balanced" } else { "fast" }.to_string();

    Ok((
        Config {
            chunk_size,
            small_file_threshold,
            codec_default,
            zstd_level,
            compression_mode,
        },
        pos,
    ))
}

fn decode_files(data: &[u8], offset: usize, strings: &[String]) -> Result<(Vec<FileEntry>, usize)> {
    let (count, mut pos) = decode_varint(data, offset)?;
    let mut files = Vec::with_capacity(count as usize);

    for _ in 0..count {
        let (file_id, p) = decode_varint(data, pos)?;
        pos = p;
        let (dir_idx, p) = decode_varint(data, pos)?;
        pos = p;
        let (name_idx, p) = decode_varint(data, pos)?;
        pos = p;
        let (size, p) = decode_varint(data, pos)?;
        pos = p;
        let (mode, p) = decode_varint(data, pos)?;
        pos = p;
        let (mtime_ns, p) = decode_varint(data, pos)?;
        pos = p;

        // type byte
        let _type_byte = data[pos];
        pos += 1;

        // sha256: 32 raw bytes
        if pos + 32 > data.len() {
            bail!("File entry sha256 extends beyond data");
        }
        let sha256 = hex::encode(&data[pos..pos + 32]);
        pos += 32;

        // chunk_refs: delta-encoded
        let (chunk_count, p) = decode_varint(data, pos)?;
        pos = p;
        let mut chunk_refs = Vec::with_capacity(chunk_count as usize);
        let mut prev: u64 = 0;
        for _ in 0..chunk_count {
            let (delta, p) = decode_varint(data, pos)?;
            pos = p;
            prev += delta;
            chunk_refs.push(prev);
        }

        // codec_hint byte
        let _codec_byte = data[pos];
        pos += 1;

        // pack_id (signed varint)
        let (_pack_id, p) = decode_signed_varint(data, pos)?;
        pos = p;

        // Reconstruct path
        let dir_part = &strings[dir_idx as usize];
        let name_part = &strings[name_idx as usize];
        let path = if dir_part.is_empty() {
            name_part.clone()
        } else {
            format!("{}/{}", dir_part, name_part)
        };

        files.push(FileEntry {
            file_id,
            path,
            size,
            mode,
            mtime_ns,
            sha256,
            chunk_refs,
        });
    }

    Ok((files, pos))
}

/// Skip past the chunk section without allocating, returning the byte offset
/// just after the last chunk entry.
fn skip_chunks(data: &[u8], offset: usize) -> Result<usize> {
    let (count, mut pos) = decode_varint(data, offset)?;
    for _ in 0..count {
        // chunk_id delta
        let (_, p) = decode_varint(data, pos)?;
        pos = p;
        // file_id
        let (_, p) = decode_varint(data, pos)?;
        pos = p;
        // file_offset
        let (_, p) = decode_varint(data, pos)?;
        pos = p;
        // size
        let (_, p) = decode_varint(data, pos)?;
        pos = p;
        // block_id
        let (_, p) = decode_varint(data, pos)?;
        pos = p;
    }
    Ok(pos)
}

fn decode_chunks(data: &[u8], offset: usize) -> Result<(Vec<ChunkEntry>, usize)> {
    let (count, mut pos) = decode_varint(data, offset)?;
    let mut chunks = Vec::with_capacity(count as usize);
    let mut prev_chunk_id: u64 = 0;

    for _ in 0..count {
        let (delta, p) = decode_varint(data, pos)?;
        pos = p;
        prev_chunk_id += delta;

        let (file_id, p) = decode_varint(data, pos)?;
        pos = p;
        let (file_offset, p) = decode_varint(data, pos)?;
        pos = p;
        let (size, p) = decode_varint(data, pos)?;
        pos = p;
        let (block_id, p) = decode_varint(data, pos)?;
        pos = p;

        chunks.push(ChunkEntry {
            chunk_id: prev_chunk_id,
            file_id,
            file_offset,
            size,
            block_id,
        });
    }

    Ok((chunks, pos))
}

fn decode_blocks(data: &[u8], offset: usize) -> Result<(Vec<BlockEntry>, usize)> {
    let (count, mut pos) = decode_varint(data, offset)?;
    let mut blocks = Vec::with_capacity(count as usize);
    let mut prev_block_id: u64 = 0;
    let mut prev_offset: u64 = 0;

    for _ in 0..count {
        let (delta, p) = decode_varint(data, pos)?;
        pos = p;
        prev_block_id += delta;

        let codec_byte = data[pos];
        pos += 1;
        let codec = if codec_byte == 1 { "zstd" } else { "store" }.to_string();

        let (offset_delta, p) = decode_varint(data, pos)?;
        pos = p;
        prev_offset += offset_delta;

        let (compressed_size, p) = decode_varint(data, pos)?;
        pos = p;
        let (uncompressed_size, p) = decode_varint(data, pos)?;
        pos = p;

        // checksum: 8 raw bytes
        if pos + 8 > data.len() {
            bail!("Block entry checksum extends beyond data");
        }
        let checksum = hex::encode(&data[pos..pos + 8]);
        pos += 8;

        blocks.push(BlockEntry {
            block_id: prev_block_id,
            codec,
            offset: prev_offset,
            compressed_size,
            uncompressed_size,
            checksum,
        });
    }

    Ok((blocks, pos))
}

fn deserialize_manifest(raw: &[u8]) -> Result<Manifest> {
    let lazy = deserialize_manifest_lazy(raw)?;
    let chunks = decode_chunks(&lazy.payload, lazy.chunks_offset)?.0;
    let blocks = decode_blocks(&lazy.payload, lazy.blocks_offset)?.0;
    Ok(Manifest {
        config: lazy.config,
        files: lazy.files,
        chunks,
        blocks,
    })
}

/// Parse only config + string table + file entries. Chunk and block sections
/// are deferred — their byte offsets are recorded for on-demand parsing.
fn deserialize_manifest_lazy(raw: &[u8]) -> Result<LazyManifest> {
    if raw.is_empty() {
        bail!("Empty manifest data");
    }
    let flags = raw[0];
    let payload_compressed = &raw[1..];

    let payload: Vec<u8> = if flags & 0x01 != 0 {
        zstd::decode_all(payload_compressed).context("Failed to decompress manifest")?
    } else {
        payload_compressed.to_vec()
    };

    let mut pos: usize = 0;

    if payload.len() < 5 {
        bail!("Manifest too short");
    }
    if &payload[0..4] != MANIFEST_MAGIC {
        bail!("Invalid manifest magic: {:?}", &payload[0..4]);
    }
    pos += 4;

    let version = payload[pos];
    pos += 1;
    if version != 1 {
        bail!("Unknown manifest version: {}", version);
    }

    // Config
    let (config_len, new_pos) = decode_varint(&payload, pos)?;
    pos = new_pos;
    let (config, new_pos) = decode_config(&payload, pos)?;
    let _ = config_len;
    pos = new_pos;

    // String table
    let (strings, new_pos) = decode_string_table(&payload, pos)?;
    pos = new_pos;

    // Files
    let (files, new_pos) = decode_files(&payload, pos, &strings)?;
    pos = new_pos;

    // Record where chunks start (don't parse yet)
    let chunks_offset = pos;

    // Skip past chunks to find blocks offset (no allocation)
    let blocks_offset = skip_chunks(&payload, pos)?;

    Ok(LazyManifest {
        config,
        files,
        payload,
        chunks_offset,
        blocks_offset,
        chunks: None,
        blocks: None,
    })
}

// ---------------------------------------------------------------------------
// Archive reader
// ---------------------------------------------------------------------------

/// Full-parse reader — used by list, extract, info commands that need the
/// complete manifest.
pub(crate) struct ArchiveReader {
    pub(crate) mmap: Mmap,
    pub(crate) header: Header,
    pub(crate) manifest: Manifest,
    pub(crate) files_by_path: HashMap<String, usize>,
    pub(crate) chunks_by_id: HashMap<u64, usize>,
    pub(crate) blocks_by_id: HashMap<u64, usize>,
}

impl ArchiveReader {
    pub(crate) fn open(path: &std::path::Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("Cannot open archive: {}", path.display()))?;
        let mmap = unsafe { Mmap::map(&file) }.context("Failed to memory-map archive")?;

        let header = read_header(&mmap)?;
        validate_footer(&mmap)?;

        let moff = header.manifest_offset as usize;
        if moff + 4 > mmap.len() {
            bail!("Manifest offset beyond file end");
        }
        let manifest_len = u32::from_le_bytes(mmap[moff..moff + 4].try_into()?) as usize;
        let manifest_start = moff + 4;
        let manifest_end = manifest_start + manifest_len;
        if manifest_end > mmap.len() {
            bail!("Manifest data extends beyond file end");
        }
        let manifest = deserialize_manifest(&mmap[manifest_start..manifest_end])?;

        let files_by_path: HashMap<String, usize> = manifest
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.path.clone(), i))
            .collect();

        let chunks_by_id: HashMap<u64, usize> = manifest
            .chunks
            .iter()
            .enumerate()
            .map(|(i, c)| (c.chunk_id, i))
            .collect();

        let blocks_by_id: HashMap<u64, usize> = manifest
            .blocks
            .iter()
            .enumerate()
            .map(|(i, b)| (b.block_id, i))
            .collect();

        Ok(ArchiveReader {
            mmap,
            header,
            manifest,
            files_by_path,
            chunks_by_id,
            blocks_by_id,
        })
    }

    pub(crate) fn read_block(&self, block_id: u64) -> Result<Vec<u8>> {
        let block_idx = self
            .blocks_by_id
            .get(&block_id)
            .with_context(|| format!("Block {} not found", block_id))?;
        let block = &self.manifest.blocks[*block_idx];

        let off = block.offset as usize;
        if off + 4 > self.mmap.len() {
            bail!("Block {} offset beyond file end", block_id);
        }
        let stored_len = u32::from_le_bytes(self.mmap[off..off + 4].try_into()?) as usize;
        let payload_start = off + 4;
        let payload_end = payload_start + stored_len;
        if payload_end > self.mmap.len() {
            bail!("Block {} payload extends beyond file end", block_id);
        }
        let compressed = &self.mmap[payload_start..payload_end];

        match block.codec.as_str() {
            "store" => Ok(compressed.to_vec()),
            "zstd" => zstd::decode_all(compressed).context("Failed to decompress block"),
            other => bail!("Unknown codec: {}", other),
        }
    }

    fn extract_file(&self, file_path: &str) -> Result<Vec<u8>> {
        let file_idx = self
            .files_by_path
            .get(file_path)
            .with_context(|| format!("File not found in archive: {}", file_path))?;
        let entry = &self.manifest.files[*file_idx];

        let mut block_cache: HashMap<u64, Vec<u8>> = HashMap::new();
        let mut data_parts: Vec<u8> = Vec::with_capacity(entry.size as usize);

        for &cid in &entry.chunk_refs {
            let chunk_idx = self
                .chunks_by_id
                .get(&cid)
                .with_context(|| format!("Chunk {} not found", cid))?;
            let chunk = &self.manifest.chunks[*chunk_idx];
            let bid = chunk.block_id;

            if let std::collections::hash_map::Entry::Vacant(e) = block_cache.entry(bid) {
                let block_data = self.read_block(bid)?;
                e.insert(block_data);
            }

            let block_data = &block_cache[&bid];
            let start = chunk.file_offset as usize;
            let end = start + chunk.size as usize;
            if end > block_data.len() {
                bail!(
                    "Chunk {} slice {}..{} exceeds block {} size {}",
                    cid,
                    start,
                    end,
                    bid,
                    block_data.len()
                );
            }
            data_parts.extend_from_slice(&block_data[start..end]);
        }

        let mut hasher = Sha256::new();
        hasher.update(&data_parts);
        let computed = format!("{:x}", hasher.finalize());
        if computed != entry.sha256 {
            bail!(
                "Hash mismatch for {}: expected {}, got {}",
                file_path,
                entry.sha256,
                computed
            );
        }

        Ok(data_parts)
    }
}

// ---------------------------------------------------------------------------
// Fast reader — lazy manifest parsing for sub-10ms selective extraction
// ---------------------------------------------------------------------------

/// Optimized reader that parses only file entries on open. Chunk and block
/// tables are parsed on-demand when a file is actually requested. Designed
/// to be kept alive across multiple `get` calls (warm manifest cache).
struct FastArchiveReader {
    mmap: Mmap,
    #[allow(dead_code)]
    header: Header,
    manifest: LazyManifest,
    files_by_path: HashMap<String, usize>,
    /// Lazily-built chunk index (chunk_id -> index into chunks vec).
    chunks_by_id: Option<HashMap<u64, usize>>,
    /// Lazily-built block index (block_id -> index into blocks vec).
    blocks_by_id: Option<HashMap<u64, usize>>,
}

impl FastArchiveReader {
    /// Open an archive with lazy manifest parsing. Only file entries and the
    /// path index are built; chunk/block tables are deferred.
    fn open(path: &std::path::Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("Cannot open archive: {}", path.display()))?;
        let mmap = unsafe { Mmap::map(&file) }.context("Failed to memory-map archive")?;

        let header = read_header(&mmap)?;
        validate_footer(&mmap)?;

        let moff = header.manifest_offset as usize;
        if moff + 4 > mmap.len() {
            bail!("Manifest offset beyond file end");
        }
        let manifest_len = u32::from_le_bytes(mmap[moff..moff + 4].try_into()?) as usize;
        let manifest_start = moff + 4;
        let manifest_end = manifest_start + manifest_len;
        if manifest_end > mmap.len() {
            bail!("Manifest data extends beyond file end");
        }
        let manifest = deserialize_manifest_lazy(&mmap[manifest_start..manifest_end])?;

        let files_by_path: HashMap<String, usize> = manifest
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.path.clone(), i))
            .collect();

        Ok(FastArchiveReader {
            mmap,
            header,
            manifest,
            files_by_path,
            chunks_by_id: None,
            blocks_by_id: None,
        })
    }

    /// Ensure chunk and block tables are parsed and indexed.
    fn ensure_chunks_and_blocks(&mut self) -> Result<()> {
        if self.manifest.chunks.is_none() {
            let (chunks, _) = decode_chunks(&self.manifest.payload, self.manifest.chunks_offset)?;
            let idx: HashMap<u64, usize> = chunks
                .iter()
                .enumerate()
                .map(|(i, c)| (c.chunk_id, i))
                .collect();
            self.manifest.chunks = Some(chunks);
            self.chunks_by_id = Some(idx);
        }
        if self.manifest.blocks.is_none() {
            let (blocks, _) = decode_blocks(&self.manifest.payload, self.manifest.blocks_offset)?;
            let idx: HashMap<u64, usize> = blocks
                .iter()
                .enumerate()
                .map(|(i, b)| (b.block_id, i))
                .collect();
            self.manifest.blocks = Some(blocks);
            self.blocks_by_id = Some(idx);
        }
        Ok(())
    }

    /// Read and decompress a single block from the mmap. Returns (raw_bytes, decompress_duration).
    fn read_block(
        &self,
        block_id: u64,
    ) -> Result<(Vec<u8>, std::time::Duration, std::time::Duration)> {
        let blocks_idx = self.blocks_by_id.as_ref().expect("blocks not parsed");
        let block_idx = blocks_idx
            .get(&block_id)
            .with_context(|| format!("Block {} not found", block_id))?;
        let blocks = self.manifest.blocks.as_ref().expect("blocks not parsed");
        let block = &blocks[*block_idx];

        let t_read = Instant::now();
        let off = block.offset as usize;
        if off + 4 > self.mmap.len() {
            bail!("Block {} offset beyond file end", block_id);
        }
        let stored_len = u32::from_le_bytes(self.mmap[off..off + 4].try_into()?) as usize;
        let payload_start = off + 4;
        let payload_end = payload_start + stored_len;
        if payload_end > self.mmap.len() {
            bail!("Block {} payload extends beyond file end", block_id);
        }
        let compressed = &self.mmap[payload_start..payload_end];
        let read_dur = t_read.elapsed();

        let t_decomp = Instant::now();
        let data = match block.codec.as_str() {
            "store" => compressed.to_vec(),
            "zstd" => zstd::decode_all(compressed).context("Failed to decompress block")?,
            other => bail!("Unknown codec: {}", other),
        };
        let decomp_dur = t_decomp.elapsed();

        Ok((data, read_dur, decomp_dur))
    }

    /// Extract a file with detailed timing breakdown.
    fn extract_file_timed(&mut self, file_path: &str) -> Result<(Vec<u8>, GetTimings)> {
        let total_start = Instant::now();

        // Phase 1: chunk/block parse (lazy — skipped if already cached)
        let t_parse = Instant::now();
        self.ensure_chunks_and_blocks()?;
        let manifest_parse_us = t_parse.elapsed().as_micros();

        // Phase 2: file lookup
        let t_lookup = Instant::now();
        let file_idx = *self
            .files_by_path
            .get(file_path)
            .with_context(|| format!("File not found in archive: {}", file_path))?;
        let entry = &self.manifest.files[file_idx];
        let chunk_refs = entry.chunk_refs.clone();
        let expected_size = entry.size;
        let expected_sha = entry.sha256.clone();
        let file_lookup_us = t_lookup.elapsed().as_micros();

        // Phase 3 & 4: block read + decompression
        let mut block_cache: HashMap<u64, Vec<u8>> = HashMap::new();
        let mut data_parts: Vec<u8> = Vec::with_capacity(expected_size as usize);
        let mut total_block_read = std::time::Duration::ZERO;
        let mut total_decompress = std::time::Duration::ZERO;

        let chunks_by_id = self.chunks_by_id.as_ref().expect("chunks not parsed");
        let chunks = self.manifest.chunks.as_ref().expect("chunks not parsed");

        for &cid in &chunk_refs {
            let chunk_idx = chunks_by_id
                .get(&cid)
                .with_context(|| format!("Chunk {} not found", cid))?;
            let chunk = &chunks[*chunk_idx];
            let bid = chunk.block_id;

            if let std::collections::hash_map::Entry::Vacant(e) = block_cache.entry(bid) {
                let (block_data, read_dur, decomp_dur) = self.read_block(bid)?;
                total_block_read += read_dur;
                total_decompress += decomp_dur;
                e.insert(block_data);
            }

            let block_data = &block_cache[&bid];
            let start = chunk.file_offset as usize;
            let end = start + chunk.size as usize;
            if end > block_data.len() {
                bail!(
                    "Chunk {} slice {}..{} exceeds block {} size {}",
                    cid,
                    start,
                    end,
                    bid,
                    block_data.len()
                );
            }
            data_parts.extend_from_slice(&block_data[start..end]);
        }

        // Verify SHA-256
        let mut hasher = Sha256::new();
        hasher.update(&data_parts);
        let computed = format!("{:x}", hasher.finalize());
        if computed != expected_sha {
            bail!(
                "Hash mismatch for {}: expected {}, got {}",
                file_path,
                expected_sha,
                computed
            );
        }

        let timings = GetTimings {
            manifest_parse_us,
            file_lookup_us,
            block_read_us: total_block_read.as_micros(),
            decompress_us: total_decompress.as_micros(),
            total_us: total_start.elapsed().as_micros(),
        };

        Ok((data_parts, timings))
    }

    /// Extract a file without timing (convenience wrapper).
    #[allow(dead_code)]
    fn extract_file(&mut self, file_path: &str) -> Result<Vec<u8>> {
        let (data, _) = self.extract_file_timed(file_path)?;
        Ok(data)
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

// ---------------------------------------------------------------------------
// Remote URL helpers
// ---------------------------------------------------------------------------

/// Returns true if the archive string looks like a remote URL.
fn is_remote_url(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://") || s.starts_with("s3://")
}

/// Convert `s3://bucket/key` to `https://bucket.s3.amazonaws.com/key`.
/// HTTP(S) URLs pass through unchanged.
fn resolve_url(s: &str) -> Result<String> {
    if let Some(rest) = s.strip_prefix("s3://") {
        // strip "s3://"
        let slash = rest
            .find('/')
            .with_context(|| format!("Invalid S3 URL (no key): {}", s))?;
        let bucket = &rest[..slash];
        let key = &rest[slash + 1..];
        if key.is_empty() {
            bail!("Invalid S3 URL (empty key): {}", s);
        }
        Ok(format!("https://{}.s3.amazonaws.com/{}", bucket, key))
    } else {
        Ok(s.to_string())
    }
}

// ---------------------------------------------------------------------------
// Remote reader — HTTP byte-range access to ARCX archives
// ---------------------------------------------------------------------------

/// Timing info for a single remote HTTP fetch.
struct FetchTiming {
    label: String,
    bytes: u64,
    duration: std::time::Duration,
}

/// Stats accumulated across all remote fetches.
struct RemoteStats {
    fetches: Vec<FetchTiming>,
    archive_size: u64,
}

impl RemoteStats {
    fn new() -> Self {
        Self {
            fetches: Vec::new(),
            archive_size: 0,
        }
    }

    fn total_bytes(&self) -> u64 {
        self.fetches.iter().map(|f| f.bytes).sum()
    }

    fn total_requests(&self) -> usize {
        self.fetches.len()
    }

    fn print_timing(&self) {
        eprintln!("--- remote timing breakdown ---");
        for f in &self.fetches {
            eprintln!(
                "  {:20} : {:>7.1} ms  ({})",
                f.label,
                f.duration.as_secs_f64() * 1000.0,
                format_size(f.bytes)
            );
        }
        let total_dur: std::time::Duration = self.fetches.iter().map(|f| f.duration).sum();
        eprintln!(
            "  {:20} : {:>7.1} ms",
            "total",
            total_dur.as_secs_f64() * 1000.0
        );
    }

    fn print_summary(&self) {
        let downloaded = self.total_bytes();
        let pct = if self.archive_size > 0 {
            (downloaded as f64 / self.archive_size as f64) * 100.0
        } else {
            0.0
        };
        eprintln!(
            "Downloaded: {} of {} archive ({:.2}%)",
            format_size(downloaded),
            format_size(self.archive_size),
            pct
        );
        eprintln!("Requests: {}", self.total_requests());
    }
}

/// Fetch a byte range from a URL. Returns (body_bytes, content-length of full resource if available).
fn http_range_get(
    url: &str,
    range_start: u64,
    range_end: u64, // inclusive
) -> Result<(Vec<u8>, Option<u64>)> {
    let range_header = format!("bytes={}-{}", range_start, range_end);
    let resp = match ureq::get(url).set("Range", &range_header).call() {
        Ok(r) => r,
        Err(ureq::Error::Status(code, _resp)) => {
            bail!(
                "HTTP {} fetching range {}-{} from {}",
                code,
                range_start,
                range_end,
                url
            );
        }
        Err(e) => {
            return Err(anyhow::anyhow!("HTTP request failed for {}: {}", url, e));
        }
    };

    let status = resp.status();

    // Parse total archive size from Content-Range header: "bytes 0-39/123456"
    let total_size = resp
        .header("Content-Range")
        .and_then(|cr| cr.rsplit('/').next())
        .and_then(|s| s.parse::<u64>().ok());

    if status == 206 {
        // Partial content — expected
        let mut body = Vec::new();
        resp.into_reader().read_to_end(&mut body)?;
        Ok((body, total_size))
    } else if status == 200 {
        // Server doesn't support Range — we got the whole file
        eprintln!("WARNING: Server does not support Range requests. Downloading full archive.");
        let mut body = Vec::new();
        resp.into_reader().read_to_end(&mut body)?;
        let len = body.len() as u64;
        // Return only the requested range
        let start = range_start as usize;
        let end = (range_end as usize + 1).min(body.len());
        if start >= body.len() {
            bail!(
                "Requested range start {} beyond file size {}",
                start,
                body.len()
            );
        }
        Ok((body[start..end].to_vec(), Some(len)))
    } else {
        bail!(
            "HTTP {} fetching range {}-{} from {}",
            status,
            range_start,
            range_end,
            url
        );
    }
}

/// Parse footer from 40-byte slice. Returns (manifest_offset, index_offset).
fn parse_footer_bytes(footer: &[u8]) -> Result<(u64, u64)> {
    if footer.len() < FOOTER_SIZE {
        bail!("Footer too short: {} bytes", footer.len());
    }
    let magic = &footer[0..8];
    if magic != FOOTER_MAGIC {
        bail!("Invalid footer magic: {:?}", magic);
    }
    let manifest_offset = u64::from_le_bytes(footer[8..16].try_into()?);
    let _index_offset = u64::from_le_bytes(footer[16..24].try_into()?);
    Ok((manifest_offset, _index_offset))
}

/// Remote archive reader: fetches only footer, manifest, and requested blocks
/// via HTTP Range requests.
struct RemoteReader {
    url: String,
    manifest: LazyManifest,
    files_by_path: HashMap<String, usize>,
    chunks_by_id: Option<HashMap<u64, usize>>,
    blocks_by_id: Option<HashMap<u64, usize>>,
    stats: RemoteStats,
}

impl RemoteReader {
    /// Open a remote archive. Performs 2 HTTP requests: footer + manifest.
    fn open(url: &str) -> Result<Self> {
        let mut stats = RemoteStats::new();

        // --- Fetch footer (last 40 bytes) ---
        // We need to know the file size, so request a range from the end.
        // We'll request the last 40 bytes using a suffix range.
        let t_footer = Instant::now();
        let range_header = "bytes=-40";
        let resp = match ureq::get(url).set("Range", range_header).call() {
            Ok(r) => r,
            Err(ureq::Error::Status(code, _)) => {
                bail!("HTTP {} fetching footer from {}", code, url);
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "HTTP request for footer failed: {}: {}",
                    url,
                    e
                ));
            }
        };

        let status = resp.status();

        let archive_size = resp
            .header("Content-Range")
            .and_then(|cr| cr.rsplit('/').next())
            .and_then(|s| s.parse::<u64>().ok());

        let footer_bytes = if status == 206 {
            let mut body = Vec::new();
            resp.into_reader().read_to_end(&mut body)?;
            body
        } else if status == 200 {
            eprintln!("WARNING: Server does not support Range requests. Downloading full archive.");
            let mut body = Vec::new();
            resp.into_reader().read_to_end(&mut body)?;
            let len = body.len();
            if len < FOOTER_SIZE {
                bail!("Archive too small: {} bytes", len);
            }
            body[len - FOOTER_SIZE..].to_vec()
        } else {
            bail!("HTTP {} fetching footer from {}", status, url);
        };

        let footer_dur = t_footer.elapsed();
        stats.fetches.push(FetchTiming {
            label: "footer".to_string(),
            bytes: footer_bytes.len() as u64,
            duration: footer_dur,
        });

        let total_size = archive_size.unwrap_or(0);
        stats.archive_size = total_size;

        let (manifest_offset, _index_offset) = parse_footer_bytes(&footer_bytes)?;

        // --- Fetch manifest ---
        // Manifest starts at manifest_offset with a 4-byte length prefix.
        // First fetch the 4-byte length, then the manifest body.
        // Optimize: fetch a generous chunk starting at manifest_offset through
        // end-of-file minus footer, which covers both length prefix + manifest.
        let manifest_region_end = if total_size > FOOTER_SIZE as u64 {
            total_size - FOOTER_SIZE as u64 - 1
        } else {
            // Fallback: just fetch a large range
            manifest_offset + 10 * 1024 * 1024
        };

        let t_manifest = Instant::now();
        let (manifest_region, _) = http_range_get(url, manifest_offset, manifest_region_end)?;
        let manifest_dur = t_manifest.elapsed();

        if manifest_region.len() < 4 {
            bail!("Manifest region too short");
        }
        let manifest_len = u32::from_le_bytes(manifest_region[0..4].try_into()?) as usize;
        let manifest_start = 4;
        let manifest_end = manifest_start + manifest_len;
        if manifest_end > manifest_region.len() {
            bail!(
                "Manifest extends beyond fetched region ({} > {})",
                manifest_end,
                manifest_region.len()
            );
        }
        let manifest_raw = &manifest_region[manifest_start..manifest_end];

        stats.fetches.push(FetchTiming {
            label: "manifest".to_string(),
            bytes: manifest_region.len() as u64,
            duration: manifest_dur,
        });

        let manifest = deserialize_manifest_lazy(manifest_raw)?;

        let files_by_path: HashMap<String, usize> = manifest
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.path.clone(), i))
            .collect();

        Ok(RemoteReader {
            url: url.to_string(),
            manifest,
            files_by_path,
            chunks_by_id: None,
            blocks_by_id: None,
            stats,
        })
    }

    /// Ensure chunk and block tables are parsed and indexed.
    fn ensure_chunks_and_blocks(&mut self) -> Result<()> {
        if self.manifest.chunks.is_none() {
            let (chunks, _) = decode_chunks(&self.manifest.payload, self.manifest.chunks_offset)?;
            let idx: HashMap<u64, usize> = chunks
                .iter()
                .enumerate()
                .map(|(i, c)| (c.chunk_id, i))
                .collect();
            self.manifest.chunks = Some(chunks);
            self.chunks_by_id = Some(idx);
        }
        if self.manifest.blocks.is_none() {
            let (blocks, _) = decode_blocks(&self.manifest.payload, self.manifest.blocks_offset)?;
            let idx: HashMap<u64, usize> = blocks
                .iter()
                .enumerate()
                .map(|(i, b)| (b.block_id, i))
                .collect();
            self.manifest.blocks = Some(blocks);
            self.blocks_by_id = Some(idx);
        }
        Ok(())
    }

    /// Fetch and decompress a single block via HTTP range request.
    fn fetch_block(&mut self, block_id: u64) -> Result<Vec<u8>> {
        let blocks_idx = self.blocks_by_id.as_ref().expect("blocks not parsed");
        let block_idx = *blocks_idx
            .get(&block_id)
            .with_context(|| format!("Block {} not found", block_id))?;
        let blocks = self.manifest.blocks.as_ref().expect("blocks not parsed");
        let block = &blocks[block_idx];

        // Block layout: 4-byte LE length prefix, then payload bytes.
        // Fetch the 4-byte prefix + compressed_size bytes.
        let fetch_start = block.offset;
        let fetch_end = block.offset + 4 + block.compressed_size - 1;

        let t_block = Instant::now();
        let (raw, _) = http_range_get(&self.url, fetch_start, fetch_end)?;
        let block_dur = t_block.elapsed();

        self.stats.fetches.push(FetchTiming {
            label: format!("block {}", block_id),
            bytes: raw.len() as u64,
            duration: block_dur,
        });

        if raw.len() < 4 {
            bail!("Block {} fetch too short", block_id);
        }
        let stored_len = u32::from_le_bytes(raw[0..4].try_into()?) as usize;
        let payload = &raw[4..4 + stored_len];

        match block.codec.as_str() {
            "store" => Ok(payload.to_vec()),
            "zstd" => zstd::decode_all(payload).context("Failed to decompress block"),
            other => bail!("Unknown codec: {}", other),
        }
    }

    /// Extract a single file from the remote archive.
    #[allow(dead_code)]
    fn extract_file(&mut self, file_path: &str) -> Result<Vec<u8>> {
        self.ensure_chunks_and_blocks()?;

        let file_idx = *self
            .files_by_path
            .get(file_path)
            .with_context(|| format!("File not found in archive: {}", file_path))?;
        let entry = &self.manifest.files[file_idx];
        let chunk_refs = entry.chunk_refs.clone();
        let expected_sha = entry.sha256.clone();
        let expected_size = entry.size;

        let mut block_cache: HashMap<u64, Vec<u8>> = HashMap::new();
        let mut data_parts: Vec<u8> = Vec::with_capacity(expected_size as usize);

        // Collect chunk info and unique block IDs needed (scope borrows)
        let mut needed_block_ids: Vec<u64> = Vec::new();
        let mut chunk_info: Vec<(u64, u64, u64)> = Vec::new(); // (block_id, file_offset, size)
        {
            let chunks_by_id = self.chunks_by_id.as_ref().expect("chunks not parsed");
            let chunks = self.manifest.chunks.as_ref().expect("chunks not parsed");
            for &cid in &chunk_refs {
                let chunk_idx = chunks_by_id
                    .get(&cid)
                    .with_context(|| format!("Chunk {} not found", cid))?;
                let chunk = &chunks[*chunk_idx];
                chunk_info.push((chunk.block_id, chunk.file_offset, chunk.size));
                if !needed_block_ids.contains(&chunk.block_id) {
                    needed_block_ids.push(chunk.block_id);
                }
            }
        }

        // Fetch all needed blocks (no conflicting borrows now)
        for bid in &needed_block_ids {
            if !block_cache.contains_key(bid) {
                let block_data = self.fetch_block(*bid)?;
                block_cache.insert(*bid, block_data);
            }
        }

        // Reassemble file from chunks
        for &(block_id, file_offset, size) in &chunk_info {
            let block_data = &block_cache[&block_id];
            let start = file_offset as usize;
            let end = start + size as usize;
            if end > block_data.len() {
                bail!(
                    "Chunk slice {}..{} exceeds block {} size {}",
                    start,
                    end,
                    block_id,
                    block_data.len()
                );
            }
            data_parts.extend_from_slice(&block_data[start..end]);
        }

        // Verify SHA-256
        let mut hasher = Sha256::new();
        hasher.update(&data_parts);
        let computed = format!("{:x}", hasher.finalize());
        if computed != expected_sha {
            bail!(
                "Hash mismatch for {}: expected {}, got {}",
                file_path,
                expected_sha,
                computed
            );
        }

        Ok(data_parts)
    }

    /// Get the fully-parsed manifest (parses chunks + blocks if needed).
    #[allow(dead_code)]
    fn full_manifest(&mut self) -> Result<&LazyManifest> {
        self.ensure_chunks_and_blocks()?;
        Ok(&self.manifest)
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

fn cmd_get(archive: &str, file_path: &str, output: Option<PathBuf>, show_time: bool) -> Result<()> {
    if is_remote_url(archive) {
        return cmd_get_remote(archive, file_path, output, show_time);
    }
    let archive_path = Path::new(archive);
    let total_start = Instant::now();

    let t_open = Instant::now();
    let mut reader = FastArchiveReader::open(archive_path)?;
    let open_us = t_open.elapsed().as_micros();

    let (data, timings) = reader.extract_file_timed(file_path)?;

    let out_path =
        output.unwrap_or_else(|| PathBuf::from(file_path.rsplit('/').next().unwrap_or(file_path)));

    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let mut f = File::create(&out_path)?;
    f.write_all(&data)?;

    let total_wall = total_start.elapsed().as_micros();

    eprintln!(
        "Extracted {} -> {} ({})",
        file_path,
        out_path.display(),
        format_size(data.len() as u64)
    );

    if show_time {
        eprintln!("--- timing breakdown ---");
        eprintln!(
            "  open + file index : {:>7} us  (mmap + header + file entries + path HashMap)",
            open_us
        );
        eprintln!(
            "  chunk/block parse : {:>7} us  (lazy — 0 if already cached)",
            timings.manifest_parse_us
        );
        eprintln!(
            "  file lookup       : {:>7} us  (HashMap path -> index)",
            timings.file_lookup_us
        );
        eprintln!(
            "  block read (mmap) : {:>7} us  (direct offset seek via mmap)",
            timings.block_read_us
        );
        eprintln!(
            "  decompression     : {:>7} us  (zstd decode)",
            timings.decompress_us
        );
        eprintln!(
            "  extraction total  : {:>7} us  (lookup + read + decompress + sha256)",
            timings.total_us
        );
        eprintln!(
            "  wall total        : {:>7} us  (open through write)",
            total_wall
        );
        eprintln!(
            "  total             : {:>7.2} ms",
            total_wall as f64 / 1000.0
        );
    }

    Ok(())
}

fn cmd_get_remote(
    archive: &str,
    file_path: &str,
    output: Option<PathBuf>,
    show_time: bool,
) -> Result<()> {
    let url = resolve_url(archive)?;
    let mut reader = RemoteReader::open(&url)?;

    let data = reader.extract_file(file_path)?;

    let out_path =
        output.unwrap_or_else(|| PathBuf::from(file_path.rsplit('/').next().unwrap_or(file_path)));

    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let mut f = File::create(&out_path)?;
    f.write_all(&data)?;

    eprintln!(
        "Extracted {} -> {} ({})",
        file_path,
        out_path.display(),
        format_size(data.len() as u64)
    );

    if show_time {
        reader.stats.print_timing();
    }
    reader.stats.print_summary();

    Ok(())
}

fn cmd_list(archive: &str) -> Result<()> {
    if is_remote_url(archive) {
        return cmd_list_remote(archive);
    }
    let reader = ArchiveReader::open(Path::new(archive))?;

    // Find max size for alignment
    let max_size_width = reader
        .manifest
        .files
        .iter()
        .map(|f| format_size(f.size).len())
        .max()
        .unwrap_or(0);

    for entry in &reader.manifest.files {
        let size_str = format_size(entry.size);
        println!(
            "{:>width$}  {}",
            size_str,
            entry.path,
            width = max_size_width
        );
    }
    println!(
        "\n{} files, {} total",
        reader.manifest.files.len(),
        format_size(reader.manifest.files.iter().map(|f| f.size).sum())
    );
    Ok(())
}

fn cmd_list_remote(archive: &str) -> Result<()> {
    let url = resolve_url(archive)?;
    let reader = RemoteReader::open(&url)?;

    let max_size_width = reader
        .manifest
        .files
        .iter()
        .map(|f| format_size(f.size).len())
        .max()
        .unwrap_or(0);

    for entry in &reader.manifest.files {
        let size_str = format_size(entry.size);
        println!(
            "{:>width$}  {}",
            size_str,
            entry.path,
            width = max_size_width
        );
    }
    println!(
        "\n{} files, {} total",
        reader.manifest.files.len(),
        format_size(reader.manifest.files.iter().map(|f| f.size).sum())
    );
    reader.stats.print_summary();
    Ok(())
}

fn cmd_extract(archive: &str, output_dir: Option<PathBuf>) -> Result<()> {
    if is_remote_url(archive) {
        return cmd_extract_remote(archive, output_dir);
    }
    let reader = ArchiveReader::open(Path::new(archive))?;
    let out_dir = output_dir.unwrap_or_else(|| PathBuf::from("."));

    let total = reader.manifest.files.len();
    for (i, entry) in reader.manifest.files.iter().enumerate() {
        let data = reader.extract_file(&entry.path)?;
        let out_path = out_dir.join(&entry.path);
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut f = File::create(&out_path)?;
        f.write_all(&data)?;
        eprint!("\r[{}/{}] {}", i + 1, total, entry.path);
    }
    eprintln!("\nExtracted {} files to {}", total, out_dir.display());
    Ok(())
}

fn cmd_extract_remote(archive: &str, output_dir: Option<PathBuf>) -> Result<()> {
    let url = resolve_url(archive)?;
    let mut reader = RemoteReader::open(&url)?;

    let out_dir = output_dir.unwrap_or_else(|| PathBuf::from("."));
    let total = reader.manifest.files.len();
    let paths: Vec<String> = reader
        .manifest
        .files
        .iter()
        .map(|f| f.path.clone())
        .collect();

    for (i, path) in paths.iter().enumerate() {
        let data = reader.extract_file(path)?;
        let out_path = out_dir.join(path);
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut f = File::create(&out_path)?;
        f.write_all(&data)?;
        eprint!("\r[{}/{}] {}", i + 1, total, path);
    }
    eprintln!("\nExtracted {} files to {}", total, out_dir.display());
    reader.stats.print_summary();
    Ok(())
}

fn cmd_info(archive: &str) -> Result<()> {
    if is_remote_url(archive) {
        return cmd_info_remote(archive);
    }
    let reader = ArchiveReader::open(Path::new(archive))?;
    let total_size: u64 = reader.manifest.files.iter().map(|f| f.size).sum();
    let archive_size = reader.mmap.len() as u64;

    println!("Archive:    {}", archive);
    println!(
        "Version:    {}.{}",
        reader.header.version_major, reader.header.version_minor
    );
    println!("Files:      {}", reader.manifest.files.len());
    println!("Chunks:     {}", reader.manifest.chunks.len());
    println!("Blocks:     {}", reader.manifest.blocks.len());
    println!("Total size: {} (uncompressed)", format_size(total_size));
    println!("Archive:    {} (on disk)", format_size(archive_size));
    if total_size > 0 {
        println!(
            "Ratio:      {:.1}%",
            (archive_size as f64 / total_size as f64) * 100.0
        );
    }
    println!("Codec:      {}", reader.manifest.config.codec_default);
    println!(
        "Chunk size: {}",
        format_size(reader.manifest.config.chunk_size)
    );
    println!("Mode:       {}", reader.manifest.config.compression_mode);
    if reader.header.creation_timestamp > 0 {
        println!("Created:    {} (unix)", reader.header.creation_timestamp);
    }
    Ok(())
}

fn cmd_info_remote(archive: &str) -> Result<()> {
    let url = resolve_url(archive)?;
    let mut reader = RemoteReader::open(&url)?;
    reader.ensure_chunks_and_blocks()?;

    let total_size: u64 = reader.manifest.files.iter().map(|f| f.size).sum();

    println!("Archive:    {} (remote)", archive);
    println!("URL:        {}", url);
    println!("Files:      {}", reader.manifest.files.len());
    let chunks = reader.manifest.chunks.as_ref().unwrap();
    let blocks = reader.manifest.blocks.as_ref().unwrap();
    println!("Chunks:     {}", chunks.len());
    println!("Blocks:     {}", blocks.len());
    println!("Total size: {} (uncompressed)", format_size(total_size));
    if reader.stats.archive_size > 0 {
        println!(
            "Archive:    {} (remote)",
            format_size(reader.stats.archive_size)
        );
        if total_size > 0 {
            println!(
                "Ratio:      {:.1}%",
                (reader.stats.archive_size as f64 / total_size as f64) * 100.0
            );
        }
    }
    println!("Codec:      {}", reader.manifest.config.codec_default);
    println!(
        "Chunk size: {}",
        format_size(reader.manifest.config.chunk_size)
    );
    println!("Mode:       {}", reader.manifest.config.compression_mode);
    reader.stats.print_summary();
    Ok(())
}

// ---------------------------------------------------------------------------
// Pack: constants and config
// ---------------------------------------------------------------------------

const CHUNK_SIZE: usize = 1024 * 1024; // 1 MB
const SMALL_FILE_THRESHOLD: usize = 64 * 1024; // 64 KB
const ZSTD_LEVEL: i32 = 3;
const VERSION_MAJOR: u32 = 0;
const VERSION_MINOR: u32 = 1;

const INCOMPRESSIBLE_EXTENSIONS: &[&str] = &[
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".mp4", ".mkv", ".avi", ".mov", ".webm",
    ".mp3", ".aac", ".ogg", ".flac", ".opus", ".zip", ".gz", ".bz2", ".xz", ".zst", ".7z", ".rar",
    ".woff", ".woff2", ".br",
];

// ---------------------------------------------------------------------------
// Pack: varint encoding
// ---------------------------------------------------------------------------

fn encode_varint(value: u64) -> Vec<u8> {
    let mut v = value;
    let mut out = Vec::with_capacity(10);
    loop {
        if v <= 0x7F {
            out.push(v as u8);
            break;
        }
        out.push(((v & 0x7F) | 0x80) as u8);
        v >>= 7;
    }
    out
}

fn encode_signed_varint(value: i64) -> Vec<u8> {
    let zigzag: u64 = if value >= 0 {
        (value as u64) << 1
    } else {
        (((-value) as u64) << 1) - 1
    };
    encode_varint(zigzag)
}

// ---------------------------------------------------------------------------
// Pack: CRC64 (compatibility — Python uses CRC32 zero-extended to 16 hex chars)
// ---------------------------------------------------------------------------

fn crc64_of(data: &[u8]) -> u64 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize() as u64
}

fn crc64_hex_of(data: &[u8]) -> String {
    format!("{:016x}", crc64_of(data))
}

// ---------------------------------------------------------------------------
// Pack: file classification
// ---------------------------------------------------------------------------

fn classify_file(path: &str, size: u64) -> &'static str {
    if size <= SMALL_FILE_THRESHOLD as u64 {
        return "tiny";
    }
    if let Some(dot_pos) = path.rfind('.') {
        let ext = &path[dot_pos..];
        let ext_lower = ext.to_ascii_lowercase();
        for &incomp in INCOMPRESSIBLE_EXTENSIONS {
            if ext_lower == incomp {
                return "incompressible";
            }
        }
    }
    "compressible"
}

// ---------------------------------------------------------------------------
// Pack: compression
// ---------------------------------------------------------------------------

fn compress_block(data: &[u8], codec: &str) -> Result<(Vec<u8>, String)> {
    if codec == "store" {
        return Ok((data.to_vec(), "store".to_string()));
    }
    let compressed = zstd::encode_all(data, ZSTD_LEVEL)?;
    if compressed.len() >= data.len() {
        Ok((data.to_vec(), "store".to_string()))
    } else {
        Ok((compressed, "zstd".to_string()))
    }
}

// ---------------------------------------------------------------------------
// Pack: manifest serialization (must match Python byte-for-byte)
// ---------------------------------------------------------------------------

struct PackFileEntry {
    file_id: u64,
    path: String,
    size: u64,
    mode: u64,
    mtime_ns: u64,
    sha256: String, // hex
    chunk_refs: Vec<u64>,
    codec_hint: Option<String>,
    pack_id: Option<i64>,
}

struct PackChunkRecord {
    chunk_id: u64,
    file_id: u64,
    file_offset: u64,
    uncompressed_size: u64,
    block_id: u64,
}

struct PackBlockRecord {
    block_id: u64,
    offset: u64,
    codec: String,
    compressed_size: u64,
    uncompressed_size: u64,
    checksum: String, // 16-char hex
}

fn build_string_table(paths: &[String]) -> (Vec<String>, Vec<(usize, usize)>) {
    let mut string_to_idx: HashMap<String, usize> = HashMap::new();
    let mut strings: Vec<String> = Vec::new();
    let mut path_refs: Vec<(usize, usize)> = Vec::new();

    for path in paths {
        let (dir_part, name_part) = match path.rfind('/') {
            Some(pos) => (path[..pos].to_string(), path[pos + 1..].to_string()),
            None => (String::new(), path.clone()),
        };

        let dir_idx = if let Some(&idx) = string_to_idx.get(&dir_part) {
            idx
        } else {
            let idx = strings.len();
            string_to_idx.insert(dir_part.clone(), idx);
            strings.push(dir_part);
            idx
        };

        let name_idx = if let Some(&idx) = string_to_idx.get(&name_part) {
            idx
        } else {
            let idx = strings.len();
            string_to_idx.insert(name_part.clone(), idx);
            strings.push(name_part);
            idx
        };

        path_refs.push((dir_idx, name_idx));
    }

    (strings, path_refs)
}

fn encode_string_table(strings: &[String]) -> Vec<u8> {
    let mut out = encode_varint(strings.len() as u64);
    for s in strings {
        let bytes = s.as_bytes();
        out.extend(encode_varint(bytes.len() as u64));
        out.extend(bytes);
    }
    out
}

fn encode_config_section() -> Vec<u8> {
    let mut parts = Vec::new();
    parts.extend(encode_varint(CHUNK_SIZE as u64));
    parts.extend(encode_varint(SMALL_FILE_THRESHOLD as u64));
    parts.push(1); // codec: 1 = zstd
    parts.extend(encode_varint(ZSTD_LEVEL as u64));
    parts.push(1); // mode: 1 = balanced
    parts
}

fn encode_files(files: &[PackFileEntry], path_refs: &[(usize, usize)]) -> Vec<u8> {
    let codec_hint_byte = |hint: &Option<String>| -> u8 {
        match hint.as_deref() {
            None => 0,
            Some("store") => 1,
            Some("zstd") => 2,
            _ => 0,
        }
    };

    let mut out = encode_varint(files.len() as u64);

    for (i, f) in files.iter().enumerate() {
        let (dir_idx, name_idx) = path_refs[i];

        out.extend(encode_varint(f.file_id));
        out.extend(encode_varint(dir_idx as u64));
        out.extend(encode_varint(name_idx as u64));
        out.extend(encode_varint(f.size));
        out.extend(encode_varint(f.mode));
        out.extend(encode_varint(f.mtime_ns));

        // type: 0 = file
        out.push(0);

        // sha256: 32 raw bytes
        let sha_bytes = hex::decode(&f.sha256).expect("invalid sha256 hex");
        out.extend(&sha_bytes);

        // chunk_refs: delta-encoded
        out.extend(encode_varint(f.chunk_refs.len() as u64));
        let mut prev: u64 = 0;
        for &cref in &f.chunk_refs {
            out.extend(encode_varint(cref - prev));
            prev = cref;
        }

        // codec_hint
        out.push(codec_hint_byte(&f.codec_hint));

        // pack_id
        let pack_id = f.pack_id.unwrap_or(-1);
        out.extend(encode_signed_varint(pack_id));
    }

    out
}

fn encode_chunks(chunks: &[PackChunkRecord]) -> Vec<u8> {
    let mut sorted: Vec<&PackChunkRecord> = chunks.iter().collect();
    sorted.sort_by_key(|c| c.chunk_id);

    let mut out = encode_varint(sorted.len() as u64);
    let mut prev_chunk_id: u64 = 0;

    for c in &sorted {
        out.extend(encode_varint(c.chunk_id - prev_chunk_id));
        prev_chunk_id = c.chunk_id;
        out.extend(encode_varint(c.file_id));
        out.extend(encode_varint(c.file_offset));
        out.extend(encode_varint(c.uncompressed_size));
        out.extend(encode_varint(c.block_id));
    }

    out
}

fn encode_blocks(blocks: &[PackBlockRecord]) -> Vec<u8> {
    let mut sorted: Vec<&PackBlockRecord> = blocks.iter().collect();
    sorted.sort_by_key(|b| b.block_id);

    let mut out = encode_varint(sorted.len() as u64);
    let mut prev_block_id: u64 = 0;
    let mut prev_offset: u64 = 0;

    for b in &sorted {
        out.extend(encode_varint(b.block_id - prev_block_id));
        prev_block_id = b.block_id;

        // codec byte
        let codec_byte: u8 = if b.codec == "zstd" { 1 } else { 0 };
        out.push(codec_byte);

        out.extend(encode_varint(b.offset - prev_offset));
        prev_offset = b.offset;

        out.extend(encode_varint(b.compressed_size));
        out.extend(encode_varint(b.uncompressed_size));

        // checksum: 8 raw bytes from hex
        let cksum_bytes = hex::decode(&b.checksum).expect("invalid checksum hex");
        out.extend(&cksum_bytes);
    }

    out
}

fn serialize_manifest(
    files: &[PackFileEntry],
    chunks: &[PackChunkRecord],
    blocks: &[PackBlockRecord],
) -> Vec<u8> {
    let paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    let (strings, path_refs) = build_string_table(&paths);

    let config_bytes = encode_config_section();
    let string_table_bytes = encode_string_table(&strings);
    let files_bytes = encode_files(files, &path_refs);
    let chunks_bytes = encode_chunks(chunks);
    let blocks_bytes = encode_blocks(blocks);

    // Assemble raw manifest
    let mut raw = Vec::new();
    raw.extend(MANIFEST_MAGIC);
    raw.push(1); // format version
    raw.extend(encode_varint(config_bytes.len() as u64));
    raw.extend(&config_bytes);
    raw.extend(&string_table_bytes);
    raw.extend(&files_bytes);
    raw.extend(&chunks_bytes);
    raw.extend(&blocks_bytes);

    // Compress with zstd level 9 (matching Python)
    let compressed = zstd::encode_all(raw.as_slice(), 9).expect("zstd compress manifest");

    if compressed.len() < raw.len() {
        let mut envelope = Vec::with_capacity(1 + compressed.len());
        envelope.push(0x01); // flag: zstd compressed
        envelope.extend(&compressed);
        envelope
    } else {
        let mut envelope = Vec::with_capacity(1 + raw.len());
        envelope.push(0x00); // flag: uncompressed
        envelope.extend(&raw);
        envelope
    }
}

// ---------------------------------------------------------------------------
// Pack: header and footer writing
// ---------------------------------------------------------------------------

fn write_pack_header<W: Write>(
    w: &mut W,
    manifest_offset: u64,
    data_offset: u64,
    index_offset: u64,
) -> Result<()> {
    let creation_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let creator_ver: u64 = ((VERSION_MAJOR as u64) << 16) | (VERSION_MINOR as u64);
    let reserved: u64 = 0;
    let flags: u64 = 0;

    let mut header_data = Vec::with_capacity(HEADER_SIZE);
    header_data.extend(MAGIC);
    header_data.extend(&VERSION_MAJOR.to_le_bytes());
    header_data.extend(&VERSION_MINOR.to_le_bytes());
    header_data.extend(&manifest_offset.to_le_bytes());
    header_data.extend(&data_offset.to_le_bytes());
    header_data.extend(&index_offset.to_le_bytes());
    header_data.extend(&flags.to_le_bytes());
    header_data.extend(&creation_ts.to_le_bytes());
    header_data.extend(&creator_ver.to_le_bytes());
    header_data.extend(&reserved.to_le_bytes());

    // CRC64 of the header so far (first 72 bytes)
    let crc = crc64_of(&header_data);
    header_data.extend(&crc.to_le_bytes());

    assert_eq!(header_data.len(), HEADER_SIZE);
    w.write_all(&header_data)?;
    Ok(())
}

fn write_pack_footer<W: Write>(w: &mut W, manifest_offset: u64, index_offset: u64) -> Result<()> {
    let flags: u64 = 0;
    let checksum: u64 = 0;

    w.write_all(FOOTER_MAGIC)?;
    w.write_all(&manifest_offset.to_le_bytes())?;
    w.write_all(&index_offset.to_le_bytes())?;
    w.write_all(&flags.to_le_bytes())?;
    w.write_all(&checksum.to_le_bytes())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Pack: main pack command
// ---------------------------------------------------------------------------

struct InputEntry {
    full_path: PathBuf,
    rel_path: String,
    size: u64,
}

fn walk_input(input_dir: &Path) -> Result<Vec<InputEntry>> {
    let mut entries = Vec::new();
    for entry in walkdir::WalkDir::new(input_dir).sort_by_file_name() {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let full_path = entry.path().to_path_buf();
        let rel = full_path
            .strip_prefix(input_dir)
            .context("strip prefix")?
            .to_string_lossy()
            .replace('\\', "/");
        let size = entry.metadata()?.len();
        entries.push(InputEntry {
            full_path,
            rel_path: rel,
            size,
        });
    }
    Ok(entries)
}

fn cmd_pack(input_dir: &Path, output_path: &Path) -> Result<()> {
    // Step 1: Walk
    let raw_entries = walk_input(input_dir)?;
    eprintln!("[arcx] Found {} files", raw_entries.len());

    // Step 2: Classify
    let mut tiny_files: Vec<&InputEntry> = Vec::new();
    let mut large_files: Vec<(&InputEntry, &'static str)> = Vec::new();

    for entry in &raw_entries {
        let class = classify_file(&entry.rel_path, entry.size);
        if class == "tiny" {
            tiny_files.push(entry);
        } else {
            large_files.push((entry, class));
        }
    }
    eprintln!(
        "[arcx] {} tiny, {} large/incompressible",
        tiny_files.len(),
        large_files.len()
    );

    // Build manifest structures
    let mut files: Vec<PackFileEntry> = Vec::new();
    let mut chunks: Vec<PackChunkRecord> = Vec::new();
    let mut blocks: Vec<PackBlockRecord> = Vec::new();
    let mut block_payloads: Vec<Vec<u8>> = Vec::new();

    let mut file_id: u64 = 0;
    let mut chunk_id: u64 = 0;
    let mut block_id: u64 = 0;

    // Step 3a: Pack tiny files into shared blocks
    {
        let mut pack_buffer: Vec<u8> = Vec::new();
        let mut pack_chunks_indices: Vec<usize> = Vec::new(); // indices into `chunks`

        for (ti, entry) in tiny_files.iter().enumerate() {
            let data = fs::read(&entry.full_path)
                .with_context(|| format!("reading {}", entry.rel_path))?;

            let mut hasher = Sha256::new();
            hasher.update(&data);
            let file_hash = format!("{:x}", hasher.finalize());

            files.push(PackFileEntry {
                file_id,
                path: entry.rel_path.clone(),
                size: entry.size,
                mode: 0o644,
                mtime_ns: 0,
                sha256: file_hash,
                chunk_refs: vec![chunk_id],
                codec_hint: None,
                pack_id: Some(0),
            });

            let chunk_idx = chunks.len();
            chunks.push(PackChunkRecord {
                chunk_id,
                file_id,
                file_offset: pack_buffer.len() as u64,
                uncompressed_size: data.len() as u64,
                block_id: u64::MAX, // placeholder
            });
            pack_chunks_indices.push(chunk_idx);
            pack_buffer.extend(&data);

            file_id += 1;
            chunk_id += 1;

            // Flush when pack reaches chunk_size
            if pack_buffer.len() >= CHUNK_SIZE {
                let (compressed, codec_used) = compress_block(&pack_buffer, "zstd")?;
                let checksum = crc64_hex_of(&compressed);

                blocks.push(PackBlockRecord {
                    block_id,
                    offset: 0, // set during write
                    codec: codec_used,
                    compressed_size: compressed.len() as u64,
                    uncompressed_size: pack_buffer.len() as u64,
                    checksum,
                });
                block_payloads.push(compressed);

                // Update chunk block refs
                for &ci in &pack_chunks_indices {
                    chunks[ci].block_id = block_id;
                }

                block_id += 1;
                pack_buffer.clear();
                pack_chunks_indices.clear();
            }

            if (ti + 1) % 100 == 0 {
                eprint!("\r[arcx] Packed {}/{} tiny files", ti + 1, tiny_files.len());
            }
        }

        // Flush remaining tiny files
        if !pack_buffer.is_empty() {
            let (compressed, codec_used) = compress_block(&pack_buffer, "zstd")?;
            let checksum = crc64_hex_of(&compressed);

            blocks.push(PackBlockRecord {
                block_id,
                offset: 0,
                codec: codec_used,
                compressed_size: compressed.len() as u64,
                uncompressed_size: pack_buffer.len() as u64,
                checksum,
            });
            block_payloads.push(compressed);

            for &ci in &pack_chunks_indices {
                chunks[ci].block_id = block_id;
            }

            block_id += 1;
        }

        if !tiny_files.is_empty() {
            eprintln!("\r[arcx] Packed {} tiny files", tiny_files.len());
        }
    }

    // Step 3b: Chunk and compress large files
    for (li, (entry, classification)) in large_files.iter().enumerate() {
        let data =
            fs::read(&entry.full_path).with_context(|| format!("reading {}", entry.rel_path))?;

        let mut hasher = Sha256::new();
        hasher.update(&data);
        let file_hash = format!("{:x}", hasher.finalize());

        let codec = if *classification == "incompressible" {
            "store"
        } else {
            "zstd"
        };

        // Chunk the file
        let mut file_chunk_ids: Vec<u64> = Vec::new();
        let mut offset: usize = 0;

        while offset < data.len() {
            let end = std::cmp::min(offset + CHUNK_SIZE, data.len());
            let chunk_data = &data[offset..end];

            let (compressed, codec_used) = compress_block(chunk_data, codec)?;
            let checksum = crc64_hex_of(&compressed);

            chunks.push(PackChunkRecord {
                chunk_id,
                file_id,
                file_offset: offset as u64,
                uncompressed_size: chunk_data.len() as u64,
                block_id,
            });

            blocks.push(PackBlockRecord {
                block_id,
                offset: 0,
                codec: codec_used,
                compressed_size: compressed.len() as u64,
                uncompressed_size: chunk_data.len() as u64,
                checksum,
            });
            block_payloads.push(compressed);

            file_chunk_ids.push(chunk_id);
            chunk_id += 1;
            block_id += 1;
            offset = end;
        }

        files.push(PackFileEntry {
            file_id,
            path: entry.rel_path.clone(),
            size: entry.size,
            mode: 0o644,
            mtime_ns: 0,
            sha256: file_hash,
            chunk_refs: file_chunk_ids,
            codec_hint: Some(codec.to_string()),
            pack_id: None,
        });

        file_id += 1;

        if (li + 1) % 10 == 0 || li + 1 == large_files.len() {
            eprint!(
                "\r[arcx] Compressed {}/{} large files",
                li + 1,
                large_files.len()
            );
        }
    }
    if !large_files.is_empty() {
        eprintln!();
    }

    // Step 4: Write archive
    let out_file =
        File::create(output_path).with_context(|| format!("creating {}", output_path.display()))?;
    let mut w = BufWriter::new(out_file);

    // Placeholder header (80 zero bytes)
    w.write_all(&[0u8; HEADER_SIZE])?;

    // Data blocks — write to capture offsets
    let data_offset = HEADER_SIZE as u64;
    for (i, payload) in block_payloads.iter().enumerate() {
        let current_pos = data_offset
            + block_payloads[..i]
                .iter()
                .map(|p| 4 + p.len() as u64)
                .sum::<u64>();
        blocks[i].offset = current_pos;

        let len_bytes = (payload.len() as u32).to_le_bytes();
        w.write_all(&len_bytes)?;
        w.write_all(payload)?;
    }

    // Serialize manifest with correct block offsets
    let manifest_bytes = serialize_manifest(&files, &chunks, &blocks);

    // Calculate manifest offset
    let manifest_offset = data_offset
        + block_payloads
            .iter()
            .map(|p| 4 + p.len() as u64)
            .sum::<u64>();

    // Write manifest with 4-byte length prefix
    w.write_all(&(manifest_bytes.len() as u32).to_le_bytes())?;
    w.write_all(&manifest_bytes)?;

    // Index (duplicate of manifest for MVP)
    let index_offset = manifest_offset + 4 + manifest_bytes.len() as u64;
    w.write_all(&(manifest_bytes.len() as u32).to_le_bytes())?;
    w.write_all(&manifest_bytes)?;

    // Footer
    write_pack_footer(&mut w, manifest_offset, index_offset)?;

    // Flush before seeking
    w.flush()?;

    // Rewrite header with real offsets
    w.seek(SeekFrom::Start(0))?;
    write_pack_header(&mut w, manifest_offset, data_offset, index_offset)?;
    w.flush()?;
    drop(w);

    // Stats
    let archive_size = fs::metadata(output_path)?.len();
    let total_input: u64 = raw_entries.iter().map(|e| e.size).sum();
    let ratio = if total_input > 0 {
        archive_size as f64 / total_input as f64
    } else {
        0.0
    };

    eprintln!(
        "[arcx] Packed {} files ({} bytes) -> {} bytes ({:.1}%)",
        files.len(),
        total_input,
        archive_size,
        ratio * 100.0
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Get {
            archive,
            file_path,
            output,
            time,
        } => cmd_get(&archive, &file_path, output, time),
        Commands::List { archive } => cmd_list(&archive),
        Commands::Extract {
            archive,
            output_dir,
        } => cmd_extract(&archive, output_dir),
        Commands::Info { archive } => cmd_info(&archive),
        Commands::Pack { input_dir, output } => cmd_pack(&input_dir, &output),
        #[cfg(feature = "fuse")]
        Commands::Mount {
            archive,
            mountpoint,
            cache_size,
        } => fuse_mount::cmd_mount(&archive, &mountpoint, cache_size),
    }
}
