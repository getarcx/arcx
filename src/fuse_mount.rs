//! FUSE filesystem for mounting ARCX archives as read-only filesystems.
//!
//! Activated via `--features fuse`. Uses the `fuser` crate (pure-Rust FUSE).

use crate::ArchiveReader;
use anyhow::{Context, Result};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, Request,
};
use libc::{EISDIR, ENOENT, ENOTDIR};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, UNIX_EPOCH};

const TTL: Duration = Duration::from_secs(3600); // attrs don't change

// ---------------------------------------------------------------------------
// Inode table
// ---------------------------------------------------------------------------

/// Represents either a directory or a file in the inode table.
enum InodeEntry {
    Dir {
        /// Child name -> inode
        children: HashMap<String, u64>,
    },
    File {
        /// Index into ArchiveReader.manifest.files
        file_idx: usize,
    },
}

struct InodeTable {
    /// inode -> entry (inode 1 = root)
    entries: HashMap<u64, InodeEntry>,
    /// inode -> FileAttr
    attrs: HashMap<u64, FileAttr>,
    next_ino: u64,
}

impl InodeTable {
    fn build(reader: &ArchiveReader) -> Self {
        let mut table = InodeTable {
            entries: HashMap::new(),
            attrs: HashMap::new(),
            next_ino: 2, // 1 is reserved for root
        };

        // Create root directory (inode 1)
        let root_attr = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o555,
            nlink: 2,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        };
        table.entries.insert(
            1,
            InodeEntry::Dir {
                children: HashMap::new(),
            },
        );
        table.attrs.insert(1, root_attr);

        // Process all files from the manifest
        for (file_idx, entry) in reader.manifest.files.iter().enumerate() {
            let path = &entry.path;
            let components: Vec<&str> = path.split('/').collect();

            // Ensure all parent directories exist
            let mut parent_ino: u64 = 1;
            for i in 0..components.len() - 1 {
                let dir_name = components[i];
                let existing_child =
                    if let Some(InodeEntry::Dir { children }) = table.entries.get(&parent_ino) {
                        children.get(dir_name).copied()
                    } else {
                        None
                    };

                if let Some(child_ino) = existing_child {
                    parent_ino = child_ino;
                } else {
                    // Create new directory inode
                    let new_ino = table.next_ino;
                    table.next_ino += 1;

                    let dir_attr = FileAttr {
                        ino: new_ino,
                        size: 0,
                        blocks: 0,
                        atime: UNIX_EPOCH,
                        mtime: UNIX_EPOCH,
                        ctime: UNIX_EPOCH,
                        crtime: UNIX_EPOCH,
                        kind: FileType::Directory,
                        perm: 0o555,
                        nlink: 2,
                        uid: unsafe { libc::getuid() },
                        gid: unsafe { libc::getgid() },
                        rdev: 0,
                        blksize: 512,
                        flags: 0,
                    };
                    table.entries.insert(
                        new_ino,
                        InodeEntry::Dir {
                            children: HashMap::new(),
                        },
                    );
                    table.attrs.insert(new_ino, dir_attr);

                    // Add to parent
                    if let Some(InodeEntry::Dir { children }) = table.entries.get_mut(&parent_ino) {
                        children.insert(dir_name.to_string(), new_ino);
                    }

                    parent_ino = new_ino;
                }
            }

            // Create the file inode
            let file_ino = table.next_ino;
            table.next_ino += 1;

            let file_name = components.last().unwrap();

            // Compute mtime from nanoseconds
            let mtime = if entry.mtime_ns > 0 {
                UNIX_EPOCH + Duration::from_nanos(entry.mtime_ns)
            } else {
                UNIX_EPOCH
            };

            // Use file mode from manifest, default to 0o444 if zero
            let perm = if entry.mode > 0 {
                (entry.mode & 0o777) as u16
            } else {
                0o444
            };

            let file_blocks = (entry.size + 511) / 512;
            let file_attr = FileAttr {
                ino: file_ino,
                size: entry.size,
                blocks: file_blocks,
                atime: mtime,
                mtime,
                ctime: mtime,
                crtime: mtime,
                kind: FileType::RegularFile,
                perm,
                nlink: 1,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                rdev: 0,
                blksize: 512,
                flags: 0,
            };

            table
                .entries
                .insert(file_ino, InodeEntry::File { file_idx });
            table.attrs.insert(file_ino, file_attr);

            // Add file to parent directory
            if let Some(InodeEntry::Dir { children }) = table.entries.get_mut(&parent_ino) {
                children.insert(file_name.to_string(), file_ino);
            }
        }

        table
    }
}

// ---------------------------------------------------------------------------
// LRU block cache
// ---------------------------------------------------------------------------

struct BlockCache {
    /// block_id -> decompressed data
    data: HashMap<u64, Vec<u8>>,
    /// Access order: most recently used at the end
    order: Vec<u64>,
    max_size: usize,
}

impl BlockCache {
    fn new(max_size: usize) -> Self {
        BlockCache {
            data: HashMap::new(),
            order: Vec::new(),
            max_size,
        }
    }

    fn get(&mut self, block_id: u64) -> Option<&Vec<u8>> {
        if self.data.contains_key(&block_id) {
            // Move to end (most recently used)
            self.order.retain(|&id| id != block_id);
            self.order.push(block_id);
            self.data.get(&block_id)
        } else {
            None
        }
    }

    fn insert(&mut self, block_id: u64, data: Vec<u8>) {
        // Evict if at capacity
        while self.data.len() >= self.max_size && !self.order.is_empty() {
            let evict_id = self.order.remove(0);
            self.data.remove(&evict_id);
        }
        self.order.push(block_id);
        self.data.insert(block_id, data);
    }
}

// ---------------------------------------------------------------------------
// FUSE filesystem
// ---------------------------------------------------------------------------

struct ArcxFs {
    reader: ArchiveReader,
    inodes: InodeTable,
    block_cache: Mutex<BlockCache>,
    next_fh: Mutex<u64>,
}

impl ArcxFs {
    fn new(reader: ArchiveReader, cache_size: usize) -> Self {
        let inodes = InodeTable::build(&reader);
        ArcxFs {
            reader,
            inodes,
            block_cache: Mutex::new(BlockCache::new(cache_size)),
            next_fh: Mutex::new(1),
        }
    }

    /// Read bytes from a file entry at the given offset and size.
    /// Decompresses only the needed blocks on demand.
    fn read_file_range(&self, file_idx: usize, offset: u64, size: u32) -> Result<Vec<u8>, i32> {
        let entry = &self.reader.manifest.files[file_idx];

        if offset >= entry.size {
            return Ok(Vec::new());
        }

        let read_end = std::cmp::min(offset + size as u64, entry.size);
        let mut result = Vec::with_capacity((read_end - offset) as usize);

        // Walk through chunks in order and figure out which contribute to
        // the requested byte range [offset, read_end).
        let mut file_pos: u64 = 0;
        for &cid in &entry.chunk_refs {
            let chunk_idx = match self.reader.chunks_by_id.get(&cid) {
                Some(&idx) => idx,
                None => return Err(libc::EIO),
            };
            let chunk = &self.reader.manifest.chunks[chunk_idx];
            let chunk_end = file_pos + chunk.size;

            // Does this chunk overlap [offset, read_end)?
            if chunk_end > offset && file_pos < read_end {
                let block_data = self.get_block(chunk.block_id)?;

                // Byte range within the decompressed block
                let block_start = chunk.file_offset as usize;
                let block_slice = &block_data[block_start..block_start + chunk.size as usize];

                // Map the requested file range into this chunk's slice
                let slice_start = if offset > file_pos {
                    (offset - file_pos) as usize
                } else {
                    0
                };
                let slice_end = if read_end < chunk_end {
                    (read_end - file_pos) as usize
                } else {
                    chunk.size as usize
                };

                result.extend_from_slice(&block_slice[slice_start..slice_end]);
            }

            file_pos = chunk_end;
            if file_pos >= read_end {
                break;
            }
        }

        Ok(result)
    }

    /// Get a decompressed block, using the cache.
    fn get_block(&self, block_id: u64) -> Result<Vec<u8>, i32> {
        {
            let mut cache = self.block_cache.lock().unwrap();
            if let Some(data) = cache.get(block_id) {
                return Ok(data.clone());
            }
        }

        // Decompress the block
        let data = self.reader.read_block(block_id).map_err(|_| libc::EIO)?;

        let mut cache = self.block_cache.lock().unwrap();
        cache.insert(block_id, data.clone());
        Ok(data)
    }
}

impl Filesystem for ArcxFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if let Some(InodeEntry::Dir { children }) = self.inodes.entries.get(&parent) {
            if let Some(&child_ino) = children.get(name_str) {
                if let Some(attr) = self.inodes.attrs.get(&child_ino) {
                    reply.entry(&TTL, attr, 0);
                    return;
                }
            }
        }

        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if let Some(attr) = self.inodes.attrs.get(&ino) {
            reply.attr(&TTL, attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let children = match self.inodes.entries.get(&ino) {
            Some(InodeEntry::Dir { children }) => children,
            _ => {
                reply.error(ENOTDIR);
                return;
            }
        };

        // Build sorted entries: ".", "..", then children
        let mut entries: Vec<(u64, FileType, String)> = Vec::new();
        entries.push((ino, FileType::Directory, ".".to_string()));
        entries.push((ino, FileType::Directory, "..".to_string()));

        let mut child_list: Vec<(&String, &u64)> = children.iter().collect();
        child_list.sort_by_key(|(name, _)| name.to_string());

        for (name, &child_ino) in child_list {
            let kind = match self.inodes.entries.get(&child_ino) {
                Some(InodeEntry::Dir { .. }) => FileType::Directory,
                Some(InodeEntry::File { .. }) => FileType::RegularFile,
                None => continue,
            };
            entries.push((child_ino, kind, name.clone()));
        }

        for (i, (ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            // reply.add returns true if the buffer is full
            if reply.add(*ino, (i + 1) as i64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.inodes.entries.get(&ino) {
            Some(InodeEntry::File { .. }) => {
                let mut fh = self.next_fh.lock().unwrap();
                let handle = *fh;
                *fh += 1;
                // Read-only, direct_io=false (we handle caching ourselves)
                reply.opened(handle, 0);
            }
            Some(InodeEntry::Dir { .. }) => {
                reply.error(EISDIR);
            }
            None => {
                reply.error(ENOENT);
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let file_idx = match self.inodes.entries.get(&ino) {
            Some(InodeEntry::File { file_idx }) => *file_idx,
            _ => {
                reply.error(ENOENT);
                return;
            }
        };

        match self.read_file_range(file_idx, offset as u64, size) {
            Ok(data) => reply.data(&data),
            Err(errno) => reply.error(errno),
        }
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }
}

// ---------------------------------------------------------------------------
// Mount command entry point
// ---------------------------------------------------------------------------

pub fn cmd_mount(archive: &str, mountpoint: &Path, cache_size: usize) -> Result<()> {
    let reader = ArchiveReader::open(Path::new(archive))
        .with_context(|| format!("Failed to open archive: {}", archive))?;

    let file_count = reader.manifest.files.len();
    let block_count = reader.manifest.blocks.len();

    let fs = ArcxFs::new(reader, cache_size);

    eprintln!(
        "Mounted {} at {} ({} files, {} blocks, cache: {} blocks) — Ctrl+C to unmount",
        archive,
        mountpoint.display(),
        file_count,
        block_count,
        cache_size,
    );

    let options = vec![
        MountOption::RO,
        MountOption::FSName(format!("arcx:{}", archive)),
        MountOption::Subtype("arcx".to_string()),
    ];

    // This blocks until the filesystem is unmounted (Ctrl+C / fusermount -u)
    fuser::mount2(fs, mountpoint, &options)
        .with_context(|| format!("Failed to mount at {}", mountpoint.display()))?;

    eprintln!("Unmounted {}", mountpoint.display());
    Ok(())
}
