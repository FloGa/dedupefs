use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufReader, Read};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Receiver;
use std::time::SystemTime;

use crazy_deduper::Hydrator;
use file_declutter::FileDeclutter;
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, Request,
};
use libc::{EIO, EISDIR, ENOENT, ENOTDIR};
use log::{debug, error, info};
use lru::LruCache;

use crate::{
    ATTRS_DEFAULT, DirEntryAddArgs, DropHookFn, HandlePool, INO_ROOT, MetaSource, Node, NodeType,
    TTL, WaitableBackgroundSession, initialize_ctrlc_handler,
};

const FH_LRU_CACHE_SIZE: usize = 32;

#[derive(Debug)]
struct FileChunk {
    start: u64,
    size: u64,
    file: PathBuf,
}

fn binary_search(chunk: &FileChunk, offset: u64) -> std::cmp::Ordering {
    debug!(
        "{} between {} an {}?",
        offset,
        chunk.start,
        chunk.start + chunk.size
    );
    if offset < chunk.start {
        std::cmp::Ordering::Greater
    } else if offset >= chunk.start + chunk.size {
        std::cmp::Ordering::Less
    } else {
        std::cmp::Ordering::Equal
    }
}

#[derive(Debug)]
struct FileWithChunks {
    size: u64,
    mtime: SystemTime,
    chunks: Vec<FileChunk>,
}

struct ReaderWithOffset {
    reader: BufReader<File>,
    offset: u64,
}

struct FileHandle {
    readers_lru: LruCache<usize, ReaderWithOffset>,
}

pub struct DedupeReverseFS {
    source: PathBuf,
    file_handles: HandlePool<FileHandle>,
    dir_handles: HandlePool<Vec<DirEntryAddArgs>>,
    drop_hook: DropHookFn,
    rx_quitter: Option<Receiver<()>>,
    nodes: HashMap<u64, Node>,
    data: HashMap<u64, FileWithChunks>,
    meta_source: MetaSource,
}

impl DedupeReverseFS {
    pub fn new(
        source: impl AsRef<Path>,
        caches: Vec<impl AsRef<Path>>,
        declutter_level: usize,
    ) -> DedupeReverseFS {
        info!("new: {:?}", source.as_ref());

        let hydrator = Hydrator::new(source.as_ref(), caches.iter().map(|p| p.as_ref()).collect());

        info!("hydrator initialized");

        if !hydrator.check_cache(declutter_level) {
            panic!("Cache is not valid");
        }

        let mut nodes = HashMap::new();
        let mut data = HashMap::new();
        let mut paths = HashMap::new();

        // Root
        nodes.insert(
            INO_ROOT,
            Node {
                nlink: 2,
                parent: INO_ROOT,
                node_type: NodeType::Directory {
                    children: HashMap::new(),
                },
            },
        );
        paths.insert(OsString::new(), INO_ROOT);
        let mut next_ino = nodes.iter().map(|(ino, _)| ino).max().unwrap() + 1;

        for (path, mut fwc) in hydrator.cache.into_iter() {
            let path = PathBuf::from(path);

            debug!("path: {:?}", path);

            let mut acc_path = PathBuf::new();
            let mut parent_ino = INO_ROOT;
            for component in &path {
                acc_path.push(component);

                if !paths.contains_key(&acc_path.as_os_str().to_os_string()) {
                    let node = if acc_path == path {
                        data.insert(
                            next_ino,
                            FileWithChunks {
                                size: fwc.size,
                                mtime: fwc.mtime,
                                chunks: fwc
                                    .take_chunks()
                                    .unwrap()
                                    .into_iter()
                                    .map(|chunk| FileChunk {
                                        start: chunk.start,
                                        size: chunk.size,
                                        file: FileDeclutter::oneshot(chunk.hash, declutter_level),
                                    })
                                    .collect(),
                            },
                        );

                        Node {
                            nlink: 1,
                            parent: parent_ino,
                            node_type: NodeType::File(component.to_os_string()),
                        }
                    } else {
                        let parent = nodes.get_mut(&parent_ino).unwrap();
                        parent.nlink += 1;

                        Node {
                            nlink: 2,
                            parent: parent_ino,
                            node_type: NodeType::Directory {
                                children: HashMap::new(),
                            },
                        }
                    };

                    paths.insert(acc_path.as_os_str().to_os_string(), next_ino);
                    nodes.insert(next_ino, node);

                    if let Some(parent_node) = nodes.get_mut(&parent_ino) {
                        if let NodeType::Directory { children } = &mut parent_node.node_type {
                            children.insert(component.to_os_string(), next_ino);
                        }
                    }

                    next_ino += 1;
                }

                parent_ino = *paths.get(&acc_path.as_os_str().to_os_string()).unwrap();
            }
        }

        debug!("nodes: {:#?}", nodes);
        debug!("data: {:#?}", data);

        let file_handles = Default::default();
        let dir_handles = Default::default();

        let (drop_hook, rx_quitter) = initialize_ctrlc_handler();

        let meta_source = MetaSource::new(std::fs::metadata(&source).unwrap());

        DedupeReverseFS {
            file_handles,
            dir_handles,
            source: source.as_ref().into(),
            drop_hook,
            rx_quitter,
            nodes,
            data,
            meta_source,
        }
    }

    pub fn mount(
        mut self,
        mountpoint: impl AsRef<Path>,
    ) -> Result<WaitableBackgroundSession, Box<dyn std::error::Error>> {
        info!("mount: {:?}", mountpoint.as_ref());

        let rx_quitter = std::mem::take(&mut self.rx_quitter).unwrap();

        let options = vec![
            MountOption::RO,
            MountOption::FSName(String::from("dedupefs")),
        ];

        let _session = fuser::spawn_mount2(self, &mountpoint, options.as_ref())?;
        Ok(WaitableBackgroundSession {
            _session,
            rx_quitter,
        })
    }

    fn create_attrs_for_file(&self, ino: u64, size: u64, mtime: SystemTime) -> FileAttr {
        FileAttr {
            ino,
            size,
            blocks: (size + 511) / 512,
            uid: self.meta_source.uid,
            gid: self.meta_source.gid,
            mtime,
            ..ATTRS_DEFAULT
        }
    }

    fn create_attrs_for_dir(&self, ino: u64, nlink: u32) -> FileAttr {
        FileAttr {
            ino,
            kind: FileType::Directory,
            perm: 0o755,
            nlink,
            uid: self.meta_source.uid,
            gid: self.meta_source.gid,
            ..ATTRS_DEFAULT
        }
    }

    fn get_ino_from_parent_and_name(&self, parent: u64, name: impl AsRef<OsStr>) -> Option<u64> {
        self.nodes
            .get(&parent)
            .and_then(|parent_node| {
                if let NodeType::Directory { children } = &parent_node.node_type {
                    children.get(name.as_ref())
                } else {
                    None
                }
            })
            .map(|ino| *ino)
    }

    fn get_attr_from_ino(&self, ino: u64) -> Option<FileAttr> {
        self.nodes.get(&ino).and_then(|node| match &node.node_type {
            NodeType::File(..) => self
                .data
                .get(&ino)
                .map(|fwc| self.create_attrs_for_file(ino, fwc.size, fwc.mtime)),
            NodeType::Directory { .. } => {
                let mut attr = self.create_attrs_for_dir(ino, node.nlink);
                if ino == INO_ROOT {
                    attr.atime = self.meta_source.atime;
                    attr.mtime = self.meta_source.mtime;
                    attr.ctime = self.meta_source.ctime;
                }
                Some(attr)
            }
        })
    }
}

impl Drop for DedupeReverseFS {
    fn drop(&mut self) {
        let _ = &(self.drop_hook)();
    }
}

impl Filesystem for DedupeReverseFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        info!("lookup: {:?}", (parent, name));

        if let Some(ino) = self.get_ino_from_parent_and_name(parent, name) {
            if let Some(attr) = self.get_attr_from_ino(ino) {
                reply.entry(&TTL, &attr, 0);
                return;
            }
        }

        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        info!("getattr: {:?}", ino);

        if let Some(attr) = self.get_attr_from_ino(ino) {
            reply.attr(&TTL, &attr);
            return;
        }

        reply.error(ENOENT);
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        info!("open: {:?}", (ino));

        let Some(node) = self.nodes.get(&ino) else {
            reply.error(ENOENT);
            return;
        };

        let NodeType::File(name) = &node.node_type else {
            reply.error(EISDIR);
            return;
        };

        let file_handle = {
            let Some(fwc) = self.data.get(&ino) else {
                error!("Inconsistent state: No data for file {:?}", name);
                reply.error(EIO);
                return;
            };

            FileHandle {
                readers_lru: LruCache::new(
                    NonZeroUsize::new(FH_LRU_CACHE_SIZE.min(fwc.chunks.len())).unwrap(),
                ),
            }
        };

        let fh = self.file_handles.insert(file_handle);
        reply.opened(fh, 0);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        info!("read: {:?}", (ino, fh));

        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }

        let mut offset = offset as u64;
        let mut size = size as u64;

        let Some(handle) = self.file_handles.used.get_mut(&fh) else {
            error!("Inconsistent state: No file handle data for fh {:?}", fh);
            reply.error(EIO);
            return;
        };

        let Some(fwc) = self.data.get(&ino) else {
            error!("Inconsistent state: No data for ino {:?}", ino);
            reply.error(EIO);
            return;
        };

        debug!("read, offset: {}", offset);
        debug!("read, size: {}", size);

        let Some(chunk_index_start) = fwc
            .chunks
            .binary_search_by(|chunk| binary_search(chunk, offset))
            .ok()
        else {
            reply.data(&[]);
            return;
        };

        let chunk_index_end = fwc
            .chunks
            .binary_search_by(|chunk| binary_search(chunk, offset + size - 1))
            .ok()
            .unwrap_or(fwc.chunks.len() - 1);

        let mut buffer = Vec::with_capacity(size as usize);

        debug!("read, chunk_index_start: {}", chunk_index_start);
        debug!("read, chunk_index_end: {}", chunk_index_end);

        for chunk_index in chunk_index_start..=chunk_index_end {
            let chunk = fwc.chunks.get(chunk_index).unwrap();
            let reader = handle
                .readers_lru
                .get_or_insert_mut(chunk_index, || ReaderWithOffset {
                    reader: BufReader::new(
                        File::open(self.source.join("data").join(&chunk.file)).unwrap(),
                    ),
                    offset: 0,
                });

            {
                let new_offset = 0.max(offset - chunk.start);
                reader
                    .reader
                    .seek_relative(new_offset as i64 - reader.offset as i64)
                    .unwrap();
                reader.offset = new_offset;
            }

            let chunk_size = {
                let readable = chunk.size - (offset - chunk.start);
                if size <= readable { size } else { readable }
            };

            let mut chunk_buffer = vec![0; chunk_size as usize];
            if let Err(e) = reader.reader.read_exact(&mut chunk_buffer) {
                error!("Error reading file: {}", e);
                reply.error(EIO);
                return;
            };
            reader.offset += chunk_size;

            buffer.append(&mut chunk_buffer);

            size -= chunk_size;
            offset = 0;
        }

        reply.data(&buffer);
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        info!("release: {:?}", (_ino, fh));

        self.file_handles.remove(fh);
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        info!("opendir: {}", ino);

        let Some(node) = self.nodes.get(&ino) else {
            reply.error(ENOENT);
            return;
        };

        let NodeType::Directory { children } = &node.node_type else {
            reply.error(ENOTDIR);
            return;
        };

        let entries = std::iter::once(DirEntryAddArgs {
            ino,
            kind: FileType::Directory,
            name: OsString::from("."),
        })
        .chain(std::iter::once(DirEntryAddArgs {
            ino: node.parent,
            kind: FileType::Directory,
            name: OsString::from(".."),
        }))
        .chain(children.iter().filter_map(|(child_name, child_ino)| {
            if let Some(child_node) = self.nodes.get(&child_ino) {
                Some(DirEntryAddArgs {
                    ino: *child_ino,
                    kind: match child_node.node_type {
                        NodeType::File(_) => FileType::RegularFile,
                        NodeType::Directory { .. } => FileType::Directory,
                    },
                    name: child_name.clone(),
                })
            } else {
                None
            }
        }))
        .collect();

        let fh = self.dir_handles.insert(entries);
        reply.opened(fh, 0);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        info!("readdir: fh={fh}, offset={offset}");

        let entries = self.dir_handles.used.get(&fh).unwrap();
        for (index, entry) in entries.iter().enumerate().skip(0.max(offset as usize)) {
            let is_full = reply.add(
                entry.ino,
                (index + 1) as i64,
                entry.kind,
                entry.name.as_os_str(),
            );

            if is_full {
                break;
            }
        }

        reply.ok();
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        info!("releasedir: {}", fh);
        self.dir_handles.remove(fh);
        reply.ok();
    }
}
