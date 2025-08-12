//! # DedupeFS
//!
//! [![badge github]][url github]
//! [![badge crates.io]][url crates.io]
//! [![badge docs.rs]][url docs.rs]
//! [![badge license]][url license]
//!
//! [//]: # (@formatter:off)
//! [badge github]: https://img.shields.io/badge/github-FloGa%2Fdedupefs-green
//! [badge crates.io]: https://img.shields.io/crates/v/dedupefs
//! [badge docs.rs]: https://img.shields.io/docsrs/dedupefs
//! [badge license]: https://img.shields.io/crates/l/dedupefs
//!
//! [url github]: https://github.com/FloGa/dedupefs
//! [url crates.io]: https://crates.io/crates/dedupefs
//! [url docs.rs]: https://docs.rs/dedupefs
//! [url license]: https://github.com/FloGa/dedupefs/blob/develop/LICENSE
//! [//]: # (@formatter:on)
//!
//! > Presents files as deduplicated, content-addressed 1MB chunks with selectable hash algorithms.
//!
//! *DedupeFS* is a FUSE filesystem over my [*Crazy Deduper*][crazy-deduper github] application. It is so to speak the
//! logical successor of [*SCFS*][scfs github]. While *SCFS* presented each file as chunks, independent of each other,
//! *DedupeFS* calculates the checksum of each chunk and collects them all in one directory. That way, each unique chunk is
//! only presented once, even if it is used by multiple files.
//!
//! *DedupeFS* is mainly useful to create efficient backups and upload them to a cloud provider. The file chunks have the
//! advantage that the upload does not have to be all-or-nothing, so if your internet connection vanishes for a second, your
//! 4GB file upload will not be completely cancelled, only the currently transferred chunk upload will be aborted.
//!
//! By keeping multiple cache files around, you can easily and efficiently have incremental backups that all share the same
//! chunks.
//!
//! [//]: # (@formatter:off)
//! [crazy-deduper github]: https://github.com/FloGa/crazy-deduper
//! [scfs github]: https://github.com/FloGa/scfs
//! [//]: # (@formatter:on)
//!
//! ## Installation
//!
//! This tool can be installed easily through Cargo via `crates.io`:
//!
//! ```shell
//! cargo install --locked dedupefs
//! ```
//!
//! Please note that the `--locked` flag is necessary here to have the exact same dependencies as when the application was
//! tagged and tested. Without it, you might get more up-to-date versions of dependencies, but you have the risk of
//! undefined and unexpected behavior if the dependencies changed some functionalities. The application might even fail to
//! build if the public API of a dependency changed too much.
//!
//! Alternatively, pre-built binaries can be downloaded from the [GitHub releases][gh-releases] page.
//!
//! [gh-releases]: https://github.com/FloGa/dedupefs/releases
//!
//! ## Usage
//!
//! ```text
//! Usage: dedupefs [OPTIONS] <SOURCE> <MOUNTPOINT>
//!
//! Arguments:
//!   <SOURCE>
//!           Source directory
//!
//!   <MOUNTPOINT>
//!           Mount point
//!
//! Options:
//!       --cache-file <CACHE_FILE>
//!           Path to cache file
//!
//!           Can be used multiple times. The files are read in reverse order, so they should be sorted with the most accurate ones in the beginning. The first given will be written.
//!
//!       --hashing-algorithm <HASHING_ALGORITHM>
//!           Hashing algorithm to use for chunk filenames
//!
//!           [default: sha1]
//!           [possible values: md5, sha1, sha256, sha512]
//!
//!   -f, --foreground
//!           Stay in foreground, do not daemonize into the background
//!
//!   -h, --help
//!           Print help (see a summary with '-h')
//!
//!   -V, --version
//!           Print version
//! ```
//!
//! To mount a deduped version of `source` directory to `deduped`, you can use:
//!
//! ```shell
//! dedupefs --cache-file cache.json.zst source deduped
//! ```
//!
//! If the cache file ends with `.zst`, it will be encoded (or decoded in the case of hydrating) using the ZSTD compression
//! algorithm. For any other extension, plain JSON will be used.
//!
//! Please note that for now it is not possible to mount a hydrated version of a deduped directory. This wil be added in a
//! future version. For now, you can use [*Crazy Deduper*][crazy-deduper github] to physically re-hydrate your files.
//!
//! ## Cache Files
//!
//! The cache file is necessary to keep track of all file chunks and hashes. Without the cache you would not be able to
//! restore your files.
//!
//! The cache file can be re-used, even if the source directory changed. It keeps track of the file sizes and modification
//! times and only re-hashes new or changed files. Deleted files are deleted from the cache.
//!
//! You can also use older cache files in addition to a new one:
//!
//! ```shell
//! dedupefs --cache-file cache.json.zst --cache-file cache-from-yesterday.json.zst source deduped
//! ```
//!
//! The cache files are read in reverse order in which they are given on the command line, so the content of earlier cache
//! files is preferred over later ones. Hence, you should put your most accurate cache files to the beginning. Moreover, the
//! first given cache file is the one that will be written to, it does not need to exist.
//!
//! In the given example, if `cache.json.zst` does not exist, the internal cache is pre-filled from
//! `cache-from-yesterday.json.zst` so that only new and modified files need to be re-hashed. The result is then written
//! into `cache.json.zst`.
//!
//! In the mounted deduped directory, the first cache file given on the command line will be presented with the same name
//! directly under the mountpoint. next to the data directory. When uploading your chunks, always make sure to also upload
//! this cache file, otherwise you wil not be able to properly re-hydrate your files afterward!
//!
//! ## TODO
//!
//! - Support mounting a re-hydrated version of a deduped directory.
//! - Make declutter level configurable (fixed to 3 at the moment).
//! - Make chunk site configurable (via *Crazy Deduper*, fixed to 1MB at the moment).
//! - Provide better documentation with examples and use case descriptions.

use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, iter};

use crazy_deduper::{Deduper, FileChunk, HashingAlgorithm};
use file_declutter::FileDeclutter;
use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request,
};
use libc::{ENOENT, ENOTDIR};
use log::{debug, info, warn};

pub mod cli;

const TTL: Duration = Duration::from_secs(1);

const INO_ROOT: u64 = 1;
const INO_CACHE: u64 = 2;

const ATTRS_DEFAULT: FileAttr = FileAttr {
    ino: 0,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH,
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 0,
    gid: 0,
    rdev: 0,
    blksize: 4096,
    flags: 0,
};

type DropHookFn = Box<dyn Fn() + Send + 'static>;

fn system_time_from_time(secs: i64, nsecs: i64) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nsecs as u32)
    } else {
        UNIX_EPOCH - Duration::new((-secs) as u64, nsecs as u32)
    }
}

pub struct WaitableBackgroundSession {
    _session: BackgroundSession,
    rx_quitter: Receiver<()>,
}

#[derive(Debug)]
enum NodeType {
    File(OsString),
    Directory { children: HashMap<OsString, u64> },
}

#[derive(Debug)]
struct Node {
    nlink: u32,
    parent: u64,
    node_type: NodeType,
}

#[derive(Clone, Copy, Debug)]
struct MetaSource {
    uid: u32,
    gid: u32,
    atime: SystemTime,
    mtime: SystemTime,
    ctime: SystemTime,
}

#[derive(Clone, Copy, Debug)]
struct MetaCache {
    size: u64,
    atime: SystemTime,
    mtime: SystemTime,
    ctime: SystemTime,
}

pub struct DedupeFS {
    source: PathBuf,
    file_handles: HashMap<u64, FileHandle>,
    dir_handles: HashMap<u64, Vec<DirEntryAddArgs>>,
    drop_hook: DropHookFn,
    rx_quitter: Option<Receiver<()>>,
    cache_file: PathBuf,
    hashed_chunks: HashMap<OsString, FileChunk>,
    nodes: HashMap<u64, Node>,
    meta_source: MetaSource,
    meta_cache: MetaCache,
}

struct FileHandle {
    file: BufReader<File>,
    start: u64,
    size: u64,
    offset: u64,
}

#[derive(Clone, Debug)]
struct DirEntryAddArgs {
    ino: u64,
    kind: FileType,
    name: OsString,
}

impl WaitableBackgroundSession {
    pub fn wait(&self) {
        self.rx_quitter
            .recv()
            .expect("Could not join quitter channel");
    }
}

impl DedupeFS {
    pub fn new(
        source: impl AsRef<Path>,
        caches: Vec<impl AsRef<Path>>,
        hashing_algorithm: HashingAlgorithm,
    ) -> DedupeFS {
        info!("new: {:?}", source.as_ref());

        let deduper = Deduper::new(
            source.as_ref(),
            caches.iter().map(|p| p.as_ref()).collect(),
            hashing_algorithm,
            false,
        );

        info!("dedupe initialized");

        let mut last_write = SystemTime::now();
        let mut write_necessary = false;

        let hashed_chunks = deduper
            .cache
            .get_chunks()
            .unwrap()
            .map(|(hash, chunk, dirty)| {
                if dirty {
                    write_necessary = true;
                    if last_write.elapsed().unwrap().as_secs() > 5 {
                        deduper.write_cache();
                        info!("dedupe cache written");
                        last_write = SystemTime::now();
                    }
                }

                debug!("{hash}: {chunk:?}");

                (OsString::from(&hash), chunk)
            })
            .collect::<HashMap<_, _>>();

        let cache_file = caches.first().unwrap().as_ref().to_path_buf();
        let cache_filename = OsString::from(cache_file.file_name().unwrap());

        if write_necessary || !cache_file.exists() {
            deduper.write_cache();
            info!("dedupe cache written");
        }

        let mut nodes = HashMap::new();
        let mut paths = HashMap::new();

        // Cache
        nodes.insert(
            INO_CACHE,
            Node {
                nlink: 1,
                parent: INO_ROOT,
                node_type: NodeType::File(cache_filename.clone()),
            },
        );

        // Root
        nodes.insert(
            INO_ROOT,
            Node {
                nlink: 2,
                parent: INO_ROOT,
                node_type: NodeType::Directory {
                    children: HashMap::from([(cache_filename, INO_CACHE)]),
                },
            },
        );
        paths.insert(OsString::new(), INO_ROOT);
        let mut next_ino = nodes.iter().map(|(ino, _)| ino).max().unwrap() + 1;

        for path in FileDeclutter::new_from_iter(hashed_chunks.keys().cloned().map(PathBuf::from))
            .base(PathBuf::from("data"))
            .levels(3)
            .map(|(_, path)| path)
        {
            debug!("path: {:?}", path);

            let mut acc_path = PathBuf::new();
            let mut parent_ino = INO_ROOT;
            for component in &path {
                acc_path.push(component);

                if !paths.contains_key(&acc_path.as_os_str().to_os_string()) {
                    let node = if acc_path == path {
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

        let file_handles = Default::default();
        let dir_handles = Default::default();

        let (tx_quitter, rx_quitter) = channel();

        {
            let tx_quitter = tx_quitter.clone();
            if let Err(e) = ctrlc::set_handler(move || {
                tx_quitter.send(()).unwrap();
            }) {
                // Failure to set the Ctrl-C handler should not cause the program to exit.
                warn!("Error setting Ctrl-C handler: {}", e.to_string());
            }
        }

        let drop_hook = Box::new(move || {
            tx_quitter.send(()).unwrap();
        });

        let rx_quitter = Some(rx_quitter);

        let meta_source_fs = fs::metadata(&source).unwrap();
        let meta_source = MetaSource {
            uid: meta_source_fs.uid(),
            gid: meta_source_fs.gid(),
            atime: system_time_from_time(meta_source_fs.atime(), meta_source_fs.atime_nsec()),
            mtime: system_time_from_time(meta_source_fs.mtime(), meta_source_fs.mtime_nsec()),
            ctime: system_time_from_time(meta_source_fs.ctime(), meta_source_fs.ctime_nsec()),
        };

        let meta_cache_fs = fs::metadata(&cache_file).unwrap();
        let meta_cache = MetaCache {
            size: meta_cache_fs.size(),
            atime: system_time_from_time(meta_cache_fs.atime(), meta_cache_fs.atime_nsec()),
            mtime: system_time_from_time(meta_cache_fs.mtime(), meta_cache_fs.mtime_nsec()),
            ctime: system_time_from_time(meta_cache_fs.ctime(), meta_cache_fs.ctime_nsec()),
        };

        DedupeFS {
            file_handles,
            dir_handles,
            source: source.as_ref().into(),
            drop_hook,
            rx_quitter,
            cache_file,
            hashed_chunks,
            nodes,
            meta_source,
            meta_cache,
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

    fn create_attrs_for_file(&self, ino: u64, size: u64) -> FileAttr {
        FileAttr {
            ino,
            size,
            blocks: (size + 511) / 512,
            uid: self.meta_source.uid,
            gid: self.meta_source.gid,
            ..ATTRS_DEFAULT
        }
    }

    fn create_attrs_for_dir(&self, ino: u64, nlink: u32) -> FileAttr {
        FileAttr {
            kind: FileType::Directory,
            perm: 0o755,
            nlink,
            ..self.create_attrs_for_file(ino, 0)
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
        if ino == INO_CACHE {
            let mut attr = self.create_attrs_for_file(ino, self.meta_cache.size);
            attr.atime = self.meta_cache.atime;
            attr.mtime = self.meta_cache.mtime;
            attr.ctime = self.meta_cache.ctime;
            Some(attr)
        } else {
            self.nodes.get(&ino).and_then(|node| match &node.node_type {
                NodeType::File(name) => self
                    .hashed_chunks
                    .get(name)
                    .map(|chunk| self.create_attrs_for_file(ino, chunk.size)),
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

    fn insert_new_file_handle(&mut self, file_handle: FileHandle) -> u64 {
        let fh = self.file_handles.keys().copied().max().unwrap_or(0) + 1;
        self.file_handles.insert(fh, file_handle);
        fh
    }

    fn insert_new_dir_handle(&mut self, dir_entries: Vec<DirEntryAddArgs>) -> u64 {
        let fh = self.dir_handles.keys().copied().max().unwrap_or(0) + 1;
        self.dir_handles.insert(fh, dir_entries);
        fh
    }
}

impl Drop for DedupeFS {
    fn drop(&mut self) {
        let _ = &(self.drop_hook)();
    }
}

impl Filesystem for DedupeFS {
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

        if ino == INO_CACHE {
            let file = File::open(&self.cache_file).unwrap();
            let file = BufReader::new(file);
            let fh = self.file_handles.keys().max().unwrap_or(&0).clone() + 1;
            self.file_handles.insert(
                fh,
                FileHandle {
                    file,
                    start: 0,
                    size: self.meta_cache.size,
                    offset: 0,
                },
            );
            reply.opened(fh, 0);
            return;
        }

        if let Some(node) = self.nodes.get(&ino) {
            if let NodeType::File(name) = &node.node_type {
                if let Some(chunk) = self.hashed_chunks.get(name) {
                    let file = File::open(self.source.join(chunk.path.as_ref().unwrap())).unwrap();
                    let file = {
                        let mut file = BufReader::new(file);
                        file.seek(SeekFrom::Start(chunk.start)).unwrap();
                        file
                    };
                    let fh = self.file_handles.keys().max().unwrap_or(&0).clone() + 1;
                    self.file_handles.insert(
                        fh,
                        FileHandle {
                            file,
                            start: chunk.start,
                            size: chunk.size,
                            offset: 0,
                        },
                    );
                    reply.opened(fh, 0);
                    return;
                }
            }
        }

        reply.error(ENOENT)
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        info!("read: {:?}", (_ino, fh));

        let offset = offset as u64;
        let size = size as u64;

        let handle = self.file_handles.get_mut(&fh).unwrap();

        if offset != handle.offset {
            handle
                .file
                .seek(SeekFrom::Start(handle.start + offset))
                .unwrap();
            handle.offset = offset;
        }

        reply.data(
            &handle
                .file
                .borrow_mut()
                .take(size.min(handle.size - offset.min(handle.size)))
                .bytes()
                .map(|b| b.unwrap())
                .collect::<Vec<_>>(),
        );

        handle.offset += size;
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

        self.file_handles.remove(&fh);
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

        let entries = iter::once(DirEntryAddArgs {
            ino,
            kind: FileType::Directory,
            name: OsString::from("."),
        })
        .chain(iter::once(DirEntryAddArgs {
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

        let fh = self.dir_handles.keys().copied().max().unwrap_or(0) + 1;
        self.dir_handles.insert(fh, entries);
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

        let entries = self.dir_handles.get(&fh).unwrap();
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
        self.dir_handles.remove(&fh);
        reply.ok();
    }
}
