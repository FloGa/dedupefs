use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::{File, Metadata};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Receiver;
use std::time::SystemTime;

use crazy_deduper::{Deduper, FileChunk, HashingAlgorithm};
use file_declutter::FileDeclutter;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, Request,
};
use libc::{EIO, EISDIR, ENOENT};
use log::{debug, error, info};

use crate::{
    ATTRS_DEFAULT, DirEntryAddArgs, DropHookFn, FilesystemShared, HandlePool, INO_CACHE, INO_ROOT,
    MetaSource, Mountable, Node, NodeType, initialize_ctrlc_handler, system_time_from_time,
};

#[derive(Clone, Copy, Debug)]
struct MetaCache {
    size: u64,
    atime: SystemTime,
    mtime: SystemTime,
    ctime: SystemTime,
}

impl MetaCache {
    fn new(meta_cache_fs: Metadata) -> Self {
        Self {
            size: meta_cache_fs.size(),
            atime: system_time_from_time(meta_cache_fs.atime(), meta_cache_fs.atime_nsec()),
            mtime: system_time_from_time(meta_cache_fs.mtime(), meta_cache_fs.mtime_nsec()),
            ctime: system_time_from_time(meta_cache_fs.ctime(), meta_cache_fs.ctime_nsec()),
        }
    }
}

struct FileHandle {
    file: BufReader<File>,
    start: u64,
    size: u64,
    offset: u64,
}

pub struct DedupeFS {
    source: PathBuf,
    file_handles: HandlePool<FileHandle>,
    dir_handles: HandlePool<Vec<DirEntryAddArgs>>,
    drop_hook: DropHookFn,
    rx_quitter: Option<Receiver<()>>,
    cache_file: PathBuf,
    hashed_chunks: HashMap<OsString, FileChunk>,
    nodes: HashMap<u64, Node>,
    meta_source: MetaSource,
    meta_cache: MetaCache,
}

impl DedupeFS {
    pub fn new(
        source: impl AsRef<Path>,
        caches: Vec<impl AsRef<Path>>,
        hashing_algorithm: HashingAlgorithm,
        declutter_level: usize,
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
            .levels(declutter_level)
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

        let (drop_hook, rx_quitter) = initialize_ctrlc_handler();

        let meta_source = MetaSource::new(std::fs::metadata(&source).unwrap());
        let meta_cache = MetaCache::new(std::fs::metadata(&cache_file).unwrap());

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
}

impl Mountable for DedupeFS {
    fn get_fsname() -> String {
        String::from("dedupefs")
    }

    fn get_quitter_receiver_option_mut(&mut self) -> &mut Option<Receiver<()>> {
        &mut self.rx_quitter
    }
}

impl FilesystemShared for DedupeFS {
    fn get_node_map(&self) -> &HashMap<u64, Node> {
        &self.nodes
    }

    fn get_dir_handles(&self) -> &HandlePool<Vec<DirEntryAddArgs>> {
        &self.dir_handles
    }

    fn get_dir_handles_mut(&mut self) -> &mut HandlePool<Vec<DirEntryAddArgs>> {
        &mut self.dir_handles
    }

    fn create_attrs_for_file(&self, ino: u64, size: u64, _mtime: SystemTime) -> FileAttr {
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
            ..self.create_attrs_for_file(ino, 0, SystemTime::now())
        }
    }

    fn get_attr_from_ino(&self, ino: u64) -> Option<FileAttr> {
        if ino == INO_CACHE {
            let mut attr = self.create_attrs_for_file(ino, self.meta_cache.size, SystemTime::now());
            attr.atime = self.meta_cache.atime;
            attr.mtime = self.meta_cache.mtime;
            attr.ctime = self.meta_cache.ctime;
            Some(attr)
        } else {
            self.nodes.get(&ino).and_then(|node| match &node.node_type {
                NodeType::File(name) => self
                    .hashed_chunks
                    .get(name)
                    .map(|chunk| self.create_attrs_for_file(ino, chunk.size, SystemTime::now())),
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
}

impl Drop for DedupeFS {
    fn drop(&mut self) {
        let _ = &(self.drop_hook)();
    }
}

impl Filesystem for DedupeFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        FilesystemShared::lookup(self, parent, name, reply);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        FilesystemShared::getattr(self, ino, reply);
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        info!("open: {:?}", ino);

        let Some(node) = self.nodes.get(&ino) else {
            reply.error(ENOENT);
            return;
        };

        let NodeType::File(name) = &node.node_type else {
            reply.error(EISDIR);
            return;
        };

        let file_handle = if ino == INO_CACHE {
            let file = File::open(&self.cache_file).unwrap();
            let file = BufReader::new(file);
            FileHandle {
                file,
                start: 0,
                size: self.meta_cache.size,
                offset: 0,
            }
        } else {
            let Some(chunk) = self.hashed_chunks.get(name) else {
                error!("Inconsistent state: No chunk for file {:?}", name);
                reply.error(EIO);
                return;
            };

            let file = File::open(self.source.join(chunk.path.as_ref().unwrap())).unwrap();
            let file = {
                let mut file = BufReader::new(file);
                file.seek(SeekFrom::Start(chunk.start)).unwrap();
                file
            };
            FileHandle {
                file,
                start: chunk.start,
                size: chunk.size,
                offset: 0,
            }
        };

        let fh = self.file_handles.insert(file_handle);
        reply.opened(fh, 0);
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

        let handle = self.file_handles.used.get_mut(&fh).unwrap();

        if offset != handle.offset {
            handle
                .file
                .seek(SeekFrom::Start(handle.start + offset))
                .unwrap();
            handle.offset = offset;
        }

        let size = size as u64;
        let size = size.min(handle.size - offset.min(handle.size));

        let mut buffer = vec![0; size as usize];
        if let Err(e) = handle.file.read_exact(&mut buffer) {
            error!("Error reading file: {}", e);
            reply.error(EIO);
            return;
        };
        handle.offset += size;

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
        FilesystemShared::opendir(self, ino, reply);
    }

    fn readdir(&mut self, _req: &Request, _ino: u64, fh: u64, offset: i64, reply: ReplyDirectory) {
        FilesystemShared::readdir(self, fh, offset, reply);
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        FilesystemShared::releasedir(self, fh, reply);
    }
}
