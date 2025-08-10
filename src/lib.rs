use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crazy_deduper::{Deduper, FileChunk, HashingAlgorithm};
use file_declutter::FileDeclutter;
use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request,
};
use libc::ENOENT;
use log::{debug, info};

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

pub struct DedupeFS {
    source: PathBuf,
    file_handles: HashMap<u64, FileHandle>,
    drop_hook: DropHookFn,
    rx_quitter: Option<Receiver<()>>,
    cache_file: PathBuf,
    hashed_chunks: HashMap<OsString, FileChunk>,
    nodes: HashMap<u64, Node>,
    source_uid: u32,
    source_gid: u32,
    source_atime: SystemTime,
    source_mtime: SystemTime,
    source_ctime: SystemTime,
    cache_size: u64,
    cache_atime: SystemTime,
    cache_mtime: SystemTime,
    cache_ctime: SystemTime,
}

struct FileHandle {
    file: BufReader<File>,
    start: u64,
    size: u64,
    offset: u64,
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

        deduper.write_cache();

        info!("dedupe cache written");

        let mut last_write = SystemTime::now();

        let hashed_chunks = deduper
            .cache
            .get_chunks()
            .unwrap()
            .map(|(hash, chunk, dirty)| {
                if dirty && last_write.elapsed().unwrap().as_secs() > 5 {
                    deduper.write_cache();
                    info!("dedupe cache written");
                    last_write = SystemTime::now();
                }

                debug!("{hash}: {chunk:?}");

                (OsString::from(&hash), chunk)
            })
            .collect::<HashMap<_, _>>();

        deduper.write_cache();

        let cache_file = caches.first().unwrap().as_ref().to_path_buf();
        let cache_filename = OsString::from(cache_file.file_name().unwrap());

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

        let (tx_quitter, rx_quitter) = channel();

        {
            let tx_quitter = tx_quitter.clone();
            ctrlc::set_handler(move || {
                tx_quitter.send(()).unwrap();
            })
            .expect("Error setting Ctrl-C handler");
        }

        let drop_hook = Box::new(move || {
            tx_quitter.send(()).unwrap();
        });

        let rx_quitter = Some(rx_quitter);

        let meta_source = fs::metadata(&source).unwrap();
        let source_uid = meta_source.uid();
        let source_gid = meta_source.gid();
        let source_atime = system_time_from_time(meta_source.atime(), meta_source.atime_nsec());
        let source_mtime = system_time_from_time(meta_source.mtime(), meta_source.mtime_nsec());
        let source_ctime = system_time_from_time(meta_source.ctime(), meta_source.ctime_nsec());

        let meta_cache = fs::metadata(&cache_file).unwrap();
        let cache_size = meta_cache.size();
        let cache_atime = system_time_from_time(meta_cache.atime(), meta_cache.atime_nsec());
        let cache_mtime = system_time_from_time(meta_cache.mtime(), meta_cache.mtime_nsec());
        let cache_ctime = system_time_from_time(meta_cache.ctime(), meta_cache.ctime_nsec());

        DedupeFS {
            file_handles,
            source: source.as_ref().into(),
            drop_hook,
            rx_quitter,
            cache_file,
            hashed_chunks,
            nodes,
            source_uid,
            source_gid,
            source_atime,
            source_mtime,
            source_ctime,
            cache_size,
            cache_atime,
            cache_mtime,
            cache_ctime,
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
            uid: self.source_uid,
            gid: self.source_gid,
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
            let mut attr = self.create_attrs_for_file(ino, self.cache_size);
            attr.atime = self.cache_atime;
            attr.mtime = self.cache_mtime;
            attr.ctime = self.cache_ctime;
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
                        attr.atime = self.source_atime;
                        attr.mtime = self.source_mtime;
                        attr.ctime = self.source_ctime;
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
                    size: u64::MAX,
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

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        info!("readdir: {}, {}", ino, offset);

        if let Some(node) = self.nodes.get(&ino) {
            if let NodeType::Directory { children, .. } = &node.node_type {
                if offset < 2 {
                    if offset == 0 {
                        if reply.add(ino, 1, FileType::Directory, ".") {
                            reply.ok();
                            return;
                        }
                    }

                    if reply.add(
                        self.nodes.get(&ino).unwrap().parent,
                        2,
                        FileType::Directory,
                        "..",
                    ) {
                        reply.ok();
                        return;
                    }
                }

                for (off, (child_name, child_ino)) in children
                    .iter()
                    .enumerate()
                    .skip(0.max((offset - 2) as usize))
                {
                    if let Some(child_node) = self.nodes.get(&child_ino) {
                        let is_full = reply.add(
                            *child_ino,
                            (off + 2 + 1) as i64,
                            match child_node.node_type {
                                NodeType::File(_) => FileType::RegularFile,
                                NodeType::Directory { .. } => FileType::Directory,
                            },
                            child_name,
                        );

                        if is_full {
                            break;
                        }
                    }
                }
            }
        } else {
            reply.error(ENOENT);
            return;
        }

        reply.ok();
    }
}
