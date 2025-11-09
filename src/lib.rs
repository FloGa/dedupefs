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
//!       --declutter-levels <DECLUTTER_LEVELS>
//!           Declutter files into this many subdirectory levels
//!
//!           [default: 3]
//!
//!       --reverse
//!           Reverse mode, present chunks re-hydrated
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
//! To mount a re-hydrated version of `deduped` directory to `restored`, you can use:
//!
//! ```shell
//! dedupefs --reverse --cache-file cache.json.zst deduped restored
//! ```
//!
//! Before mounting, it will be checked if all chunks present in the cache file are available in the `deduped/data`
//! directory. If not, the mount will fail.
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
//! ## Helper Commands
//!
//! There are several helper commands available to work with the cache files and to inspect the internal state of the
//! deduplicated data chunks:
//!
//! ### Check Cache
//!
//! ```text
//! Check if cache file is valid and all chunks exist.
//!
//! Usage: dedupefs_check_cache [OPTIONS] <SOURCE>
//!
//! Arguments:
//!   <SOURCE>
//!           Source directory to deduped files
//!
//! Options:
//!       --cache-file <CACHE_FILE>
//!           Path to cache file
//!
//!           Can be used multiple times. The files are read in reverse order, so they should be sorted with the most accurate ones in the beginning. They will only be read, not written.
//!
//!       --declutter-levels <DECLUTTER_LEVELS>
//!           Declutter files into this many subdirectory levels
//!
//!           [default: 3]
//!
//!   -h, --help
//!           Print help (see a summary with '-h')
//!
//!   -V, --version
//!           Print version
//! ```
//!
//! ### Create Cache
//!
//! ```text
//! Only create cache file without actually mounting.
//!
//! Usage: dedupefs_create_cache [OPTIONS] <SOURCE>
//!
//! Arguments:
//!   <SOURCE>
//!           Source directory
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
//!       --declutter-levels <DECLUTTER_LEVELS>
//!           Declutter files into this many subdirectory levels
//!
//!           [default: 3]
//!
//!   -h, --help
//!           Print help (see a summary with '-h')
//!
//!   -V, --version
//!           Print version
//! ```
//!
//! ### Delete Extra Files
//!
//! ```text
//! Delete files not present in any cache files.
//!
//! Usage: dedupefs_delete_extra_files [OPTIONS] <SOURCE>
//!
//! Arguments:
//!   <SOURCE>
//!           Source directory
//!
//! Options:
//!       --cache-file <CACHE_FILE>
//!           Path to cache file
//!
//!           Can be used multiple times. The files are read in reverse order, so they should be sorted with the most accurate ones in the beginning. They will only be read, not written.
//!
//!   -v
//!           List deleted files
//!
//!   -f
//!           Force deletion
//!
//!       --declutter-levels <DECLUTTER_LEVELS>
//!           Declutter files into this many subdirectory levels
//!
//!           [default: 3]
//!
//!   -h, --help
//!           Print help (see a summary with '-h')
//!
//!   -V, --version
//!           Print version
//! ```
//!
//! ### List Extra Files
//!
//! ```text
//! List files not present in any cache files.
//!
//! Usage: dedupefs_list_extra_files [OPTIONS] <SOURCE>
//!
//! Arguments:
//!   <SOURCE>
//!           Source directory
//!
//! Options:
//!       --cache-file <CACHE_FILE>
//!           Path to cache file
//!
//!           Can be used multiple times. The files are read in reverse order, so they should be sorted with the most accurate ones in the beginning. They will only be read, not written.
//!
//!   -0
//!           Separate file names with null character instead of newline
//!
//!       --declutter-levels <DECLUTTER_LEVELS>
//!           Declutter files into this many subdirectory levels
//!
//!           [default: 3]
//!
//!   -h, --help
//!           Print help (see a summary with '-h')
//!
//!   -V, --version
//!           Print version
//! ```
//!
//! ### List Missing Chunks
//!
//! ```text
//! List chunks from cache files that are not present in the source directory.
//!
//! Usage: dedupefs_list_missing_chunks [OPTIONS] <SOURCE>
//!
//! Arguments:
//!   <SOURCE>
//!           Source directory
//!
//! Options:
//!       --cache-file <CACHE_FILE>
//!           Path to cache file
//!
//!           Can be used multiple times. The files are read in reverse order, so they should be sorted with the most accurate ones in the beginning. They will only be read, not written.
//!
//!       --with-reason
//!           Also display the reason for the missing or invalid chunk
//!
//!   -0
//!           Separate file names with null character instead of newline
//!
//!   -h, --help
//!           Print help (see a summary with '-h')
//!
//!   -V, --version
//!           Print version
//! ```
//!
//! ## TODO
//!
//! - Make chunk size configurable (via *Crazy Deduper*, fixed to 1MB at the moment).
//! - Provide better documentation with examples and use case descriptions.

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::Metadata;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen,
};
use libc::{ENOENT, ENOTDIR};
use log::{info, warn};

pub mod cli;
mod fs;

pub use fs::normal::DedupeFS;
pub use fs::reverse::DedupeReverseFS;

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

impl WaitableBackgroundSession {
    pub fn wait(&self) {
        self.rx_quitter
            .recv()
            .expect("Could not join quitter channel");
    }
}

#[derive(Debug)]
enum NodeType {
    File(OsString),
    Directory { children: HashMap<OsString, u64> },
}

#[derive(Debug)]
pub struct Node {
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

impl MetaSource {
    fn new(meta_source_fs: Metadata) -> Self {
        Self {
            uid: meta_source_fs.uid(),
            gid: meta_source_fs.gid(),
            atime: system_time_from_time(meta_source_fs.atime(), meta_source_fs.atime_nsec()),
            mtime: system_time_from_time(meta_source_fs.mtime(), meta_source_fs.mtime_nsec()),
            ctime: system_time_from_time(meta_source_fs.ctime(), meta_source_fs.ctime_nsec()),
        }
    }
}

#[derive(Clone, Debug)]
struct DirEntryAddArgs {
    ino: u64,
    kind: FileType,
    name: OsString,
}

struct HandlePool<T> {
    used: HashMap<u64, T>,
    free: Vec<u64>,
    next_free: u64,
}

impl<T> Default for HandlePool<T> {
    fn default() -> Self {
        HandlePool {
            used: Default::default(),
            free: Default::default(),
            next_free: 1,
        }
    }
}

impl<T> HandlePool<T> {
    fn get_free_id(&mut self) -> u64 {
        self.free.pop().unwrap_or_else(|| {
            let id = self.next_free;
            self.next_free += 1;
            id
        })
    }

    fn insert(&mut self, entry: T) -> u64 {
        let handle_id = self.get_free_id();
        self.used.insert(handle_id, entry);
        handle_id
    }

    fn remove(&mut self, handle_id: u64) {
        self.used.remove(&handle_id);
        self.free.push(handle_id);
    }
}

fn initialize_ctrlc_handler() -> (DropHookFn, Option<Receiver<()>>) {
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

    (drop_hook, rx_quitter)
}

pub trait Mountable {
    fn get_fsname() -> String;
    fn get_quitter_receiver_option_mut(&mut self) -> &mut Option<Receiver<()>>;

    fn mount(
        mut self,
        mountpoint: impl AsRef<Path>,
    ) -> Result<WaitableBackgroundSession, Box<dyn std::error::Error>>
    where
        Self: Filesystem + Send + Sized + 'static,
    {
        info!("mount: {:?}", mountpoint.as_ref());

        let rx_quitter = std::mem::take(self.get_quitter_receiver_option_mut()).unwrap();

        let options = vec![MountOption::RO, MountOption::FSName(Self::get_fsname())];

        let _session = fuser::spawn_mount2(self, &mountpoint, options.as_ref())?;
        Ok(WaitableBackgroundSession {
            _session,
            rx_quitter,
        })
    }
}

trait FilesystemShared {
    fn get_node_map(&self) -> &HashMap<u64, Node>;
    fn get_dir_handles(&self) -> &HandlePool<Vec<DirEntryAddArgs>>;
    fn get_dir_handles_mut(&mut self) -> &mut HandlePool<Vec<DirEntryAddArgs>>;

    fn create_attrs_for_file(&self, ino: u64, size: u64, mtime: SystemTime) -> FileAttr;
    fn create_attrs_for_dir(&self, ino: u64, nlink: u32) -> FileAttr;
    fn get_attr_from_ino(&self, ino: u64) -> Option<FileAttr>;

    fn get_ino_from_parent_and_name(&self, parent: u64, name: impl AsRef<OsStr>) -> Option<u64> {
        self.get_node_map()
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

    fn lookup(&mut self, parent: u64, name: &OsStr, reply: ReplyEntry) {
        info!("lookup: {:?}", (parent, name));

        if let Some(ino) = self.get_ino_from_parent_and_name(parent, name) {
            if let Some(attr) = self.get_attr_from_ino(ino) {
                reply.entry(&TTL, &attr, 0);
                return;
            }
        }

        reply.error(ENOENT);
    }

    fn getattr(&mut self, ino: u64, reply: ReplyAttr) {
        info!("getattr: {:?}", ino);

        if let Some(attr) = self.get_attr_from_ino(ino) {
            reply.attr(&TTL, &attr);
            return;
        }

        reply.error(ENOENT);
    }

    fn opendir(&mut self, ino: u64, reply: ReplyOpen) {
        info!("opendir: {}", ino);

        let Some(node) = self.get_node_map().get(&ino) else {
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
            if let Some(child_node) = self.get_node_map().get(&child_ino) {
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

        let fh = self.get_dir_handles_mut().insert(entries);
        reply.opened(fh, 0);
    }

    fn readdir(&mut self, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        info!("readdir: fh={fh}, offset={offset}");

        let entries = self.get_dir_handles().used.get(&fh).unwrap();
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

    fn releasedir(&mut self, fh: u64, reply: ReplyEmpty) {
        info!("releasedir: {}", fh);
        self.get_dir_handles_mut().remove(fh);
        reply.ok();
    }
}
