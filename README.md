# DedupeFS

[![badge github]][url github]
[![badge crates.io]][url crates.io]
[![badge docs.rs]][url docs.rs]
[![badge license]][url license]

[//]: # (@formatter:off)
[badge github]: https://img.shields.io/badge/github-FloGa%2Fdedupefs-green
[badge crates.io]: https://img.shields.io/crates/v/dedupefs
[badge docs.rs]: https://img.shields.io/docsrs/dedupefs
[badge license]: https://img.shields.io/crates/l/dedupefs

[url github]: https://github.com/FloGa/dedupefs
[url crates.io]: https://crates.io/crates/dedupefs
[url docs.rs]: https://docs.rs/dedupefs
[url license]: https://github.com/FloGa/dedupefs/blob/develop/LICENSE
[//]: # (@formatter:on)

> Presents files as deduplicated, content-addressed 1MB chunks with selectable hash algorithms.

*DedupeFS* is a FUSE filesystem over my [*Crazy Deduper*][crazy-deduper github] application. It is so to speak the
logical successor of [*SCFS*][scfs github]. While *SCFS* presented each file as chunks, independent of each other,
*DedupeFS* calculates the checksum of each chunk and collects them all in one directory. That way, each unique chunk is
only presented once, even if it is used by multiple files.

*DedupeFS* is mainly useful to create efficient backups and upload them to a cloud provider. The file chunks have the
advantage that the upload does not have to be all-or-nothing, so if your internet connection vanishes for a second, your
4GB file upload will not be completely cancelled, only the currently transferred chunk upload will be aborted.

By keeping multiple cache files around, you can easily and efficiently have incremental backups that all share the same
chunks.

[//]: # (@formatter:off)
[crazy-deduper github]: https://github.com/FloGa/crazy-deduper
[scfs github]: https://github.com/FloGa/scfs
[//]: # (@formatter:on)

## Installation

This tool can be installed easily through Cargo via `crates.io`:

```shell
cargo install --locked dedupefs
```

Please note that the `--locked` flag is necessary here to have the exact same dependencies as when the application was
tagged and tested. Without it, you might get more up-to-date versions of dependencies, but you have the risk of
undefined and unexpected behavior if the dependencies changed some functionalities. The application might even fail to
build if the public API of a dependency changed too much.

Alternatively, pre-built binaries can be downloaded from the [GitHub releases][gh-releases] page.

[gh-releases]: https://github.com/FloGa/dedupefs/releases

## Usage

<!--% !cargo --quiet run -- --help | tail -n+3 %-->

```text
```

To mount a deduped version of `source` directory to `deduped`, you can use:

```shell
dedupefs --cache-file cache.json.zst source deduped
```

If the cache file ends with `.zst`, it will be encoded (or decoded in the case of hydrating) using the ZSTD compression
algorithm. For any other extension, plain JSON will be used.

Please note that for now it is not possible to mount a hydrated version of a deduped directory. This wil be added in a
future version. For now, you can use [*Crazy Deduper*][crazy-deduper github] to physically re-hydrate your files.

## Cache Files

The cache file is necessary to keep track of all file chunks and hashes. Without the cache you would not be able to
restore your files.

The cache file can be re-used, even if the source directory changed. It keeps track of the file sizes and modification
times and only re-hashes new or changed files. Deleted files are deleted from the cache.

You can also use older cache files in addition to a new one:

```shell
dedupefs --cache-file cache.json.zst --cache-file cache-from-yesterday.json.zst source deduped
```

The cache files are read in reverse order in which they are given on the command line, so the content of earlier cache
files is preferred over later ones. Hence, you should put your most accurate cache files to the beginning. Moreover, the
first given cache file is the one that will be written to, it does not need to exist.

In the given example, if `cache.json.zst` does not exist, the internal cache is pre-filled from
`cache-from-yesterday.json.zst` so that only new and modified files need to be re-hashed. The result is then written
into `cache.json.zst`.

In the mounted deduped directory, the first cache file given on the command line will be presented with the same name
directly under the mountpoint. next to the data directory. When uploading your chunks, always make sure to also upload
this cache file, otherwise you wil not be able to properly re-hydrate your files afterward!

## TODO

- Support mounting a re-hydrated version of a deduped directory.
- Make declutter level configurable (fixed to 3 at the moment).
- Make chunk site configurable (via *Crazy Deduper*, fixed to 1MB at the moment).
- Provide better documentation with examples and use case descriptions.
