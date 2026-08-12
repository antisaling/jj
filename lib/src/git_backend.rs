// Copyright 2020 The Jujutsu Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![expect(missing_docs)]

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fmt::Error;
use std::fmt::Formatter;
use std::fs;
use std::io;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Command;
use std::process::ExitStatus;
use std::str::Utf8Error;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::AsyncReadExt as _;
use futures::StreamExt as _;
use futures::channel::oneshot;
use futures::io::Cursor;
use futures::stream::BoxStream;
use gix::bstr::BString;
use gix::objs::CommitRefIter;
use gix::objs::Write as _;
use gix::objs::WriteTo as _;
use gix::objs::commit::signature_field_name;
use itertools::Itertools as _;
use once_cell::sync::OnceCell as OnceLock;
use pollster::FutureExt as _;
use prost::Message as _;
#[cfg(not(target_vendor = "apple"))]
use sha1::Digest as _;
use smallvec::SmallVec;
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::backend::Backend;
use crate::backend::BackendError;
use crate::backend::BackendInitError;
use crate::backend::BackendLoadError;
use crate::backend::BackendResult;
use crate::backend::ChangeId;
use crate::backend::Commit;
use crate::backend::CommitId;
use crate::backend::CopyHistory;
use crate::backend::CopyId;
use crate::backend::CopyRecord;
use crate::backend::FileId;
use crate::backend::GcOptions;
use crate::backend::MillisSinceEpoch;
use crate::backend::RelatedCopy;
use crate::backend::SecureSig;
use crate::backend::Signature;
use crate::backend::SigningFn;
use crate::backend::SymlinkId;
use crate::backend::Timestamp;
use crate::backend::Tree;
use crate::backend::TreeId;
use crate::backend::TreeValue;
use crate::backend::make_root_commit;
use crate::config::ConfigGetError;
use crate::file_util;
use crate::file_util::BadPathEncoding;
use crate::file_util::IoResultExt as _;
use crate::file_util::PathError;
use crate::git::GitSettings;
use crate::index::Index;
use crate::lock::FileLock;
use crate::merge::Merge;
use crate::merge::MergeBuilder;
use crate::object_id::ObjectId;
use crate::repo_path::RepoPath;
use crate::repo_path::RepoPathBuf;
use crate::repo_path::RepoPathComponentBuf;
use crate::settings::UserSettings;
use crate::stacked_table::MutableTable;
use crate::stacked_table::ReadonlyTable;
use crate::stacked_table::TableSegment;
use crate::stacked_table::TableStore;
use crate::stacked_table::TableStoreError;

const CHANGE_ID_LENGTH: usize = 16;
const GIT_TREE_READ_CONCURRENCY: usize = 4;
const GIT_OBJECT_CACHE_SIZE: usize = 3 * 1024 * 1024 * 1024;
const GIT_OBJECT_CACHE_SHARDS: usize = 16;
const GIT_PACK_CACHE_SIZE: usize = 3 * 1024 * 1024 * 1024;
const GIT_PACK_CACHE_SHARDS: usize = 16;
/// Ref namespace used only for preventing GC.
const NO_GC_REF_NAMESPACE: &str = "refs/jj/keep/";

pub const JJ_CONFLICT_README_FILE_NAME: &str = "JJ-CONFLICT-README";

pub const JJ_TREES_COMMIT_HEADER: &str = "jj:trees";
pub const JJ_CONFLICT_LABELS_COMMIT_HEADER: &str = "jj:conflict-labels";
pub const CHANGE_ID_COMMIT_HEADER: &str = "change-id";

#[derive(Debug, Error)]
pub enum GitBackendInitError {
    #[error("Failed to initialize git repository")]
    InitRepository(#[source] gix::init::Error),
    #[error("Failed to open git repository")]
    OpenRepository(#[source] gix::open::Error),
    #[error("Failed to encode git repository path")]
    EncodeRepositoryPath(#[source] BadPathEncoding),
    #[error(transparent)]
    Config(ConfigGetError),
    #[error(transparent)]
    Path(PathError),
}

impl From<Box<GitBackendInitError>> for BackendInitError {
    fn from(err: Box<GitBackendInitError>) -> Self {
        Self(err)
    }
}

#[derive(Debug, Error)]
pub enum GitBackendLoadError {
    #[error("Failed to open git repository")]
    OpenRepository(#[source] gix::open::Error),
    #[error("Failed to decode git repository path")]
    DecodeRepositoryPath(#[source] BadPathEncoding),
    #[error(transparent)]
    Config(ConfigGetError),
    #[error(transparent)]
    Path(PathError),
}

impl From<Box<GitBackendLoadError>> for BackendLoadError {
    fn from(err: Box<GitBackendLoadError>) -> Self {
        Self(err)
    }
}

/// `GitBackend`-specific error that may occur after the backend is loaded.
#[derive(Debug, Error)]
pub enum GitBackendError {
    #[error("Failed to read non-git metadata")]
    ReadMetadata(#[source] TableStoreError),
    #[error("Failed to write non-git metadata")]
    WriteMetadata(#[source] TableStoreError),
}

impl From<GitBackendError> for BackendError {
    fn from(err: GitBackendError) -> Self {
        Self::Other(err.into())
    }
}

#[derive(Debug, Error)]
pub enum GitRepoAtWorkdirError {
    #[error("No Git repository found at {path}")]
    NotFound {
        path: PathBuf,
        source: gix::discover::is_git::Error,
    },
    #[error("Unrelated Git repository found at {path}")]
    Unrelated { path: PathBuf },
    #[error("Failed to open Git repository")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Error)]
pub enum GitGcError {
    #[error("Failed to run git gc command")]
    GcCommand(#[source] std::io::Error),
    #[error("git gc command exited with an error: {0}")]
    GcCommandErrorStatus(ExitStatus),
}

pub struct GitBackend {
    // While gix::Repository can be created from gix::ThreadSafeRepository, it's
    // cheaper to cache the thread-local instance behind a mutex than creating
    // one for each backend method call. Our GitBackend is most likely to be
    // used in a single-threaded context.
    base_repo: gix::ThreadSafeRepository,
    objects_dir: PathBuf,
    repo: Mutex<gix::Repository>,
    write_repos: Arc<GitWriteRepoPool>,
    read_repos: Arc<GitReadRepoPool>,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
    shallow_root_ids: OnceLock<Vec<CommitId>>,
    extra_metadata_store: Arc<TableStore>,
    cached_extra_metadata: Arc<Mutex<Option<Arc<ReadonlyTable>>>>,
    write_batch_state: Arc<Mutex<GitWriteBatchState>>,
    buffered_objects: Arc<Mutex<HashMap<gix::hash::ObjectId, Arc<BufferedGitObject>>>>,
    written_objects: Arc<GitObjectWriteTracker>,
    git_executable: PathBuf,
    write_change_id_header: bool,
}

struct GitReadRepoPool {
    // Slot 0 is for calls made outside Rayon. The remaining slots correspond
    // one-to-one with Rayon worker indexes, so concurrent readers don't fight
    // over a round-robin repository mutex.
    repos: Vec<Mutex<gix::Repository>>,
}

type GitObjectCacheShards = Arc<[Mutex<gix::odb::pack::cache::object::MemoryCappedHashmap>]>;
type GitPackCacheShards = Arc<[Mutex<gix::odb::pack::cache::lru::MemoryCappedHashmap>]>;

struct SharedGitObjectCache {
    shards: GitObjectCacheShards,
}

#[derive(Default)]
struct GitObjectWriteTracker {
    state: Mutex<GitObjectWriteState>,
    completed: Condvar,
}

#[derive(Default)]
struct GitObjectWriteState {
    written: HashSet<gix::hash::ObjectId>,
    in_flight: HashSet<gix::hash::ObjectId>,
}

struct BufferedGitObject {
    kind: gix::objs::Kind,
    data: Vec<u8>,
    compressed_data: Vec<u8>,
}

const MIN_BUFFERED_OBJECTS_PER_PACK: usize = 128;
const GIT_OBJECT_STORE_LOCK_FILE: &str = "jj-object-store.lock";

impl GitObjectWriteTracker {
    fn write_once(
        &self,
        oid: gix::hash::ObjectId,
        write: impl FnOnce() -> BackendResult<()>,
    ) -> BackendResult<()> {
        let mut state = self.state.lock().unwrap();
        loop {
            if state.written.contains(&oid) {
                return Ok(());
            }
            if state.in_flight.insert(oid) {
                break;
            }
            state = self.completed.wait(state).unwrap();
        }
        drop(state);

        let result = write();
        let mut state = self.state.lock().unwrap();
        state.in_flight.remove(&oid);
        if result.is_ok() {
            state.written.insert(oid);
        }
        self.completed.notify_all();
        result
    }
}

impl SharedGitObjectCache {
    fn shard(
        &self,
        id: &gix::hash::ObjectId,
    ) -> &Mutex<gix::odb::pack::cache::object::MemoryCappedHashmap> {
        &self.shards[usize::from(id.as_bytes()[0]) % self.shards.len()]
    }
}

impl gix::odb::pack::cache::Object for SharedGitObjectCache {
    fn put(&mut self, id: gix::hash::ObjectId, kind: gix::objs::Kind, data: &[u8]) {
        gix::odb::pack::cache::Object::put(&mut *self.shard(&id).lock().unwrap(), id, kind, data);
    }

    fn get(&mut self, id: &gix::hash::ObjectId, out: &mut Vec<u8>) -> Option<gix::objs::Kind> {
        gix::odb::pack::cache::Object::get(&mut *self.shard(id).lock().unwrap(), id, out)
    }
}

struct SharedGitPackCache {
    shards: GitPackCacheShards,
}

impl SharedGitPackCache {
    fn shard(
        &self,
        pack_id: u32,
        offset: u64,
    ) -> &Mutex<gix::odb::pack::cache::lru::MemoryCappedHashmap> {
        let hash = u64::from(pack_id).wrapping_mul(0x9e37_79b9) ^ offset;
        &self.shards[hash as usize % self.shards.len()]
    }
}

impl gix::odb::pack::cache::DecodeEntry for SharedGitPackCache {
    fn put(
        &mut self,
        pack_id: u32,
        offset: u64,
        data: &[u8],
        kind: gix::objs::Kind,
        compressed_size: usize,
    ) {
        gix::odb::pack::cache::DecodeEntry::put(
            &mut *self.shard(pack_id, offset).lock().unwrap(),
            pack_id,
            offset,
            data,
            kind,
            compressed_size,
        );
    }

    fn get(
        &mut self,
        pack_id: u32,
        offset: u64,
        out: &mut Vec<u8>,
    ) -> Option<(gix::objs::Kind, usize)> {
        gix::odb::pack::cache::DecodeEntry::get(
            &mut *self.shard(pack_id, offset).lock().unwrap(),
            pack_id,
            offset,
            out,
        )
    }
}

fn new_git_object_cache() -> GitObjectCacheShards {
    let shard_capacity = GIT_OBJECT_CACHE_SIZE.div_ceil(GIT_OBJECT_CACHE_SHARDS);
    (0..GIT_OBJECT_CACHE_SHARDS)
        .map(|_| {
            Mutex::new(gix::odb::pack::cache::object::MemoryCappedHashmap::new(
                shard_capacity,
            ))
        })
        .collect::<Vec<_>>()
        .into()
}

fn new_git_pack_cache() -> GitPackCacheShards {
    let shard_capacity = GIT_PACK_CACHE_SIZE.div_ceil(GIT_PACK_CACHE_SHARDS);
    (0..GIT_PACK_CACHE_SHARDS)
        .map(|_| {
            Mutex::new(gix::odb::pack::cache::lru::MemoryCappedHashmap::new(
                shard_capacity,
            ))
        })
        .collect::<Vec<_>>()
        .into()
}

fn set_git_object_cache(repo: &mut gix::Repository, shards: GitObjectCacheShards) {
    repo.objects.set_object_cache(move || {
        Box::new(SharedGitObjectCache {
            shards: shards.clone(),
        })
    });
}

fn set_git_pack_cache(repo: &mut gix::Repository, shards: GitPackCacheShards) {
    repo.objects.set_pack_cache(move || {
        Box::new(SharedGitPackCache {
            shards: shards.clone(),
        })
    });
}

struct GitWriteRepoPool {
    // Slot 0 is for calls made outside Rayon. The remaining slots correspond
    // one-to-one with Rayon worker indexes, so concurrent writers don't fight
    // over a round-robin repository mutex.
    repos: Vec<Mutex<gix::Repository>>,
}

struct GitTreeEntry<'a> {
    name: &'a [u8],
    mode: &'static [u8],
    oid: &'a [u8],
    is_tree: bool,
}

fn git_tree_entry_cmp(a: &GitTreeEntry<'_>, b: &GitTreeEntry<'_>) -> Ordering {
    let common = a.name.len().min(b.name.len());
    a.name[..common].cmp(&b.name[..common]).then_with(|| {
        let a = a.name.get(common).or_else(|| a.is_tree.then_some(&b'/'));
        let b = b.name.get(common).or_else(|| b.is_tree.then_some(&b'/'));
        a.cmp(&b)
    })
}

fn serialize_git_tree(mut entries: Vec<GitTreeEntry<'_>>) -> io::Result<Vec<u8>> {
    if !entries.is_sorted_by(|a, b| git_tree_entry_cmp(a, b) != Ordering::Greater) {
        entries.sort_unstable_by(git_tree_entry_cmp);
    }
    let size = entries
        .iter()
        .map(|entry| entry.mode.len() + 1 + entry.name.len() + 1 + entry.oid.len())
        .sum();
    let mut bytes = Vec::with_capacity(size);
    for entry in entries {
        if entry.name.contains(&0) {
            return Err(gix::objs::tree::write::Error::NullbyteInFilename {
                name: BString::from(entry.name),
            }
            .into());
        }
        bytes.extend_from_slice(entry.mode);
        bytes.push(b' ');
        bytes.extend_from_slice(entry.name);
        bytes.push(0);
        bytes.extend_from_slice(entry.oid);
    }
    Ok(bytes)
}

impl GitReadRepoPool {
    fn new(
        base_repo: &gix::ThreadSafeRepository,
        object_cache: GitObjectCacheShards,
        pack_cache: GitPackCacheShards,
    ) -> Self {
        let repos = (0..rayon::current_num_threads().max(1) + 1)
            .map(|_| {
                let mut repo = base_repo.to_thread_local();
                repo.objects.refresh_never();
                set_git_object_cache(&mut repo, object_cache.clone());
                set_git_pack_cache(&mut repo, pack_cache.clone());
                Mutex::new(repo)
            })
            .collect();
        Self { repos }
    }

    fn with_repo<T>(&self, f: impl FnOnce(&mut gix::Repository) -> T) -> T {
        let index = rayon::current_thread_index().map_or(0, |index| index + 1);
        let mut repo = self.repos[index].lock().unwrap();
        f(&mut repo)
    }
}

impl GitWriteRepoPool {
    fn new(base_repo: &gix::ThreadSafeRepository) -> Self {
        let repos = (0..rayon::current_num_threads().max(1) + 1)
            .map(|_| {
                let mut repo = base_repo.to_thread_local();
                repo.objects.refresh_never();
                Mutex::new(repo)
            })
            .collect();
        Self { repos }
    }

    fn with_repo<T>(&self, f: impl FnOnce(&mut gix::Repository) -> T) -> T {
        let index = rayon::current_thread_index().map_or(0, |index| index + 1);
        let mut repo = self.repos[index].lock().unwrap();
        f(&mut repo)
    }
}

#[derive(Default)]
struct GitWriteBatch {
    table: Option<MutableTable>,
    table_lock: Option<FileLock>,
    object_store_lock: Option<FileLock>,
    object_ids: Vec<gix::hash::ObjectId>,
    commit_ids: HashSet<CommitId>,
}

#[derive(Default)]
struct GitWriteBatchState {
    batch: Option<GitWriteBatch>,
    users: usize,
}

enum GitWriteTable<'a> {
    Batch(&'a mut MutableTable),
    Direct {
        table: Arc<ReadonlyTable>,
        table_lock: FileLock,
    },
}

impl GitWriteTable<'_> {
    fn get_value(&self, key: &[u8]) -> Option<&[u8]> {
        match self {
            Self::Batch(table) => table.get_value(key),
            Self::Direct { table, .. } => table.get_value(key),
        }
    }

    fn finish(self, backend: &GitBackend, key: Vec<u8>, value: Vec<u8>) -> BackendResult<()> {
        match self {
            Self::Batch(table) => {
                table.add_entry(key, value);
                Ok(())
            }
            Self::Direct { table, table_lock } => {
                let mut mut_table = table.start_mutation();
                mut_table.add_entry(key, value);
                backend.save_extra_metadata_table(mut_table, &table_lock)
            }
        }
    }
}

/// Keeps the extra metadata table lock across a group of commit writes.
pub struct GitWriteBatchGuard {
    state: Arc<Mutex<GitWriteBatchState>>,
    extra_metadata_store: Arc<TableStore>,
    cached_extra_metadata: Arc<Mutex<Option<Arc<ReadonlyTable>>>>,
    buffered_objects: Arc<Mutex<HashMap<gix::hash::ObjectId, Arc<BufferedGitObject>>>>,
    write_repos: Arc<GitWriteRepoPool>,
    objects_dir: PathBuf,
    git_repo_path: PathBuf,
    finished: bool,
}

impl GitWriteBatch {
    fn ensure_object_store_lock(&mut self, git_repo_path: &Path) -> BackendResult<()> {
        if self.object_store_lock.is_none() {
            self.object_store_lock = Some(
                FileLock::lock(git_repo_path.join(GIT_OBJECT_STORE_LOCK_FILE))
                    .map_err(|err| BackendError::Other(Box::new(err)))?,
            );
        }
        Ok(())
    }

    fn table<'a>(&'a mut self, backend: &GitBackend) -> BackendResult<&'a mut MutableTable> {
        if self.table.is_none() {
            let (table, table_lock) = backend.read_extra_metadata_table_locked()?;
            self.table = Some(table.start_mutation());
            self.table_lock = Some(table_lock);
        }
        Ok(self.table.as_mut().unwrap())
    }

    fn flush(
        &mut self,
        extra_metadata_store: &TableStore,
        cached_extra_metadata: &Mutex<Option<Arc<ReadonlyTable>>>,
        buffered_objects: &Mutex<HashMap<gix::hash::ObjectId, Arc<BufferedGitObject>>>,
        write_repos: &GitWriteRepoPool,
        objects_dir: &Path,
        git_repo_path: &Path,
    ) -> BackendResult<()> {
        if self.object_ids.is_empty() && self.commit_ids.is_empty() && self.table.is_none() {
            return Ok(());
        }
        self.ensure_object_store_lock(git_repo_path)?;
        flush_git_write_batch(
            &self.object_ids,
            &self.commit_ids,
            buffered_objects,
            write_repos,
            objects_dir,
        )?;
        if let Some(mut_table) = self.table.take() {
            let table_lock = self.table_lock.take().expect("metadata table lock missing");
            let table = extra_metadata_store
                .save_table(mut_table)
                .map_err(GitBackendError::WriteMetadata)?;
            *cached_extra_metadata.lock().unwrap() = Some(table);
            drop(table_lock);
        }
        self.object_ids.clear();
        self.commit_ids.clear();
        Ok(())
    }

    fn finish(
        mut self,
        extra_metadata_store: &TableStore,
        cached_extra_metadata: &Mutex<Option<Arc<ReadonlyTable>>>,
        buffered_objects: &Mutex<HashMap<gix::hash::ObjectId, Arc<BufferedGitObject>>>,
        write_repos: &GitWriteRepoPool,
        objects_dir: &Path,
        git_repo_path: &Path,
    ) -> BackendResult<()> {
        self.flush(
            extra_metadata_store,
            cached_extra_metadata,
            buffered_objects,
            write_repos,
            objects_dir,
            git_repo_path,
        )?;
        drop(self.object_store_lock.take());
        Ok(())
    }
}

struct GitHashWriter<W> {
    inner: W,
    hasher: Option<gix::hash::Hasher>,
}

impl<W: io::Write> GitHashWriter<W> {
    fn new(inner: W, hash_kind: gix::hash::Kind) -> Self {
        Self {
            inner,
            hasher: Some(gix::hash::hasher(hash_kind)),
        }
    }

    fn finish(mut self) -> io::Result<(W, gix::hash::ObjectId)> {
        let digest = self
            .hasher
            .take()
            .unwrap()
            .try_finalize()
            .map_err(io::Error::other)?;
        self.inner.write_all(digest.as_bytes())?;
        self.inner.flush()?;
        Ok((self.inner, digest))
    }
}

impl<W: io::Write> io::Write for GitHashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hasher.as_mut().unwrap().update(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn encode_pack_entry_header(kind: gix::objs::Kind, mut size: usize) -> ([u8; 16], usize) {
    let kind_bits = match kind {
        gix::objs::Kind::Commit => 1,
        gix::objs::Kind::Tree => 2,
        gix::objs::Kind::Blob => 3,
        gix::objs::Kind::Tag => 4,
    };
    let mut header = [0; 16];
    let mut len = 0;
    let mut byte = ((kind_bits << 4) | (size & 0xf)) as u8;
    size >>= 4;
    while size != 0 {
        header[len] = byte | 0x80;
        len += 1;
        byte = (size & 0x7f) as u8;
        size >>= 7;
    }
    header[len] = byte;
    len += 1;
    (header, len)
}

struct GitPackIndexEntry {
    id: gix::hash::ObjectId,
    offset: u64,
    crc32: u32,
}

fn write_git_pack(
    objects: &[(gix::hash::ObjectId, Arc<BufferedGitObject>)],
    output: &mut fs::File,
) -> io::Result<(gix::hash::ObjectId, Vec<GitPackIndexEntry>)> {
    let object_count = u32::try_from(objects.len())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let hash_kind = objects[0].0.kind();
    if objects.iter().any(|(id, _)| id.kind() != hash_kind) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "pack objects use different hash kinds",
        ));
    }

    let mut writer = GitHashWriter::new(io::BufWriter::new(output), hash_kind);
    writer.write_all(b"PACK")?;
    writer.write_all(&2_u32.to_be_bytes())?;
    writer.write_all(&object_count.to_be_bytes())?;
    let mut offset = 12_u64;
    let mut entries = Vec::with_capacity(objects.len());
    for (id, object) in objects {
        let (header, header_len) = encode_pack_entry_header(object.kind, object.data.len());
        let header = &header[..header_len];
        let crc32 = gix::features::hash::crc32_update(
            gix::features::hash::crc32(header),
            &object.compressed_data,
        );
        writer.write_all(header)?;
        writer.write_all(&object.compressed_data)?;
        entries.push(GitPackIndexEntry {
            id: *id,
            offset,
            crc32,
        });
        offset = offset
            .checked_add(header.len() as u64)
            .and_then(|offset| offset.checked_add(object.compressed_data.len() as u64))
            .ok_or_else(|| io::Error::other("pack offset overflow"))?;
    }
    let (_, pack_hash) = writer.finish()?;
    Ok((pack_hash, entries))
}

fn write_git_pack_index(
    mut entries: Vec<GitPackIndexEntry>,
    pack_hash: &gix::hash::ObjectId,
    output: &mut fs::File,
) -> io::Result<()> {
    const V2_SIGNATURE: &[u8; 4] = b"\xfftOc";
    const LARGE_OFFSET_THRESHOLD: u64 = 0x7fff_ffff;
    const HIGH_BIT: u32 = 0x8000_0000;

    entries.sort_unstable_by_key(|entry| entry.id);
    let mut fanout = [0_u32; 256];
    for entry in &entries {
        let first_byte = usize::from(entry.id.as_bytes()[0]);
        fanout[first_byte] = fanout[first_byte]
            .checked_add(1)
            .ok_or_else(|| io::Error::other("pack fanout overflow"))?;
    }
    for i in 1..fanout.len() {
        fanout[i] = fanout[i]
            .checked_add(fanout[i - 1])
            .ok_or_else(|| io::Error::other("pack fanout overflow"))?;
    }

    let mut writer = GitHashWriter::new(io::BufWriter::new(output), pack_hash.kind());
    writer.write_all(V2_SIGNATURE)?;
    writer.write_all(&2_u32.to_be_bytes())?;
    for value in fanout {
        writer.write_all(&value.to_be_bytes())?;
    }
    for entry in &entries {
        writer.write_all(entry.id.as_bytes())?;
    }
    for entry in &entries {
        writer.write_all(&entry.crc32.to_be_bytes())?;
    }
    let mut large_offsets = Vec::new();
    for entry in &entries {
        let encoded_offset = if entry.offset > LARGE_OFFSET_THRESHOLD {
            let index = u32::try_from(large_offsets.len())
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
            if index >= HIGH_BIT {
                return Err(io::Error::other("too many large pack offsets"));
            }
            large_offsets.push(entry.offset);
            index | HIGH_BIT
        } else {
            entry.offset as u32
        };
        writer.write_all(&encoded_offset.to_be_bytes())?;
    }
    for offset in large_offsets {
        writer.write_all(&offset.to_be_bytes())?;
    }
    writer.write_all(pack_hash.as_bytes())?;
    writer.finish()?;
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    fs::File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn write_buffered_git_pack(
    objects: &[(gix::hash::ObjectId, Arc<BufferedGitObject>)],
    objects_dir: &Path,
) -> BackendResult<()> {
    if objects.is_empty() {
        return Ok(());
    }
    let result = (|| -> io::Result<()> {
        let pack_dir = objects_dir.join("pack");
        fs::create_dir_all(&pack_dir)?;
        let mut pack_temp = NamedTempFile::new_in(&pack_dir)?;
        let (pack_hash, entries) = write_git_pack(objects, pack_temp.as_file_mut())?;
        let mut index_temp = NamedTempFile::new_in(&pack_dir)?;
        write_git_pack_index(entries, &pack_hash, index_temp.as_file_mut())?;

        let basename = format!("pack-{pack_hash}");
        let pack_path = pack_dir.join(format!("{basename}.pack"));
        let index_path = pack_dir.join(format!("{basename}.idx"));
        file_util::persist_content_addressed_temp_file(pack_temp, pack_path)?;
        // Make the pack durable before publishing the index that makes it visible.
        sync_directory(&pack_dir)?;
        file_util::persist_content_addressed_temp_file(index_temp, index_path)?;
        sync_directory(&pack_dir)
    })();
    result.map_err(|err| BackendError::WriteObject {
        object_type: "pack",
        source: Box::new(err),
    })
}

fn flush_buffered_git_objects(
    object_ids: &[gix::hash::ObjectId],
    buffered_objects: &Mutex<HashMap<gix::hash::ObjectId, Arc<BufferedGitObject>>>,
    write_repos: &GitWriteRepoPool,
    objects_dir: &Path,
) -> BackendResult<()> {
    let objects = {
        let buffered_objects = buffered_objects.lock().unwrap();
        object_ids
            .iter()
            .filter_map(|id| buffered_objects.get(id).map(|object| (*id, object.clone())))
            .collect_vec()
    };
    if objects.len() < MIN_BUFFERED_OBJECTS_PER_PACK {
        return write_buffered_git_objects_loose(&objects, write_repos);
    }
    if let Err(pack_err) = write_buffered_git_pack(&objects, objects_dir) {
        // Preserve correctness if the batch writer fails. This is slower, but it
        // leaves every object reachable by the already-committed operation in the ODB.
        tracing::warn!(
            ?pack_err,
            "failed to write Git object pack; falling back to loose objects"
        );
        write_buffered_git_objects_loose(&objects, write_repos)?;
    }
    Ok(())
}

fn write_buffered_git_objects_loose(
    objects: &[(gix::hash::ObjectId, Arc<BufferedGitObject>)],
    write_repos: &GitWriteRepoPool,
) -> BackendResult<()> {
    for (id, object) in objects {
        write_repos.with_repo(|repo| {
            write_object_with_known_id(
                repo,
                object.kind,
                &object.data,
                *id,
                object_kind_name(object.kind),
            )
        })?;
    }
    Ok(())
}

fn flush_git_write_batch(
    object_ids: &[gix::hash::ObjectId],
    commit_ids: &HashSet<CommitId>,
    buffered_objects: &Mutex<HashMap<gix::hash::ObjectId, Arc<BufferedGitObject>>>,
    write_repos: &GitWriteRepoPool,
    objects_dir: &Path,
) -> BackendResult<()> {
    flush_buffered_git_objects(object_ids, buffered_objects, write_repos, objects_dir)?;
    if !commit_ids.is_empty() {
        write_repos.with_repo(|repo| {
            repo.edit_references(commit_ids.iter().map(to_no_gc_ref_update))
                .map_err(|err| BackendError::Other(Box::new(err)))
        })?;
    }
    Ok(())
}

fn object_kind_name(kind: gix::objs::Kind) -> &'static str {
    match kind {
        gix::objs::Kind::Commit => "commit",
        gix::objs::Kind::Tree => "tree",
        gix::objs::Kind::Blob => "blob",
        gix::objs::Kind::Tag => "tag",
    }
}

impl GitWriteBatchGuard {
    /// Makes the current batch durable without releasing the command-scoped
    /// object-store lock. This must happen before publishing an operation that
    /// references objects from the batch.
    pub fn flush(&mut self) -> BackendResult<()> {
        if self.finished {
            return Ok(());
        }
        flush_active_git_write_batch(
            &self.state,
            &self.extra_metadata_store,
            &self.cached_extra_metadata,
            &self.buffered_objects,
            &self.write_repos,
            &self.objects_dir,
            &self.git_repo_path,
        )
    }

    fn finish_inner(&mut self) -> BackendResult<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        let batch = {
            let mut state = self.state.lock().unwrap();
            state.users -= 1;
            if state.users == 0 {
                state.batch.take()
            } else {
                None
            }
        };
        if let Some(batch) = batch {
            batch.finish(
                &self.extra_metadata_store,
                &self.cached_extra_metadata,
                &self.buffered_objects,
                &self.write_repos,
                &self.objects_dir,
                &self.git_repo_path,
            )?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> BackendResult<()> {
        self.finish_inner()
    }
}

fn flush_active_git_write_batch(
    state: &Mutex<GitWriteBatchState>,
    extra_metadata_store: &TableStore,
    cached_extra_metadata: &Mutex<Option<Arc<ReadonlyTable>>>,
    buffered_objects: &Mutex<HashMap<gix::hash::ObjectId, Arc<BufferedGitObject>>>,
    write_repos: &GitWriteRepoPool,
    objects_dir: &Path,
    git_repo_path: &Path,
) -> BackendResult<()> {
    let mut state = state.lock().unwrap();
    let Some(batch) = state.batch.as_mut() else {
        return Ok(());
    };
    batch.flush(
        extra_metadata_store,
        cached_extra_metadata,
        buffered_objects,
        write_repos,
        objects_dir,
        git_repo_path,
    )
}

impl Drop for GitWriteBatchGuard {
    fn drop(&mut self) {
        if let Err(err) = self.finish_inner() {
            tracing::error!(?err, "Failed to flush Git extra metadata write batch");
        }
    }
}

impl GitBackend {
    pub fn name() -> &'static str {
        "git"
    }

    fn new(
        base_repo: gix::ThreadSafeRepository,
        extra_metadata_store: TableStore,
        git_settings: GitSettings,
    ) -> Self {
        let objects_dir = base_repo.objects_dir().to_owned();
        let mut repo = base_repo.to_thread_local();
        // Rebase writes many objects that are intentionally not in the ODB yet.
        // Don't rescan the ODB from disk after every expected miss.
        repo.objects.refresh_never();
        let object_cache = new_git_object_cache();
        let pack_cache = new_git_pack_cache();
        set_git_object_cache(&mut repo, object_cache.clone());
        set_git_pack_cache(&mut repo, pack_cache.clone());
        let root_commit_id = CommitId::from_bytes(repo.object_hash().null_ref().as_bytes());
        let root_change_id = ChangeId::from_bytes(&[0; CHANGE_ID_LENGTH]);
        let empty_tree_id =
            TreeId::from_bytes(gix::ObjectId::empty_tree(repo.object_hash()).as_bytes());
        let repo = Mutex::new(repo);
        let write_repos = Arc::new(GitWriteRepoPool::new(&base_repo));
        let read_repos = Arc::new(GitReadRepoPool::new(&base_repo, object_cache, pack_cache));
        Self {
            base_repo,
            objects_dir,
            repo,
            write_repos,
            read_repos,
            root_commit_id,
            root_change_id,
            empty_tree_id,
            shallow_root_ids: OnceLock::new(),
            extra_metadata_store: Arc::new(extra_metadata_store),
            cached_extra_metadata: Arc::new(Mutex::new(None)),
            write_batch_state: Arc::new(Mutex::new(GitWriteBatchState::default())),
            buffered_objects: Arc::new(Mutex::new(HashMap::new())),
            written_objects: Arc::new(GitObjectWriteTracker::default()),
            git_executable: git_settings.executable_path,
            write_change_id_header: git_settings.write_change_id_header,
        }
    }

    pub fn start_write_batch(&self) -> GitWriteBatchGuard {
        let mut state = self.write_batch_state.lock().unwrap();
        if state.users == 0 {
            debug_assert!(state.batch.is_none());
            state.batch = Some(GitWriteBatch::default());
        }
        state.users += 1;
        GitWriteBatchGuard {
            state: self.write_batch_state.clone(),
            extra_metadata_store: self.extra_metadata_store.clone(),
            cached_extra_metadata: self.cached_extra_metadata.clone(),
            buffered_objects: self.buffered_objects.clone(),
            write_repos: self.write_repos.clone(),
            objects_dir: self.objects_dir.clone(),
            git_repo_path: self.git_repo_path().to_owned(),
            finished: false,
        }
    }

    fn git_object_store_lock_path(&self) -> PathBuf {
        self.git_repo_path().join(GIT_OBJECT_STORE_LOCK_FILE)
    }

    fn flush_active_write_batch(&self) -> BackendResult<()> {
        flush_active_git_write_batch(
            &self.write_batch_state,
            &self.extra_metadata_store,
            &self.cached_extra_metadata,
            &self.buffered_objects,
            &self.write_repos,
            &self.objects_dir,
            self.git_repo_path(),
        )
    }

    pub fn init_internal(
        settings: &UserSettings,
        store_path: &Path,
        object_hash: gix::hash::Kind,
    ) -> Result<Self, Box<GitBackendInitError>> {
        let git_repo_path = Path::new("git");
        let git_repo = gix::ThreadSafeRepository::init_opts(
            store_path.join(git_repo_path),
            gix::create::Kind::Bare,
            gix::create::Options {
                object_hash: Some(object_hash),
                ..Default::default()
            },
            gix_open_opts_from_settings(settings),
        )
        .map_err(GitBackendInitError::InitRepository)?;
        let git_settings =
            GitSettings::from_settings(settings).map_err(GitBackendInitError::Config)?;
        Self::init_with_repo(store_path, git_repo_path, git_repo, git_settings)
    }

    /// Initializes backend by creating a new Git repo at the specified
    /// workspace path. The workspace directory must exist.
    pub fn init_colocated(
        settings: &UserSettings,
        store_path: &Path,
        workspace_root: &Path,
        object_hash: gix::hash::Kind,
    ) -> Result<Self, Box<GitBackendInitError>> {
        let canonical_workspace_root = {
            let path = store_path.join(workspace_root);
            dunce::canonicalize(&path)
                .context(&path)
                .map_err(GitBackendInitError::Path)?
        };
        let git_repo = gix::ThreadSafeRepository::init_opts(
            canonical_workspace_root,
            gix::create::Kind::WithWorktree,
            gix::create::Options {
                object_hash: Some(object_hash),
                ..Default::default()
            },
            gix_open_opts_from_settings(settings),
        )
        .map_err(GitBackendInitError::InitRepository)?;
        let git_repo_path = workspace_root.join(".git");
        let git_settings =
            GitSettings::from_settings(settings).map_err(GitBackendInitError::Config)?;
        Self::init_with_repo(store_path, &git_repo_path, git_repo, git_settings)
    }

    /// Initializes backend with an existing Git repo at the specified path.
    pub fn init_external(
        settings: &UserSettings,
        store_path: &Path,
        git_repo_path: &Path,
    ) -> Result<Self, Box<GitBackendInitError>> {
        let canonical_git_repo_path = {
            let path = store_path.join(git_repo_path);
            canonicalize_git_repo_path(&path)
                .context(&path)
                .map_err(GitBackendInitError::Path)?
        };
        let git_repo = gix::ThreadSafeRepository::open_opts(
            canonical_git_repo_path,
            gix_open_opts_from_settings(settings),
        )
        .map_err(GitBackendInitError::OpenRepository)?;
        let git_settings =
            GitSettings::from_settings(settings).map_err(GitBackendInitError::Config)?;
        Self::init_with_repo(store_path, git_repo_path, git_repo, git_settings)
    }

    fn init_with_repo(
        store_path: &Path,
        git_repo_path: &Path,
        repo: gix::ThreadSafeRepository,
        git_settings: GitSettings,
    ) -> Result<Self, Box<GitBackendInitError>> {
        let extra_path = store_path.join("extra");
        fs::create_dir(&extra_path)
            .context(&extra_path)
            .map_err(GitBackendInitError::Path)?;
        let target_path = store_path.join("git_target");
        let git_repo_path = if cfg!(windows) && git_repo_path.is_relative() {
            // When a repository is created in Windows, format the path with *forward
            // slashes* and not backwards slashes. This makes it possible to use the same
            // repository under Windows Subsystem for Linux.
            //
            // This only works for relative paths. If the path is absolute, there's not much
            // we can do, and it simply won't work inside and outside WSL at the same time.
            file_util::slash_path(git_repo_path)
        } else {
            git_repo_path.into()
        };
        let git_repo_path_bytes = file_util::path_to_bytes(&git_repo_path)
            .map_err(GitBackendInitError::EncodeRepositoryPath)?;
        fs::write(&target_path, git_repo_path_bytes)
            .context(&target_path)
            .map_err(GitBackendInitError::Path)?;
        let extra_metadata_store = TableStore::init(
            extra_path,
            repo.to_thread_local().object_hash().len_in_bytes(),
        );
        Ok(Self::new(repo, extra_metadata_store, git_settings))
    }

    pub fn load(
        settings: &UserSettings,
        store_path: &Path,
    ) -> Result<Self, Box<GitBackendLoadError>> {
        let git_repo_path = {
            let target_path = store_path.join("git_target");
            let git_repo_path_bytes = fs::read(&target_path)
                .context(&target_path)
                .map_err(GitBackendLoadError::Path)?;
            let git_repo_path = file_util::path_from_bytes(&git_repo_path_bytes)
                .map_err(GitBackendLoadError::DecodeRepositoryPath)?;
            let git_repo_path = store_path.join(git_repo_path);
            canonicalize_git_repo_path(&git_repo_path)
                .context(&git_repo_path)
                .map_err(GitBackendLoadError::Path)?
        };
        let repo = gix::ThreadSafeRepository::open_opts(
            git_repo_path,
            gix_open_opts_from_settings(settings),
        )
        .map_err(GitBackendLoadError::OpenRepository)?;
        let extra_metadata_store = TableStore::load(
            store_path.join("extra"),
            repo.to_thread_local().object_hash().len_in_bytes(),
        );
        let git_settings =
            GitSettings::from_settings(settings).map_err(GitBackendLoadError::Config)?;
        Ok(Self::new(repo, extra_metadata_store, git_settings))
    }

    fn lock_git_repo(&self) -> MutexGuard<'_, gix::Repository> {
        self.repo.lock().unwrap()
    }

    /// Returns a new thread-local handle for the underlying Git repository.
    ///
    /// Use [`Self::open_git_repo_at_workdir()`] for worktree operations.
    pub fn git_repo(&self) -> gix::Repository {
        self.base_repo.to_thread_local()
    }

    /// Reopens the repository at the given workspace path. Returns a new
    /// thread-local handle.
    pub fn open_git_repo_at_workdir(
        &self,
        path: &Path,
    ) -> Result<gix::Repository, GitRepoAtWorkdirError> {
        // Try the open repository first.
        let open_repo = self.git_repo();
        if let Some(workdir) = open_repo.workdir()
            && (workdir == path || dunce::canonicalize(path).is_ok_and(|path| workdir == path))
        {
            return Ok(open_repo);
        }

        // The input path doesn't include ".git".
        let opts = open_repo.open_options().clone().open_path_as_is(false);
        let work_repo = gix::ThreadSafeRepository::open_opts(path, opts)
            .map_err(|err| match err {
                gix::open::Error::NotARepository { path, source } => {
                    GitRepoAtWorkdirError::NotFound { path, source }
                }
                err => GitRepoAtWorkdirError::Other(err.into()),
            })?
            .to_thread_local();
        let canonicalize = |path: &Path| {
            dunce::canonicalize(path).map_err(|err| GitRepoAtWorkdirError::Other(err.into()))
        };
        if open_repo.common_dir() == work_repo.common_dir()
            || canonicalize(open_repo.common_dir())? == canonicalize(work_repo.common_dir())?
        {
            // The last (path, work_repo) can be cached if needed.
            Ok(work_repo)
        } else {
            let path = path.to_owned();
            Err(GitRepoAtWorkdirError::Unrelated { path })
        }
    }

    /// Path to the `.git` directory or the repository itself if it's bare.
    pub fn git_repo_path(&self) -> &Path {
        self.base_repo.path()
    }

    fn shallow_root_ids(&self, git_repo: &gix::Repository) -> BackendResult<&[CommitId]> {
        // The list of shallow roots is cached by gix, but it's still expensive
        // to stat file on every read_object() call. Refreshing shallow roots is
        // also bad for consistency reasons.
        self.shallow_root_ids
            .get_or_try_init(|| {
                let maybe_oids = git_repo
                    .shallow_commits()
                    .map_err(|err| BackendError::Other(err.into()))?;
                let commit_ids = maybe_oids.map_or(vec![], |oids| {
                    oids.iter()
                        .map(|oid| CommitId::from_bytes(oid.as_bytes()))
                        .collect()
                });
                Ok(commit_ids)
            })
            .map(AsRef::as_ref)
    }

    fn cached_extra_metadata_table(&self) -> BackendResult<Arc<ReadonlyTable>> {
        let mut locked_head = self.cached_extra_metadata.lock().unwrap();
        match locked_head.as_ref() {
            Some(head) => Ok(head.clone()),
            None => {
                let table = self
                    .extra_metadata_store
                    .get_head()
                    .map_err(GitBackendError::ReadMetadata)?;
                *locked_head = Some(table.clone());
                Ok(table)
            }
        }
    }

    fn extra_metadata_for_commit(&self, id: &CommitId) -> BackendResult<Option<Vec<u8>>> {
        let pending = self
            .write_batch_state
            .lock()
            .unwrap()
            .batch
            .as_ref()
            .and_then(|batch| batch.table.as_ref())
            .and_then(|table| table.get_value(id.as_bytes()))
            .map(<[u8]>::to_vec);
        if pending.is_some() {
            return Ok(pending);
        }
        let table = self.cached_extra_metadata_table()?;
        Ok(table.get_value(id.as_bytes()).map(<[u8]>::to_vec))
    }

    fn read_extra_metadata_table_locked(&self) -> BackendResult<(Arc<ReadonlyTable>, FileLock)> {
        let table = self
            .extra_metadata_store
            .get_head_locked()
            .map_err(GitBackendError::ReadMetadata)?;
        Ok(table)
    }

    fn save_extra_metadata_table(
        &self,
        mut_table: MutableTable,
        _table_lock: &FileLock,
    ) -> BackendResult<()> {
        let table = self
            .extra_metadata_store
            .save_table(mut_table)
            .map_err(GitBackendError::WriteMetadata)?;
        // Since the parent table was the head, saved table are likely to be new head.
        // If it's not, cache will be reloaded when entry can't be found.
        *self.cached_extra_metadata.lock().unwrap() = Some(table);
        Ok(())
    }

    /// Imports the given commits and ancestors from the backing Git repo.
    ///
    /// The `head_ids` may contain commits that have already been imported, but
    /// the caller should filter them out to eliminate redundant I/O processing.
    #[tracing::instrument(skip(self, head_ids))]
    pub fn import_head_commits<'a>(
        &self,
        head_ids: impl IntoIterator<Item = &'a CommitId>,
    ) -> BackendResult<()> {
        let head_ids: HashSet<&CommitId> = head_ids
            .into_iter()
            .filter(|&id| *id != self.root_commit_id)
            .collect();
        if head_ids.is_empty() {
            return Ok(());
        }

        // Keep the lock order consistent with write_commit(): Git repository,
        // then the extra metadata table.
        let locked_repo = self.lock_git_repo();
        let mut write_batch_state = self.write_batch_state.lock().unwrap();
        if let Some(batch) = write_batch_state.batch.as_mut() {
            // Reuse the command-scoped table and lock if a read discovers a Git
            // commit that hasn't been imported yet.
            batch.ensure_object_store_lock(self.git_repo_path())?;
            batch
                .commit_ids
                .extend(head_ids.iter().map(|id| (*id).clone()));
            let table = batch.table(self)?;
            import_extra_metadata_entries_from_heads(
                &locked_repo,
                table,
                &head_ids,
                self.shallow_root_ids(&locked_repo)?,
            )?;
            return Ok(());
        }
        drop(write_batch_state);

        // Create no-gc ref even if known to the extras table. Concurrent GC
        // process might have deleted the no-gc ref.
        locked_repo
            .edit_references(head_ids.iter().copied().map(to_no_gc_ref_update))
            .map_err(|err| BackendError::Other(Box::new(err)))?;

        // These commits are imported from Git. Make our change ids persist (otherwise
        // future write_commit() could reassign new change id.)
        tracing::debug!(
            heads_count = head_ids.len(),
            "import extra metadata entries"
        );
        let (table, table_lock) = self.read_extra_metadata_table_locked()?;
        let mut mut_table = table.start_mutation();
        import_extra_metadata_entries_from_heads(
            &locked_repo,
            &mut mut_table,
            &head_ids,
            self.shallow_root_ids(&locked_repo)?,
        )?;
        self.save_extra_metadata_table(mut_table, &table_lock)
    }

    fn read_file_sync(&self, id: &FileId) -> BackendResult<Vec<u8>> {
        let git_blob_id = validate_git_object_id_kind(self.base_repo.objects.object_hash(), id)?;
        let buffered_object = self
            .buffered_objects
            .lock()
            .unwrap()
            .get(&git_blob_id)
            .cloned();
        if let Some(object) = buffered_object {
            if object.kind != gix::objs::Kind::Blob {
                return Err(to_read_object_err(
                    io::Error::other(format!(
                        "expected blob, found {}",
                        object_kind_name(object.kind)
                    )),
                    id,
                ));
            }
            return Ok(object.data.clone());
        }
        let locked_repo = self.lock_git_repo();
        let mut blob = locked_repo
            .find_object(git_blob_id)
            .map_err(|err| map_not_found_err(err, id))?
            .try_into_blob()
            .map_err(|err| to_read_object_err(err, id))?;
        Ok(blob.take_data())
    }

    fn new_diff_platform(&self) -> BackendResult<gix::diff::blob::Platform> {
        let attributes = gix::worktree::Stack::new(
            Path::new(""),
            gix::worktree::stack::State::AttributesStack(Default::default()),
            gix::worktree::glob::pattern::Case::Sensitive,
            Vec::new(),
            Vec::new(),
        );
        let filter = gix::diff::blob::Pipeline::new(
            Default::default(),
            gix::filter::plumbing::Pipeline::new(
                self.git_repo()
                    .command_context()
                    .map_err(|err| BackendError::Other(Box::new(err)))?,
                Default::default(),
            ),
            Vec::new(),
            Default::default(),
        );
        Ok(gix::diff::blob::Platform::new(
            Default::default(),
            filter,
            gix::diff::blob::pipeline::Mode::ToGit,
            attributes,
        ))
    }

    fn read_tree_for_commit<'repo>(
        &self,
        repo: &'repo gix::Repository,
        id: &CommitId,
    ) -> BackendResult<gix::Tree<'repo>> {
        let tree = self.read_commit(id).block_on()?.root_tree;
        // TODO(kfm): probably want to do something here if it is a merge
        let tree_id = tree.first().clone();
        let gix_id = validate_git_object_id(repo, &tree_id)?;
        repo.find_object(gix_id)
            .map_err(|err| map_not_found_err(err, &tree_id))?
            .try_into_tree()
            .map_err(|err| to_read_object_err(err, &tree_id))
    }

    // Similar to gix's write_blob, but compute the hash outside our lock to
    // reduce contention and pass the known ID to avoid hashing again.
    fn write_blob(
        &self,
        bytes: &[u8],
        object_type: &'static str,
    ) -> BackendResult<gix::hash::ObjectId> {
        self.write_object_bytes(gix::objs::Kind::Blob, bytes.to_vec(), object_type)
    }

    fn write_object_bytes(
        &self,
        kind: gix::objs::Kind,
        bytes: Vec<u8>,
        object_type: &'static str,
    ) -> BackendResult<gix::hash::ObjectId> {
        let oid = gix::objs::compute_hash(self.base_repo.objects.object_hash(), kind, &bytes)
            .map_err(|err| BackendError::WriteObject {
                object_type,
                source: Box::new(err),
            })?;

        write_or_buffer_object_on_pool(
            &self.objects_dir,
            &self.written_objects,
            &self.write_repos,
            &self.write_batch_state,
            &self.buffered_objects,
            kind,
            bytes,
            oid,
            object_type,
        )?;
        Ok(oid)
    }
}

/// Computes a Git SHA-1 without collision detection.
///
/// This is only used for tree objects synthesized from parsed [`Tree`] values. Blobs and
/// commits can contain arbitrary input and continue to use gix's collision-checking hasher.
fn compute_synthesized_tree_hash(bytes: &[u8]) -> gix::hash::ObjectId {
    #[cfg(target_vendor = "apple")]
    {
        compute_synthesized_tree_hash_common_crypto(bytes)
    }

    #[cfg(not(target_vendor = "apple"))]
    {
        let mut hasher = sha1::Sha1::new();
        hasher.update(gix::objs::encode::loose_header(
            gix::objs::Kind::Tree,
            bytes.len() as u64,
        ));
        hasher.update(bytes);
        gix::hash::ObjectId::from_bytes_or_panic(&hasher.finalize())
    }
}

#[cfg(target_vendor = "apple")]
#[allow(unsafe_code)]
fn compute_synthesized_tree_hash_common_crypto(bytes: &[u8]) -> gix::hash::ObjectId {
    #[repr(C)]
    #[derive(Default)]
    struct Sha1Context {
        state: [u32; 5],
        bit_count: [u32; 2],
        data: [u32; 16],
        buffered_len: std::ffi::c_int,
    }

    #[link(name = "System", kind = "dylib")]
    unsafe extern "C" {
        #[link_name = "CC_SHA1_Init"]
        fn cc_sha1_init(context: *mut Sha1Context) -> std::ffi::c_int;
        #[link_name = "CC_SHA1_Update"]
        fn cc_sha1_update(
            context: *mut Sha1Context,
            data: *const std::ffi::c_void,
            len: u32,
        ) -> std::ffi::c_int;
        #[link_name = "CC_SHA1_Final"]
        fn cc_sha1_final(digest: *mut u8, context: *mut Sha1Context) -> std::ffi::c_int;
    }

    fn update(context: &mut Sha1Context, bytes: &[u8]) {
        for chunk in bytes.chunks(u32::MAX as usize) {
            // SAFETY: `context` and `chunk` remain valid for the duration of the call.
            let success =
                unsafe { cc_sha1_update(context, chunk.as_ptr().cast(), chunk.len() as u32) };
            assert_eq!(success, 1);
        }
    }

    let mut context = Sha1Context::default();
    // SAFETY: `context` points to writable storage with the layout declared by CommonCrypto.
    assert_eq!(unsafe { cc_sha1_init(&raw mut context) }, 1);
    update(
        &mut context,
        &gix::objs::encode::loose_header(gix::objs::Kind::Tree, bytes.len() as u64),
    );
    update(&mut context, bytes);

    let mut digest = [0; 20];
    // SAFETY: `digest` has SHA-1's required 20 bytes and `context` is initialized.
    assert_eq!(
        unsafe { cc_sha1_final(digest.as_mut_ptr(), &raw mut context) },
        1
    );
    gix::hash::ObjectId::from_bytes_or_panic(&digest)
}

fn compress_git_object_data(bytes: &[u8]) -> io::Result<Vec<u8>> {
    let mut writer =
        gix::zlib::stream::deflate::Write::new(Vec::new(), gix::zlib::Compression::DEFAULT);
    writer.write_all(bytes)?;
    writer.flush()?;
    Ok(writer.into_inner())
}

fn buffer_git_object(
    batch: &mut GitWriteBatch,
    buffered_objects: &Mutex<HashMap<gix::hash::ObjectId, Arc<BufferedGitObject>>>,
    kind: gix::objs::Kind,
    bytes: Vec<u8>,
    compressed_data: Vec<u8>,
    oid: gix::hash::ObjectId,
) -> BackendResult<()> {
    let mut objects = buffered_objects.lock().unwrap();
    if objects.contains_key(&oid) {
        return Ok(());
    }
    let object = Arc::new(BufferedGitObject {
        kind,
        data: bytes,
        compressed_data,
    });
    objects.insert(oid, object);
    batch.object_ids.push(oid);
    Ok(())
}

#[expect(clippy::too_many_arguments)]
fn write_or_buffer_object_on_pool(
    objects_dir: &Path,
    written_objects: &GitObjectWriteTracker,
    write_repos: &GitWriteRepoPool,
    write_batch_state: &Mutex<GitWriteBatchState>,
    buffered_objects: &Mutex<HashMap<gix::hash::ObjectId, Arc<BufferedGitObject>>>,
    kind: gix::objs::Kind,
    bytes: Vec<u8>,
    oid: gix::hash::ObjectId,
    object_type: &'static str,
) -> BackendResult<()> {
    written_objects.write_once(oid, || {
        let batch_active = write_batch_state.lock().unwrap().batch.is_some();
        if batch_active {
            // Rebase writes create huge numbers of objects. Don't stat the loose
            // object path for each one: buffering an existing object again is
            // harmless, and the command-scoped pack makes it durable in bulk.
            if buffered_objects.lock().unwrap().contains_key(&oid) {
                return Ok(());
            }
            let compressed_data =
                compress_git_object_data(&bytes).map_err(|err| BackendError::WriteObject {
                    object_type,
                    source: Box::new(err),
                })?;
            let mut state = write_batch_state.lock().unwrap();
            if let Some(batch) = state.batch.as_mut() {
                return buffer_git_object(
                    batch,
                    buffered_objects,
                    kind,
                    bytes,
                    compressed_data,
                    oid,
                );
            }
        }

        // Packed objects are intentionally not checked here; finding those would
        // require an ODB lookup. Avoid rewriting an existing loose object when
        // there is no active batch to absorb it cheaply.
        if loose_object_path(objects_dir, &oid).is_file() {
            return Ok(());
        }

        write_repos
            .with_repo(|repo| write_object_with_known_id(repo, kind, &bytes, oid, object_type))
    })
}

fn loose_object_path(objects_dir: &Path, oid: &gix::hash::oid) -> PathBuf {
    let mut hex_buf = gix::hash::Kind::hex_buf();
    let hex = oid.hex_to_buf(&mut hex_buf);
    objects_dir.join(&hex[..2]).join(&hex[2..])
}

fn write_object_with_known_id(
    repo: &gix::Repository,
    kind: gix::objs::Kind,
    bytes: &[u8],
    oid: gix::hash::ObjectId,
    object_type: &'static str,
) -> BackendResult<()> {
    // The object ID was computed from `kind` and `bytes`, so checking the ODB first is
    // unnecessary. Writing the same loose object again is harmless, and avoids a costly
    // pack/loose lookup for every object produced during a rebase.
    match repo.objects.write_buf_with_known_id(kind, bytes, oid) {
        Ok(write_oid) => {
            assert_eq!(oid, write_oid);
            Ok(())
        }
        Err(err) if is_already_exists_error(err.as_ref()) => Ok(()),
        Err(err) => Err(BackendError::WriteObject {
            object_type,
            source: err,
        }),
    }
}

fn is_already_exists_error(mut err: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if err
            .downcast_ref::<std::io::Error>()
            .is_some_and(|err| err.kind() == std::io::ErrorKind::AlreadyExists)
        {
            return true;
        }
        let Some(source) = err.source() else {
            return false;
        };
        err = source;
    }
}

/// Canonicalizes the given `path` except for the last `".git"` component.
///
/// The last path component matters when opening a Git repo without `core.bare`
/// config. This config is usually set, but the "repo" tool will set up such
/// repositories and symlinks. Opening such repo with fully-canonicalized path
/// would turn a colocated Git repo into a bare repo.
pub fn canonicalize_git_repo_path(path: &Path) -> io::Result<PathBuf> {
    if path.ends_with(".git") {
        let workdir = path.parent().unwrap();
        dunce::canonicalize(workdir).map(|dir| dir.join(".git"))
    } else {
        dunce::canonicalize(path)
    }
}

fn gix_open_opts_from_settings(settings: &UserSettings) -> gix::open::Options {
    let user_name = settings.user_name();
    let user_email = settings.user_email();
    gix::open::Options::default()
        .config_overrides([
            // Committer has to be configured to record reflog. Author isn't
            // needed, but let's copy the same values.
            format!("author.name={user_name}"),
            format!("author.email={user_email}"),
            format!("committer.name={user_name}"),
            format!("committer.email={user_email}"),
        ])
        // The git_target path should point the repository, not the working directory.
        .open_path_as_is(true)
        // Gitoxide recommends this when correctness is preferred
        .strict_config(true)
}

/// Parses the `jj:conflict-labels` header value if present.
fn extract_conflict_labels_from_commit(commit: &gix::objs::CommitRef) -> Merge<String> {
    let Some(value) = commit
        .extra_headers()
        .find(JJ_CONFLICT_LABELS_COMMIT_HEADER)
    else {
        return Merge::resolved(String::new());
    };

    str::from_utf8(value)
        .expect("labels should be valid utf8")
        .split_terminator('\n')
        .map(str::to_owned)
        .collect::<MergeBuilder<_>>()
        .build()
}

/// Parses the `jj:trees` header value if present, otherwise returns the
/// resolved tree ID from Git.
fn extract_root_tree_from_commit(commit: &gix::objs::CommitRef) -> Result<Merge<TreeId>, ()> {
    let Some(value) = commit.extra_headers().find(JJ_TREES_COMMIT_HEADER) else {
        let tree_id = TreeId::from_bytes(commit.tree().as_bytes());
        return Ok(Merge::resolved(tree_id));
    };

    let hash_len = commit.tree().kind().len_in_bytes();
    let mut tree_ids = SmallVec::new();
    for hex in value.split(|b| *b == b' ') {
        let tree_id = TreeId::try_from_hex(hex).ok_or(())?;
        if tree_id.as_bytes().len() != hash_len {
            return Err(());
        }
        tree_ids.push(tree_id);
    }
    // It is invalid to use `jj:trees` with a non-conflicted tree. If this were
    // allowed, it would be possible to construct a commit which appears to have
    // different contents depending on whether it is viewed using `jj` or `git`.
    if tree_ids.len() == 1 || tree_ids.len() % 2 == 0 {
        return Err(());
    }
    Ok(Merge::from_vec(tree_ids))
}

fn commit_from_git_without_root_parent(
    id: &CommitId,
    git_object: &gix::Object,
    is_shallow: bool,
) -> BackendResult<Commit> {
    commit_from_git_bytes(id, &git_object.data, is_shallow, git_object.id.kind())
}

fn commit_from_git_bytes(
    id: &CommitId,
    data: &[u8],
    is_shallow: bool,
    hash_kind: gix::hash::Kind,
) -> BackendResult<Commit> {
    let decode_err = |err: gix::objs::decode::Error| to_read_object_err(err, id);
    let commit = gix::objs::CommitRef::from_bytes(data, hash_kind)
        .map_err(|err| to_read_object_err(err, id))?;

    // If the git header has a change-id field, we attempt to convert that to a
    // valid JJ Change Id
    let change_id = extract_change_id_from_commit(&commit)
        .unwrap_or_else(|| synthetic_change_id_from_git_commit_id(id));

    // shallow commits don't have parents their parents actually fetched, so we
    // discard them here
    // TODO: This causes issues when a shallow repository is deepened/unshallowed
    let parents = if is_shallow {
        vec![]
    } else {
        commit
            .parents()
            .map(|oid| CommitId::from_bytes(oid.as_bytes()))
            .collect_vec()
    };
    // If the commit is a conflict, the conflict labels are stored in a commit
    // header separately from the trees.
    let conflict_labels = extract_conflict_labels_from_commit(&commit);
    // Conflicted commits written before we started using the `jj:trees` header
    // (~March 2024) may have the root trees stored in the extra metadata table
    // instead. For such commits, we'll update the root tree later when we read the
    // extra metadata.
    let root_tree = extract_root_tree_from_commit(&commit)
        .map_err(|()| to_read_object_err("Invalid jj:trees header", id))?;
    // Use lossy conversion as commit message with "mojibake" is still better than
    // nothing.
    // TODO: what should we do with commit.encoding?
    let description = String::from_utf8_lossy(commit.message).into_owned();
    let author = signature_from_git(commit.author().map_err(decode_err)?);
    let committer = signature_from_git(commit.committer().map_err(decode_err)?);

    // If the commit is signed, extract both the signature and the signed data
    // (which is the commit buffer with the gpgsig header omitted).
    // We have to re-parse the raw commit data because gix CommitRef does not give
    // us the sogned data, only the signature.
    // Ideally, we could use try_to_commit_ref_iter at the beginning of this
    // function and extract everything from that. For now, this works
    let secure_sig = commit
        .extra_headers
        .iter()
        // gix does not recognize gpgsig-sha256, but prevent future footguns by checking for it too
        .any(|(k, _)| {
            *k == signature_field_name(hash_kind) || *k == "gpgsig" || *k == "gpgsig-sha256"
        })
        .then(|| CommitRefIter::signature(data, hash_kind))
        .transpose()
        .map_err(decode_err)?
        .flatten()
        .map(|(sig, data)| SecureSig {
            data: data.to_bstring().into(),
            sig: sig.into_owned().into(),
        });

    Ok(Commit {
        parents,
        predecessors: vec![],
        // If this commit has associated extra metadata, we may reset this later.
        root_tree,
        conflict_labels,
        change_id,
        description,
        author,
        committer,
        secure_sig,
    })
}

/// Extracts change id from commit headers.
pub fn extract_change_id_from_commit(commit: &gix::objs::CommitRef) -> Option<ChangeId> {
    commit
        .extra_headers()
        .find(CHANGE_ID_COMMIT_HEADER)
        .and_then(ChangeId::try_from_reverse_hex)
        .filter(|val| val.as_bytes().len() == CHANGE_ID_LENGTH)
}

/// Deterministically creates a change id based on the commit id
///
/// Used when we get a commit without a change id. The exact algorithm for the
/// computation should not be relied upon.
pub fn synthetic_change_id_from_git_commit_id(id: &CommitId) -> ChangeId {
    // We reverse the bits of the commit id to create the change id. We don't
    // want to use the first bytes unmodified because then it would be ambiguous
    // if a given hash prefix refers to the commit id or the change id. It would
    // have been enough to pick the last 16 bytes instead of the leading 16
    // bytes to address that. We also reverse the bits to make it less likely
    // that users depend on any relationship between the two ids.
    let bytes = id.as_bytes()[id.as_bytes().len() - CHANGE_ID_LENGTH..]
        .iter()
        .rev()
        .map(|b| b.reverse_bits())
        .collect();
    ChangeId::new(bytes)
}

const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";

fn signature_from_git(signature: gix::actor::SignatureRef) -> Signature {
    let name = signature.name;
    let name = if name != EMPTY_STRING_PLACEHOLDER {
        String::from_utf8_lossy(name).into_owned()
    } else {
        "".to_string()
    };
    let email = signature.email;
    let email = if email != EMPTY_STRING_PLACEHOLDER {
        String::from_utf8_lossy(email).into_owned()
    } else {
        "".to_string()
    };
    let time = signature.time().unwrap_or_default();
    let timestamp = MillisSinceEpoch(time.seconds * 1000);
    let tz_offset = time.offset.div_euclid(60); // in minutes
    Signature {
        name,
        email,
        timestamp: Timestamp {
            timestamp,
            tz_offset,
        },
    }
}

fn signature_to_git(signature: &Signature) -> gix::actor::Signature {
    // git does not support empty names or emails
    let name = if !signature.name.is_empty() {
        &signature.name
    } else {
        EMPTY_STRING_PLACEHOLDER
    };
    let email = if !signature.email.is_empty() {
        &signature.email
    } else {
        EMPTY_STRING_PLACEHOLDER
    };
    let time = gix::date::Time::new(
        signature.timestamp.timestamp.0.div_euclid(1000),
        signature.timestamp.tz_offset * 60, // in seconds
    );
    gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time,
    }
}

fn serialize_extras(commit: &Commit) -> Vec<u8> {
    let mut proto = crate::protos::git_store::Commit {
        change_id: commit.change_id.to_bytes(),
        ..Default::default()
    };
    proto.uses_tree_conflict_format = true;
    for predecessor in &commit.predecessors {
        proto.predecessors.push(predecessor.to_bytes());
    }
    proto.encode_to_vec()
}

fn deserialize_extras(commit: &mut Commit, bytes: &[u8]) {
    let proto = crate::protos::git_store::Commit::decode(bytes).unwrap();
    if !proto.change_id.is_empty() {
        commit.change_id = ChangeId::new(proto.change_id);
    }
    if commit.root_tree.is_resolved()
        && proto.uses_tree_conflict_format
        && !proto.root_tree.is_empty()
    {
        let merge_builder: MergeBuilder<_> = proto
            .root_tree
            .iter()
            .map(|id_bytes| TreeId::from_bytes(id_bytes))
            .collect();
        commit.root_tree = merge_builder.build();
    }
    for predecessor in &proto.predecessors {
        commit.predecessors.push(CommitId::from_bytes(predecessor));
    }
}

/// Returns `RefEdit` that will create a ref in `refs/jj/keep` if not exist.
/// Used for preventing GC of commits we create.
fn to_no_gc_ref_update(id: &CommitId) -> gix::refs::transaction::RefEdit {
    let name = format!("{NO_GC_REF_NAMESPACE}{id}");
    let new = gix::refs::Target::Object(gix::ObjectId::from_bytes_or_panic(id.as_bytes()));
    let expected = gix::refs::transaction::PreviousValue::ExistingMustMatch(new.clone());
    gix::refs::transaction::RefEdit {
        change: gix::refs::transaction::Change::Update {
            log: gix::refs::transaction::LogChange {
                message: "used by jj".into(),
                ..Default::default()
            },
            expected,
            new,
        },
        name: name.try_into().unwrap(),
        deref: false,
    }
}

fn to_ref_deletion(git_ref: gix::refs::Reference) -> gix::refs::transaction::RefEdit {
    let expected = gix::refs::transaction::PreviousValue::ExistingMustMatch(git_ref.target);
    gix::refs::transaction::RefEdit {
        change: gix::refs::transaction::Change::Delete {
            expected,
            log: gix::refs::transaction::RefLog::AndReference,
        },
        name: git_ref.name,
        deref: false,
    }
}

/// Recreates `refs/jj/keep` refs for the `new_heads`, and removes the other
/// unreachable and non-head refs.
fn recreate_no_gc_refs(
    git_repo: &gix::Repository,
    new_heads: impl IntoIterator<Item = CommitId>,
    keep_newer: SystemTime,
) -> BackendResult<()> {
    // Calculate diff between existing no-gc refs and new heads.
    let new_heads: HashSet<CommitId> = new_heads.into_iter().collect();
    let mut no_gc_refs_to_keep_count: usize = 0;
    let mut no_gc_refs_to_delete: Vec<gix::refs::Reference> = Vec::new();
    let git_references = git_repo
        .references()
        .map_err(|err| BackendError::Other(err.into()))?;
    let no_gc_refs_iter = git_references
        .prefixed(NO_GC_REF_NAMESPACE)
        .map_err(|err| BackendError::Other(err.into()))?;
    for git_ref in no_gc_refs_iter {
        let git_ref = git_ref.map_err(BackendError::Other)?.detach();
        let oid = git_ref.target.try_id().ok_or_else(|| {
            let name = git_ref.name.as_bstr();
            BackendError::Other(format!("Symbolic no-gc ref found: {name}").into())
        })?;
        let id = CommitId::from_bytes(oid.as_bytes());
        let name_good = git_ref.name.as_bstr()[NO_GC_REF_NAMESPACE.len()..] == id.hex();
        if new_heads.contains(&id) && name_good {
            no_gc_refs_to_keep_count += 1;
            continue;
        }
        // Check timestamp of loose ref, but this is still racy on re-import
        // because:
        // - existing packed ref won't be demoted to loose ref
        // - existing loose ref won't be touched
        //
        // TODO: might be better to switch to a dummy merge, where new no-gc ref
        // will always have a unique name. Doing that with the current
        // ref-per-head strategy would increase the number of the no-gc refs.
        // https://github.com/jj-vcs/jj/pull/2659#issuecomment-1837057782
        let loose_ref_path = git_repo.path().join(git_ref.name.to_path());
        if let Ok(metadata) = loose_ref_path.metadata() {
            let mtime = metadata.modified().expect("unsupported platform?");
            if mtime > keep_newer {
                tracing::trace!(?git_ref, "not deleting new");
                no_gc_refs_to_keep_count += 1;
                continue;
            }
        }
        // Also deletes no-gc ref of random name created by old jj.
        tracing::trace!(?git_ref, ?name_good, "will delete");
        no_gc_refs_to_delete.push(git_ref);
    }
    tracing::info!(
        new_heads_count = new_heads.len(),
        no_gc_refs_to_keep_count,
        no_gc_refs_to_delete_count = no_gc_refs_to_delete.len(),
        "collected reachable refs"
    );

    // It's slow to delete packed refs one by one, so update refs all at once.
    let ref_edits = itertools::chain(
        no_gc_refs_to_delete.into_iter().map(to_ref_deletion),
        new_heads.iter().map(to_no_gc_ref_update),
    );
    git_repo
        .edit_references(ref_edits)
        .map_err(|err| BackendError::Other(err.into()))?;

    Ok(())
}

fn git_gc_command(program: &OsStr, git_dir: &Path, options: GcOptions) -> Command {
    let keep_newer = options
        .keep_newer
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default(); // underflow
    let mut git = Command::new(program);
    git.arg("--git-dir=.") // turn off discovery
        .arg("gc")
        .arg(format!("--prune=@{} +0000", keep_newer.as_secs()));
    if !options.use_cruft {
        git.arg("--no-cruft");
    }
    // Don't specify it by GIT_DIR/--git-dir. On Windows, the path could be
    // canonicalized as UNC path, which wouldn't be supported by git.
    git.current_dir(git_dir);
    git
}

fn run_git_gc(program: &OsStr, git_dir: &Path, options: GcOptions) -> Result<(), GitGcError> {
    let mut git = git_gc_command(program, git_dir, options);
    // TODO: pass output to UI layer instead of printing directly here
    tracing::info!(?git, "running git gc");
    let status = git.status().map_err(GitGcError::GcCommand)?;
    tracing::info!(?status, "git gc exited");
    if !status.success() {
        return Err(GitGcError::GcCommandErrorStatus(status));
    }
    Ok(())
}

fn validate_git_object_id(
    repo: &gix::Repository,
    id: &impl ObjectId,
) -> BackendResult<gix::ObjectId> {
    validate_git_object_id_kind(repo.object_hash(), id)
}

fn validate_git_object_id_kind(
    expected_kind: gix::hash::Kind,
    id: &impl ObjectId,
) -> BackendResult<gix::ObjectId> {
    match gix::ObjectId::try_from(id.as_bytes()) {
        Ok(id) if id.kind() == expected_kind => Ok(id),
        _ => Err(BackendError::InvalidHashLength {
            expected: expected_kind.len_in_bytes(),
            actual: id.as_bytes().len(),
            object_type: id.object_type(),
            hash: id.hex(),
        }),
    }
}

fn map_not_found_err(err: gix::object::find::existing::Error, id: &impl ObjectId) -> BackendError {
    if matches!(err, gix::object::find::existing::Error::NotFound { .. }) {
        BackendError::ObjectNotFound {
            object_type: id.object_type(),
            hash: id.hex(),
            source: Box::new(err),
        }
    } else {
        to_read_object_err(err, id)
    }
}

fn to_read_object_err(
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    id: &impl ObjectId,
) -> BackendError {
    BackendError::ReadObject {
        object_type: id.object_type(),
        hash: id.hex(),
        source: err.into(),
    }
}

fn to_invalid_utf8_err(source: Utf8Error, id: &impl ObjectId) -> BackendError {
    BackendError::InvalidUtf8 {
        object_type: id.object_type(),
        hash: id.hex(),
        source,
    }
}

fn import_extra_metadata_entries_from_heads(
    git_repo: &gix::Repository,
    mut_table: &mut MutableTable,
    head_ids: &HashSet<&CommitId>,
    shallow_roots: &[CommitId],
) -> BackendResult<()> {
    let mut work_ids = head_ids
        .iter()
        .filter(|&id| mut_table.get_value(id.as_bytes()).is_none())
        .map(|&id| id.clone())
        .collect_vec();
    while let Some(id) = work_ids.pop() {
        let git_object = git_repo
            .find_object(validate_git_object_id(git_repo, &id)?)
            .map_err(|err| map_not_found_err(err, &id))?;
        let is_shallow = shallow_roots.contains(&id);
        // TODO(#1624): Should we read the root tree here and check if it has a
        // `.jjconflict-...` entries? That could happen if the user used `git` to e.g.
        // change the description of a commit with tree-level conflicts.
        let commit = commit_from_git_without_root_parent(&id, &git_object, is_shallow)?;
        mut_table.add_entry(id.to_bytes(), serialize_extras(&commit));
        work_ids.extend(
            commit
                .parents
                .into_iter()
                .filter(|id| mut_table.get_value(id.as_bytes()).is_none()),
        );
    }
    Ok(())
}

impl Debug for GitBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.debug_struct("GitBackend")
            .field("path", &self.git_repo_path())
            .finish()
    }
}

fn read_tree_from_git_repo(
    repo: &mut gix::Repository,
    id: &TreeId,
    git_tree_id: gix::ObjectId,
) -> BackendResult<Tree> {
    let git_tree = repo
        .find_object(git_tree_id)
        .map_err(|err| map_not_found_err(err, id))?
        .try_into_tree()
        .map_err(|err| to_read_object_err(err, id))?;
    let mut entries: Vec<_> = git_tree
        .iter()
        .map(|entry| -> BackendResult<_> {
            let entry = entry.map_err(|err| to_read_object_err(err, id))?;
            tree_entry_from_git(entry.filename(), entry.mode().kind(), entry.oid(), id)
        })
        .try_collect()?;
    sort_git_tree_entries(&mut entries);
    Ok(Tree::from_sorted_entries(entries))
}

fn read_tree_from_git_bytes(
    data: &[u8],
    id: &TreeId,
    hash_kind: gix::hash::Kind,
) -> BackendResult<Tree> {
    let mut entries: Vec<_> = gix::objs::TreeRefIter::from_bytes(data, hash_kind)
        .map(|entry| -> BackendResult<_> {
            let entry = entry.map_err(|err| to_read_object_err(err, id))?;
            tree_entry_from_git(entry.filename, entry.mode.kind(), entry.oid, id)
        })
        .try_collect()?;
    sort_git_tree_entries(&mut entries);
    Ok(Tree::from_sorted_entries(entries))
}

fn tree_entry_from_git(
    filename: &[u8],
    kind: gix::object::tree::EntryKind,
    oid: &gix::oid,
    id: &TreeId,
) -> BackendResult<(RepoPathComponentBuf, TreeValue)> {
    let name = RepoPathComponentBuf::new(
        str::from_utf8(filename).map_err(|err| to_invalid_utf8_err(err, id))?,
    )
    .unwrap();
    let value = match kind {
        gix::object::tree::EntryKind::Tree => {
            let id = TreeId::from_bytes(oid.as_bytes());
            TreeValue::Tree(id)
        }
        gix::object::tree::EntryKind::Blob => {
            let id = FileId::from_bytes(oid.as_bytes());
            TreeValue::File {
                id,
                executable: false,
                copy_id: CopyId::placeholder(),
            }
        }
        gix::object::tree::EntryKind::BlobExecutable => {
            let id = FileId::from_bytes(oid.as_bytes());
            TreeValue::File {
                id,
                executable: true,
                copy_id: CopyId::placeholder(),
            }
        }
        gix::object::tree::EntryKind::Link => {
            let id = SymlinkId::from_bytes(oid.as_bytes());
            TreeValue::Symlink(id)
        }
        gix::object::tree::EntryKind::Commit => {
            let id = CommitId::from_bytes(oid.as_bytes());
            TreeValue::GitSubmodule(id)
        }
    };
    Ok((name, value))
}

fn sort_git_tree_entries(entries: &mut [(RepoPathComponentBuf, TreeValue)]) {
    // While Git tree entries are sorted, the rule is slightly different.
    // Directory names are sorted as if they had trailing "/".
    if !entries.is_sorted_by_key(|(name, _)| name) {
        entries.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
    }
}

#[async_trait]
impl Backend for GitBackend {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        self.base_repo.objects.object_hash().len_in_bytes()
    }

    fn change_id_length(&self) -> usize {
        CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        GIT_TREE_READ_CONCURRENCY
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        let data = self.read_file_sync(id)?;
        Ok(Box::pin(Cursor::new(data)))
    }

    async fn read_file_bytes(&self, _path: &RepoPath, id: &FileId) -> BackendResult<Vec<u8>> {
        self.read_file_sync(id)
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut bytes = Vec::new();
        contents.read_to_end(&mut bytes).await.unwrap();

        let oid = self.write_blob(&bytes, "file")?;
        Ok(FileId::new(oid.as_bytes().to_vec()))
    }

    async fn write_file_bytes(&self, _path: &RepoPath, contents: &[u8]) -> BackendResult<FileId> {
        let oid = self.write_blob(contents, "file")?;
        Ok(FileId::new(oid.as_bytes().to_vec()))
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let git_blob_id = validate_git_object_id_kind(self.base_repo.objects.object_hash(), id)?;
        let buffered_object = self
            .buffered_objects
            .lock()
            .unwrap()
            .get(&git_blob_id)
            .cloned();
        if let Some(object) = buffered_object {
            if object.kind != gix::objs::Kind::Blob {
                return Err(to_read_object_err(
                    io::Error::other(format!(
                        "expected blob, found {}",
                        object_kind_name(object.kind)
                    )),
                    id,
                ));
            }
            return String::from_utf8(object.data.clone())
                .map_err(|err| to_invalid_utf8_err(err.utf8_error(), id));
        }
        let locked_repo = self.lock_git_repo();
        let mut blob = locked_repo
            .find_object(git_blob_id)
            .map_err(|err| map_not_found_err(err, id))?
            .try_into_blob()
            .map_err(|err| to_read_object_err(err, id))?;
        let target = String::from_utf8(blob.take_data())
            .map_err(|err| to_invalid_utf8_err(err.utf8_error(), id))?;
        Ok(target)
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        let oid = self.write_blob(target.as_bytes(), "symlink")?;
        Ok(SymlinkId::new(oid.as_bytes().to_vec()))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "The Git backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "The Git backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported(
            "The Git backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if id == &self.empty_tree_id {
            return Ok(Tree::default());
        }
        let git_tree_id = validate_git_object_id_kind(self.base_repo.objects.object_hash(), id)?;
        let buffered_object = self
            .buffered_objects
            .lock()
            .unwrap()
            .get(&git_tree_id)
            .cloned();
        if let Some(object) = buffered_object {
            if object.kind != gix::objs::Kind::Tree {
                return Err(to_read_object_err(
                    io::Error::other(format!(
                        "expected tree, found {}",
                        object_kind_name(object.kind)
                    )),
                    id,
                ));
            }
            return read_tree_from_git_bytes(&object.data, id, git_tree_id.kind());
        }
        // File snapshotting already runs on Rayon workers. Use the worker's
        // dedicated repository instead of spawning a nested job or contending
        // with other workers on a shared read-repository pool.
        if rayon::current_thread_index().is_some() {
            return self.read_repos.with_repo(|repo| {
                let git_tree_id = validate_git_object_id(repo, id)?;
                read_tree_from_git_repo(repo, id, git_tree_id)
            });
        }

        let read_repos = self.read_repos.clone();
        let id = id.clone();
        let (sender, receiver) = oneshot::channel();
        rayon::spawn(move || {
            let result =
                read_repos.with_repo(|repo| read_tree_from_git_repo(repo, &id, git_tree_id));
            drop(sender.send(result));
        });
        receiver.await.map_err(|_| {
            BackendError::Other(Box::new(std::io::Error::other("Git tree read task exited")))
        })?
    }

    async fn write_tree(&self, _path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        let entries = contents
            .entries()
            .map(|entry| {
                match entry.value() {
                    TreeValue::File {
                        id,
                        executable: false,
                        copy_id: _, // TODO: Use the value
                    } => GitTreeEntry {
                        name: entry.name().as_internal_str().as_bytes(),
                        mode: b"100644",
                        oid: id.as_bytes(),
                        is_tree: false,
                    },
                    TreeValue::File {
                        id,
                        executable: true,
                        copy_id: _, // TODO: Use the value
                    } => GitTreeEntry {
                        name: entry.name().as_internal_str().as_bytes(),
                        mode: b"100755",
                        oid: id.as_bytes(),
                        is_tree: false,
                    },
                    TreeValue::Symlink(id) => GitTreeEntry {
                        name: entry.name().as_internal_str().as_bytes(),
                        mode: b"120000",
                        oid: id.as_bytes(),
                        is_tree: false,
                    },
                    TreeValue::Tree(id) => GitTreeEntry {
                        name: entry.name().as_internal_str().as_bytes(),
                        mode: b"40000",
                        oid: id.as_bytes(),
                        is_tree: true,
                    },
                    TreeValue::GitSubmodule(id) => GitTreeEntry {
                        name: entry.name().as_internal_str().as_bytes(),
                        mode: b"160000",
                        oid: id.as_bytes(),
                        is_tree: false,
                    },
                }
            })
            .collect();
        // Serialize and hash outside the repository lock. gix's `write_object()` does both
        // while holding the lock and hashes the serialized object again before writing it.
        let bytes = serialize_git_tree(entries).map_err(|err| BackendError::WriteObject {
            object_type: "tree",
            source: Box::new(err),
        })?;
        let oid = compute_synthesized_tree_hash(&bytes);
        let write_result = if rayon::current_thread_index().is_some() {
            write_or_buffer_object_on_pool(
                &self.objects_dir,
                &self.written_objects,
                &self.write_repos,
                &self.write_batch_state,
                &self.buffered_objects,
                gix::objs::Kind::Tree,
                bytes,
                oid,
                "tree",
            )
        } else {
            let objects_dir = self.objects_dir.clone();
            let written_objects = self.written_objects.clone();
            let write_repos = self.write_repos.clone();
            let write_batch_state = self.write_batch_state.clone();
            let buffered_objects = self.buffered_objects.clone();
            let (sender, receiver) = oneshot::channel();
            rayon::spawn(move || {
                let result = write_or_buffer_object_on_pool(
                    &objects_dir,
                    &written_objects,
                    &write_repos,
                    &write_batch_state,
                    &buffered_objects,
                    gix::objs::Kind::Tree,
                    bytes,
                    oid,
                    "tree",
                );
                drop(sender.send(result));
            });
            receiver.await.map_err(|_| {
                BackendError::Other(Box::new(std::io::Error::other(
                    "Git tree write task exited",
                )))
            })?
        };
        write_result?;
        Ok(TreeId::from_bytes(oid.as_bytes()))
    }

    #[tracing::instrument(skip(self))]
    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let git_commit_id = validate_git_object_id_kind(self.base_repo.objects.object_hash(), id)?;
        let buffered_object = self
            .buffered_objects
            .lock()
            .unwrap()
            .get(&git_commit_id)
            .cloned();
        let mut commit = if let Some(object) = buffered_object {
            if object.kind != gix::objs::Kind::Commit {
                return Err(to_read_object_err(
                    io::Error::other(format!(
                        "expected commit, found {}",
                        object_kind_name(object.kind)
                    )),
                    id,
                ));
            }
            commit_from_git_bytes(
                id,
                &object.data,
                false,
                self.base_repo.objects.object_hash(),
            )?
        } else {
            let locked_repo = self.lock_git_repo();
            let git_commit_id = validate_git_object_id(&locked_repo, id)?;
            let git_object = locked_repo
                .find_object(git_commit_id)
                .map_err(|err| map_not_found_err(err, id))?;
            let is_shallow = self.shallow_root_ids(&locked_repo)?.contains(id);
            commit_from_git_without_root_parent(id, &git_object, is_shallow)?
        };
        if commit.parents.is_empty() {
            commit.parents.push(self.root_commit_id.clone());
        }

        if let Some(extras) = self.extra_metadata_for_commit(id)? {
            deserialize_extras(&mut commit, &extras);
        } else {
            // TODO: Remove this hack and map to ObjectNotFound error if we're sure that
            // there are no reachable ancestor commits without extras metadata. Git commits
            // imported by jj < 0.8.0 might not have extras (#924).
            // https://github.com/jj-vcs/jj/issues/2343
            tracing::info!("unimported Git commit found");
            self.import_head_commits([id])?;
            let extras = self
                .extra_metadata_for_commit(id)?
                .expect("imported Git commit should have extra metadata");
            deserialize_extras(&mut commit, &extras);
        }
        Ok(commit)
    }

    async fn write_commit(
        &self,
        mut contents: Commit,
        mut sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        assert!(contents.secure_sig.is_none(), "commit.secure_sig was set");

        let locked_repo = self.lock_git_repo();
        let tree_ids = &contents.root_tree;
        let git_tree_id = match tree_ids.as_resolved() {
            Some(tree_id) => validate_git_object_id(&locked_repo, tree_id)?,
            None => write_tree_conflict(self, &locked_repo, tree_ids)?,
        };
        let author = signature_to_git(&contents.author);
        let mut committer = signature_to_git(&contents.committer);
        let message = &contents.description;
        if contents.parents.is_empty() {
            return Err(BackendError::Other(
                "Cannot write a commit with no parents".into(),
            ));
        }
        let mut parents = SmallVec::new();
        for parent_id in &contents.parents {
            if *parent_id == self.root_commit_id {
                // Git doesn't have a root commit, so if the parent is the root commit, we don't
                // add it to the list of parents to write in the Git commit. We also check that
                // there are no other parents since Git cannot represent a merge between a root
                // commit and another commit.
                if contents.parents.len() > 1 {
                    return Err(BackendError::Unsupported(
                        "The Git backend does not support creating merge commits with the root \
                         commit as one of the parents."
                            .to_owned(),
                    ));
                }
            } else {
                parents.push(validate_git_object_id(&locked_repo, parent_id)?);
            }
        }
        let mut extra_headers: Vec<(BString, BString)> = vec![];
        if !contents.conflict_labels.is_resolved() {
            // Labels cannot contain '\n' since we use it as a separator in the header.
            assert!(
                contents
                    .conflict_labels
                    .iter()
                    .all(|label| !label.contains('\n'))
            );
            let mut joined_with_newlines = contents.conflict_labels.iter().join("\n");
            joined_with_newlines.push('\n');
            extra_headers.push((
                JJ_CONFLICT_LABELS_COMMIT_HEADER.into(),
                joined_with_newlines.into(),
            ));
        }
        if !tree_ids.is_resolved() {
            let value = tree_ids.iter().map(|id| id.hex()).join(" ");
            extra_headers.push((JJ_TREES_COMMIT_HEADER.into(), value.into()));
        }
        if self.write_change_id_header {
            extra_headers.push((
                CHANGE_ID_COMMIT_HEADER.into(),
                contents.change_id.reverse_hex().into(),
            ));
        }

        if tree_ids.iter().any(|id| id == &self.empty_tree_id) {
            let tree_id =
                self.write_object_bytes(gix::objs::Kind::Tree, Vec::new(), "empty tree")?;
            assert!(tree_id.is_empty_tree());
        }

        let extras = serialize_extras(&contents);

        // If two writers write commits of the same id with different metadata, they
        // will both succeed and the metadata entries will be "merged" later. Since
        // metadata entry is keyed by the commit id, one of the entries would be lost.
        // To prevent such race condition locally, we extend the scope covered by the
        // table lock. This is still racy if multiple machines are involved and the
        // repository is rsync-ed.
        let mut write_batch_state = self.write_batch_state.lock().unwrap();
        let write_batch_active = write_batch_state.batch.is_some();
        let metadata_table = if let Some(batch) = write_batch_state.batch.as_mut() {
            batch.ensure_object_store_lock(self.git_repo_path())?;
            GitWriteTable::Batch(batch.table(self)?)
        } else {
            let (table, table_lock) = self.read_extra_metadata_table_locked()?;
            GitWriteTable::Direct { table, table_lock }
        };
        let (id, buffered_commit) = loop {
            let mut commit = gix::objs::Commit {
                message: message.to_owned().into(),
                tree: git_tree_id,
                author: author.clone(),
                committer: committer.clone(),
                encoding: None,
                parents: parents.clone(),
                extra_headers: extra_headers.clone(),
            };

            if let Some(sign) = &mut sign_with {
                // we don't use gix pool, but at least use their heuristic
                let mut data = Vec::with_capacity(512);
                commit.write_to(&mut data).unwrap();

                let sig = sign(&data).map_err(|err| BackendError::WriteObject {
                    object_type: "commit",
                    source: Box::new(err),
                })?;
                let field = signature_field_name(git_tree_id.kind());
                commit
                    .extra_headers
                    .push((field.into(), sig.clone().into()));
                contents.secure_sig = Some(SecureSig { data, sig });
            }

            let (git_id, commit_data) = if write_batch_active {
                let mut data = Vec::with_capacity(512);
                commit.write_to(&mut data).unwrap();
                let git_id = gix::objs::compute_hash(
                    self.base_repo.objects.object_hash(),
                    gix::objs::Kind::Commit,
                    &data,
                )
                .map_err(|err| BackendError::WriteObject {
                    object_type: "commit",
                    source: Box::new(err),
                })?;
                (git_id, Some(data))
            } else {
                let git_id =
                    locked_repo
                        .write_object(&commit)
                        .map_err(|err| BackendError::WriteObject {
                            object_type: "commit",
                            source: Box::new(err),
                        })?;
                (git_id.detach(), None)
            };

            match metadata_table.get_value(git_id.as_bytes()) {
                Some(existing_extras) if existing_extras != extras => {
                    // It's possible a commit already exists with the same
                    // commit id but different change id. Adjust the timestamp
                    // until this is no longer the case.
                    //
                    // For example, this can happen when rebasing duplicate
                    // commits, https://github.com/jj-vcs/jj/issues/694.
                    //
                    // `jj` resets the committer timestamp to the current
                    // timestamp whenever it rewrites a commit. So, it's
                    // unlikely for the timestamp to be 0 even if the original
                    // commit had its timestamp set to 0. Moreover, we test that
                    // a commit with a negative timestamp can still be written
                    // and read back by `jj`.
                    committer.time.seconds -= 1;
                }
                _ => {
                    break (
                        CommitId::from_bytes(git_id.as_bytes()),
                        commit_data.map(|data| (git_id, data)),
                    );
                }
            }
        };

        if !write_batch_active {
            // Everything up to this point had no permanent effect on the repo except
            // GC-able objects.
            locked_repo
                .edit_reference(to_no_gc_ref_update(&id))
                .map_err(|err| BackendError::Other(Box::new(err)))?;
        }

        // Update the signature to match the one that was actually written to the object
        // store
        contents.committer.timestamp.timestamp = MillisSinceEpoch(committer.time.seconds * 1000);
        metadata_table.finish(self, id.to_bytes(), extras)?;
        if write_batch_active {
            let (git_id, data) = buffered_commit.unwrap();
            self.written_objects.write_once(git_id, || {
                if loose_object_path(&self.objects_dir, &git_id).is_file() {
                    return Ok(());
                }
                if self.buffered_objects.lock().unwrap().contains_key(&git_id) {
                    return Ok(());
                }
                let compressed_data =
                    compress_git_object_data(&data).map_err(|err| BackendError::WriteObject {
                        object_type: "commit",
                        source: Box::new(err),
                    })?;
                buffer_git_object(
                    write_batch_state.batch.as_mut().unwrap(),
                    &self.buffered_objects,
                    gix::objs::Kind::Commit,
                    data,
                    compressed_data,
                    git_id,
                )
            })?;
            write_batch_state
                .batch
                .as_mut()
                .unwrap()
                .commit_ids
                .insert(id.clone());
        }
        Ok((id, contents))
    }

    fn get_copy_records(
        &self,
        paths: Option<&[RepoPathBuf]>,
        root_id: &CommitId,
        head_id: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        let hash_kind = self.base_repo.objects.object_hash();
        let root_oid = validate_git_object_id_kind(hash_kind, root_id)?;
        let head_oid = validate_git_object_id_kind(hash_kind, head_id)?;
        let needs_flush = {
            let objects = self.buffered_objects.lock().unwrap();
            objects.contains_key(&root_oid) || objects.contains_key(&head_oid)
        };
        if needs_flush {
            // gix's copy detector recursively reads trees through its own ODB
            // instead of the Backend interface, so checkpoint the quarantine
            // before handing these commits to it.
            self.flush_active_write_batch()?;
        }
        let repo = self.git_repo();
        let root_tree = self.read_tree_for_commit(&repo, root_id)?;
        let head_tree = self.read_tree_for_commit(&repo, head_id)?;

        let change_to_copy_record =
            |change: gix::object::tree::diff::Change| -> BackendResult<Option<CopyRecord>> {
                let gix::object::tree::diff::Change::Rewrite {
                    source_location,
                    source_entry_mode,
                    source_id,
                    entry_mode: dest_entry_mode,
                    location: dest_location,
                    ..
                } = change
                else {
                    return Ok(None);
                };
                // TODO: Renamed symlinks cannot be returned because CopyRecord
                // expects `source_file: FileId`.
                if !source_entry_mode.is_blob() || !dest_entry_mode.is_blob() {
                    return Ok(None);
                }

                let source = str::from_utf8(source_location)
                    .map_err(|err| to_invalid_utf8_err(err, root_id))?;
                let dest = str::from_utf8(dest_location)
                    .map_err(|err| to_invalid_utf8_err(err, head_id))?;

                let target = RepoPathBuf::from_internal_string(dest).unwrap();
                if !paths.is_none_or(|paths| paths.contains(&target)) {
                    return Ok(None);
                }

                Ok(Some(CopyRecord {
                    target,
                    target_commit: head_id.clone(),
                    source: RepoPathBuf::from_internal_string(source).unwrap(),
                    source_file: FileId::from_bytes(source_id.as_bytes()),
                    source_commit: root_id.clone(),
                }))
            };

        let mut records: Vec<BackendResult<CopyRecord>> = Vec::new();
        root_tree
            .changes()
            .map_err(|err| BackendError::Other(err.into()))?
            .options(|opts| {
                opts.track_path().track_rewrites(Some(gix::diff::Rewrites {
                    copies: Some(gix::diff::rewrites::Copies {
                        source: gix::diff::rewrites::CopySource::FromSetOfModifiedFiles,
                        percentage: Some(0.5),
                    }),
                    percentage: Some(0.5),
                    limit: 1000,
                    track_empty: false,
                }));
            })
            .for_each_to_obtain_tree_with_cache(
                &head_tree,
                &mut self.new_diff_platform()?,
                |change| -> BackendResult<_> {
                    match change_to_copy_record(change) {
                        Ok(None) => {}
                        Ok(Some(change)) => records.push(Ok(change)),
                        Err(err) => records.push(Err(err)),
                    }
                    Ok(gix::object::tree::diff::Action::Continue(()))
                },
            )
            .map_err(|err| BackendError::Other(err.into()))?;
        Ok(futures::stream::iter(records).boxed())
    }

    #[tracing::instrument(skip(self, index))]
    fn gc(&self, index: &dyn Index, options: GcOptions) -> BackendResult<()> {
        let git_repo = self.lock_git_repo();
        // A workspace command already holds this lock through its write batch.
        // Direct library callers acquire it here. Keep the batch-state guard
        // for the entire GC so in-process writers cannot race us either.
        let mut write_batch_state = self.write_batch_state.lock().unwrap();
        let _object_store_lock = if let Some(batch) = write_batch_state.batch.as_mut() {
            batch.ensure_object_store_lock(self.git_repo_path())?;
            None
        } else {
            Some(
                FileLock::lock(self.git_object_store_lock_path())
                    .map_err(|err| BackendError::Other(Box::new(err)))?,
            )
        };
        let new_heads = index
            .all_heads_for_gc()
            .map_err(|err| BackendError::Other(err.into()))?
            .filter(|id| *id != self.root_commit_id);
        recreate_no_gc_refs(&git_repo, new_heads, options.keep_newer)?;

        // No locking is needed since we aren't going to add new "commits".
        let table = self.cached_extra_metadata_table()?;
        // TODO: remove unreachable entries from extras table if segment file
        // mtime <= keep_newer? (it won't be consistent with no-gc refs
        // preserved by the keep_newer timestamp though)
        self.extra_metadata_store
            .gc(&table, options.keep_newer)
            .map_err(|err| BackendError::Other(err.into()))?;

        run_git_gc(self.git_executable.as_ref(), self.git_repo_path(), options)
            .map_err(|err| BackendError::Other(err.into()))?;
        // Since "git gc" will move loose refs into packed refs, in-memory
        // packed-refs cache should be invalidated without relying on mtime.
        git_repo.refs.force_refresh_packed_buffer().ok();
        Ok(())
    }
}

/// Write a tree conflict as a special tree with `.jjconflict-base-N` and
/// `.jjconflict-side-N` subtrees. This ensure that the parts are not GC'd.
/// Also includes a `JJ-CONFLICT-README` file explaining why these trees are
/// present. The rest of the tree is copied from the first term of the conflict,
/// which prevents editors with Git support from highlighting all files as new.
fn write_tree_conflict(
    backend: &GitBackend,
    repo: &gix::Repository,
    conflict: &Merge<TreeId>,
) -> BackendResult<gix::ObjectId> {
    // Tree entries to be written must be sorted by Entry::filename().
    let mut entries = itertools::chain(
        conflict
            .removes()
            .enumerate()
            .map(|(i, tree_id)| (format!(".jjconflict-base-{i}"), tree_id)),
        conflict
            .adds()
            .enumerate()
            .map(|(i, tree_id)| (format!(".jjconflict-side-{i}"), tree_id)),
    )
    .map(|(name, tree_id)| gix::objs::tree::Entry {
        mode: gix::object::tree::EntryKind::Tree.into(),
        filename: name.into(),
        oid: gix::ObjectId::from_bytes_or_panic(tree_id.as_bytes()),
    })
    .collect_vec();
    let readme_id = backend.write_blob(
        r#"This commit was made by jj, https://jj-vcs.dev/.
The commit contains file conflicts, and therefore looks wrong when used with
plain Git or other tools that are unfamiliar with jj.

The .jjconflict-* directories represent the different inputs to the conflict.
For details, see
https://docs.jj-vcs.dev/latest/git-compatibility/#format-mapping-details

If you see this file in your working copy, it probably means that you used a
regular `git` command to check out a conflicted commit. Use `jj abandon` to
recover.
"#
        .as_bytes(),
        "conflict README",
    )?;
    entries.push(gix::objs::tree::Entry {
        mode: gix::object::tree::EntryKind::Blob.into(),
        filename: JJ_CONFLICT_README_FILE_NAME.into(),
        oid: readme_id,
    });
    let first_tree_id = conflict.first();
    let first_tree_id_git =
        validate_git_object_id_kind(backend.base_repo.objects.object_hash(), first_tree_id)?;
    let buffered_tree = backend
        .buffered_objects
        .lock()
        .unwrap()
        .get(&first_tree_id_git)
        .cloned();
    let first_tree_entries: Vec<gix::objs::tree::Entry> = if let Some(object) = buffered_tree {
        if object.kind != gix::objs::Kind::Tree {
            return Err(to_read_object_err(
                io::Error::other(format!(
                    "expected tree, found {}",
                    object_kind_name(object.kind)
                )),
                first_tree_id,
            ));
        }
        gix::objs::TreeRefIter::from_bytes(&object.data, first_tree_id_git.kind())
            .map(|entry| -> BackendResult<_> {
                let entry = entry.map_err(|err| to_read_object_err(err, first_tree_id))?;
                Ok(gix::objs::tree::Entry::from(entry))
            })
            .try_collect()?
    } else {
        let first_tree = repo
            .find_tree(first_tree_id_git)
            .map_err(|err| to_read_object_err(err, first_tree_id))?;
        first_tree
            .iter()
            .map(|entry| {
                entry
                    .map(|entry| entry.detach().into())
                    .map_err(|err| to_read_object_err(err, first_tree_id))
            })
            .try_collect()?
    };
    for entry in first_tree_entries {
        if !entry.filename.starts_with(b".jjconflict")
            && entry.filename != JJ_CONFLICT_README_FILE_NAME
        {
            entries.push(entry);
        }
    }
    entries.sort_unstable();
    let mut data = Vec::new();
    gix::objs::Tree { entries }.write_to(&mut data).unwrap();
    backend.write_object_bytes(gix::objs::Kind::Tree, data, "conflict tree")
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use gix::date::parse::TimeBuf;
    use gix::objs::CommitRef;
    use indoc::indoc;
    use test_case::test_case;

    use super::*;
    use crate::config::StackedConfig;
    use crate::content_hash::blake2b_hash;
    use crate::hex_util;
    use crate::tests::TestResult;
    use crate::tests::new_temp_dir;

    const GIT_USER: &str = "Someone";
    const GIT_EMAIL: &str = "someone@example.com";

    fn git_config() -> Vec<bstr::BString> {
        vec![
            format!("user.name = {GIT_USER}").into(),
            format!("user.email = {GIT_EMAIL}").into(),
            "init.defaultBranch = master".into(),
        ]
    }

    fn open_options() -> gix::open::Options {
        gix::open::Options::isolated()
            .config_overrides(git_config())
            .strict_config(true)
    }

    #[test]
    fn serialize_git_tree_uses_git_sort_order() -> TestResult {
        const SHA1_LENGTH: usize = 20;
        let entries = vec![
            GitTreeEntry {
                name: b"foo",
                mode: b"40000",
                oid: &[1; SHA1_LENGTH],
                is_tree: true,
            },
            GitTreeEntry {
                name: b"foo.bar",
                mode: b"100644",
                oid: &[2; SHA1_LENGTH],
                is_tree: false,
            },
        ];
        let bytes = serialize_git_tree(entries)?;
        let mut expected = b"100644 foo.bar\0".to_vec();
        expected.extend_from_slice(&[2; SHA1_LENGTH]);
        expected.extend_from_slice(b"40000 foo\0");
        expected.extend_from_slice(&[1; SHA1_LENGTH]);
        assert_eq!(bytes, expected);
        Ok(())
    }

    #[test]
    fn synthesized_tree_hash_matches_collision_checked_hash() -> TestResult {
        let large_tree = vec![0x5a; 128 * 1024];
        for bytes in [&[][..], b"100644 file\0object-id".as_slice(), &large_tree] {
            let expected =
                gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, bytes)?;
            assert_eq!(compute_synthesized_tree_hash(bytes), expected);
        }
        assert_eq!(
            compute_synthesized_tree_hash(&[]),
            gix::ObjectId::from_hex(b"4b825dc642cb6eb9a060e54bf8d69288fbee4904")?
        );
        Ok(())
    }

    #[test]
    fn git_gc_cruft_policy_is_explicit() {
        let keep_newer = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(42);
        let command_args = |use_cruft| {
            git_gc_command(
                OsStr::new("git"),
                Path::new("repo"),
                GcOptions {
                    keep_newer,
                    use_cruft,
                },
            )
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect_vec()
        };

        assert_eq!(
            command_args(true),
            ["--git-dir=.", "gc", "--prune=@42 +0000"]
        );
        assert_eq!(
            command_args(false),
            ["--git-dir=.", "gc", "--prune=@42 +0000", "--no-cruft"]
        );
    }

    #[test]
    fn git_object_cache_is_shared_between_handles() {
        let shards = new_git_object_cache();
        let mut first = SharedGitObjectCache {
            shards: shards.clone(),
        };
        let mut second = SharedGitObjectCache { shards };
        let id = gix::ObjectId::from_hex(b"1111111111111111111111111111111111111111").unwrap();

        gix::odb::pack::cache::Object::put(&mut first, id, gix::objs::Kind::Tree, b"tree");
        let mut out = Vec::new();
        let kind = gix::odb::pack::cache::Object::get(&mut second, &id, &mut out);

        assert_eq!(kind, Some(gix::objs::Kind::Tree));
        assert_eq!(out, b"tree");
    }

    #[test]
    fn git_pack_cache_is_shared_between_handles() {
        let shards = new_git_pack_cache();
        let mut first = SharedGitPackCache {
            shards: shards.clone(),
        };
        let mut second = SharedGitPackCache { shards };

        gix::odb::pack::cache::DecodeEntry::put(
            &mut first,
            1,
            42,
            b"base",
            gix::objs::Kind::Tree,
            3,
        );
        let mut out = Vec::new();
        let metadata = gix::odb::pack::cache::DecodeEntry::get(&mut second, 1, 42, &mut out);

        assert_eq!(metadata, Some((gix::objs::Kind::Tree, 3)));
        assert_eq!(out, b"base");
    }

    #[test]
    fn concurrent_git_object_writes_are_deduplicated() {
        use std::sync::Barrier;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;
        use std::thread;
        use std::time::Duration;

        let tracker = Arc::new(GitObjectWriteTracker::default());
        let barrier = Arc::new(Barrier::new(4));
        let writes = Arc::new(AtomicUsize::new(0));
        let id = gix::ObjectId::from_hex(b"1111111111111111111111111111111111111111").unwrap();
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let tracker = tracker.clone();
                let barrier = barrier.clone();
                let writes = writes.clone();
                thread::spawn(move || {
                    barrier.wait();
                    tracker
                        .write_once(id, || {
                            writes.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(Duration::from_millis(20));
                            Ok(())
                        })
                        .unwrap();
                })
            })
            .collect();

        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(writes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn write_batch_holds_one_object_store_lock() -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path().join("store");
        fs::create_dir(&store_path)?;
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(&git_repo_path, gix::hash::Kind::default());
        let backend = GitBackend::init_external(&settings, &store_path, git_repo.path())?;
        let lock_path = backend.git_object_store_lock_path();

        let mut first_batch = backend.start_write_batch();
        let second_batch = backend.start_write_batch();
        assert!(FileLock::try_lock(lock_path.clone())?.is_some());
        backend
            .write_file_bytes(RepoPath::root(), b"take the lazy lock")
            .block_on()?;
        first_batch.flush()?;
        assert!(FileLock::try_lock(lock_path.clone())?.is_none());
        first_batch.finish()?;
        assert!(FileLock::try_lock(lock_path.clone())?.is_none());
        second_batch.finish()?;
        assert!(FileLock::try_lock(lock_path)?.is_some());
        Ok(())
    }

    #[test]
    fn write_batch_packs_git_objects() -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path().join("store");
        fs::create_dir(&store_path)?;
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(&git_repo_path, gix::hash::Kind::default());
        let backend = GitBackend::init_external(&settings, &store_path, git_repo.path())?;

        let mut write_batch = backend.start_write_batch();
        let lock_path = backend.git_object_store_lock_path();
        let file_id = backend
            .write_file_bytes(RepoPath::root(), b"packed content")
            .block_on()?;
        let tree = Tree::from_sorted_entries(vec![(
            RepoPathComponentBuf::new("file").unwrap(),
            TreeValue::File {
                id: file_id.clone(),
                executable: false,
                copy_id: CopyId::placeholder(),
            },
        )]);
        let tree_id = backend.write_tree(RepoPath::root(), &tree).block_on()?;

        let file_oid = validate_git_object_id(&git_repo, &file_id)?;
        let tree_oid = validate_git_object_id(&git_repo, &tree_id)?;
        assert!(!loose_object_path(&backend.objects_dir, &file_oid).exists());
        assert!(!loose_object_path(&backend.objects_dir, &tree_oid).exists());
        assert_eq!(
            backend
                .read_file_bytes(RepoPath::root(), &file_id)
                .block_on()?,
            b"packed content"
        );
        assert_eq!(
            backend.read_tree(RepoPath::root(), &tree_id).block_on()?,
            tree
        );

        for i in 0..MIN_BUFFERED_OBJECTS_PER_PACK {
            backend
                .write_file_bytes(RepoPath::root(), format!("object {i}").as_bytes())
                .block_on()?;
        }
        let commit = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(tree_id.clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::from_hex("1111eeee1111eeee1111eeee1111eeee"),
            description: "packed tree".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };
        let (commit_id, _) = backend.write_commit(commit, None).block_on()?;
        let second_file_id = backend
            .write_file_bytes(RepoPath::root(), b"second packed content")
            .block_on()?;
        let second_tree = Tree::from_sorted_entries(vec![(
            RepoPathComponentBuf::new("file").unwrap(),
            TreeValue::File {
                id: second_file_id,
                executable: false,
                copy_id: CopyId::placeholder(),
            },
        )]);
        let second_tree_id = backend
            .write_tree(RepoPath::root(), &second_tree)
            .block_on()?;
        let second_commit = Commit {
            parents: vec![commit_id.clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(second_tree_id.clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::from_hex("2222dddd2222dddd2222dddd2222dddd"),
            description: "second packed tree".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };
        let (second_commit_id, _) = backend.write_commit(second_commit, None).block_on()?;

        assert!(!loose_object_path(&backend.objects_dir, &file_oid).exists());
        assert!(!loose_object_path(&backend.objects_dir, &tree_oid).exists());
        let fresh_repo =
            gix::ThreadSafeRepository::open_opts(&git_repo_path, open_options())?.to_thread_local();
        let keep_ref_name = format!("{NO_GC_REF_NAMESPACE}{commit_id}");
        let second_keep_ref_name = format!("{NO_GC_REF_NAMESPACE}{second_commit_id}");
        assert!(fresh_repo.find_object(file_oid).is_err());
        assert!(fresh_repo.find_object(tree_oid).is_err());
        assert!(
            fresh_repo
                .find_object(validate_git_object_id(&fresh_repo, &commit_id)?)
                .is_err()
        );
        assert!(
            fresh_repo
                .find_object(validate_git_object_id(&fresh_repo, &second_tree_id)?)
                .is_err()
        );
        assert!(
            fresh_repo
                .find_object(validate_git_object_id(&fresh_repo, &second_commit_id)?)
                .is_err()
        );
        assert!(fresh_repo.find_reference(keep_ref_name.as_str()).is_err());
        assert!(
            fresh_repo
                .find_reference(second_keep_ref_name.as_str())
                .is_err()
        );
        write_batch.flush()?;
        assert!(FileLock::try_lock(lock_path)?.is_none());
        let pack_dir = backend.objects_dir.join("pack");
        let pack_files = fs::read_dir(&pack_dir)?
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension() == Some(OsStr::new("pack")))
            .collect_vec();
        let index_files = fs::read_dir(&pack_dir)?
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension() == Some(OsStr::new("idx")))
            .collect_vec();
        assert_eq!(pack_files.len(), 1);
        assert_eq!(index_files.len(), 1);
        assert_eq!(pack_files[0].file_stem(), index_files[0].file_stem());
        let verify_output = Command::new(&backend.git_executable)
            .arg("verify-pack")
            .arg(&index_files[0])
            .output()?;
        assert!(
            verify_output.status.success(),
            "git verify-pack failed: {}",
            String::from_utf8_lossy(&verify_output.stderr)
        );
        let fresh_repo =
            gix::ThreadSafeRepository::open_opts(&git_repo_path, open_options())?.to_thread_local();
        let mut blob = fresh_repo.find_object(file_oid)?.try_into_blob()?;
        assert_eq!(blob.take_data(), b"packed content");
        assert!(fresh_repo.find_object(tree_oid)?.try_into_tree().is_ok());
        assert!(
            fresh_repo
                .find_object(validate_git_object_id(&fresh_repo, &second_tree_id)?)?
                .try_into_tree()
                .is_ok()
        );
        assert!(
            fresh_repo
                .find_object(validate_git_object_id(&fresh_repo, &commit_id)?)?
                .try_into_commit()
                .is_ok()
        );
        assert!(fresh_repo.find_reference(keep_ref_name.as_str()).is_ok());
        assert!(
            fresh_repo
                .find_reference(second_keep_ref_name.as_str())
                .is_ok()
        );
        let reopened_backend = GitBackend::load(&settings, &store_path)?;
        let reopened_commit = reopened_backend.read_commit(&commit_id).block_on()?;
        assert_eq!(
            reopened_commit.change_id,
            ChangeId::from_hex("1111eeee1111eeee1111eeee1111eeee")
        );
        assert_eq!(reopened_commit.root_tree, Merge::resolved(tree_id));
        let reopened_second_commit = reopened_backend.read_commit(&second_commit_id).block_on()?;
        assert_eq!(
            reopened_second_commit.change_id,
            ChangeId::from_hex("2222dddd2222dddd2222dddd2222dddd")
        );
        assert_eq!(
            reopened_second_commit.root_tree,
            Merge::resolved(second_tree_id)
        );
        write_batch.finish()?;
        Ok(())
    }

    #[test]
    fn git_pack_index_v2_layout() -> TestResult {
        let small_id = gix::hash::ObjectId::from_hex(b"00112233445566778899aabbccddeeff00112233")?;
        let large_id = gix::hash::ObjectId::from_hex(b"ffeeddccbbaa99887766554433221100ffeeddcc")?;
        let pack_hash = gix::hash::ObjectId::from_hex(b"1234567890abcdef1234567890abcdef12345678")?;
        let mut file = NamedTempFile::new()?;
        write_git_pack_index(
            vec![
                GitPackIndexEntry {
                    id: large_id,
                    offset: 0x8000_0000,
                    crc32: 0xfeed_beef,
                },
                GitPackIndexEntry {
                    id: small_id,
                    offset: 12,
                    crc32: 0x1234_5678,
                },
            ],
            &pack_hash,
            file.as_file_mut(),
        )?;
        let bytes = fs::read(file.path())?;

        let fanout_end = 8 + 256 * 4;
        assert_eq!(&bytes[..8], b"\xfftOc\0\0\0\x02");
        assert_eq!(
            u32::from_be_bytes(bytes[fanout_end - 4..fanout_end].try_into()?),
            2
        );
        let ids_end = fanout_end + 2 * HASH_LENGTH;
        assert_eq!(
            &bytes[fanout_end..fanout_end + HASH_LENGTH],
            small_id.as_bytes()
        );
        assert_eq!(
            &bytes[fanout_end + HASH_LENGTH..ids_end],
            large_id.as_bytes()
        );
        let crc_end = ids_end + 2 * 4;
        assert_eq!(
            &bytes[ids_end..crc_end],
            b"\x12\x34\x56\x78\xfe\xed\xbe\xef"
        );
        let offsets_end = crc_end + 2 * 4;
        assert_eq!(&bytes[crc_end..offsets_end], b"\0\0\0\x0c\x80\0\0\0");
        assert_eq!(
            &bytes[offsets_end..offsets_end + 8],
            &0x8000_0000_u64.to_be_bytes()
        );
        let pack_hash_start = offsets_end + 8;
        assert_eq!(
            &bytes[pack_hash_start..pack_hash_start + HASH_LENGTH],
            pack_hash.as_bytes()
        );
        let index_hash_start = pack_hash_start + HASH_LENGTH;
        let mut hasher = gix::hash::hasher(pack_hash.kind());
        hasher.update(&bytes[..index_hash_start]);
        assert_eq!(
            &bytes[index_hash_start..],
            hasher.try_finalize()?.as_bytes()
        );
        Ok(())
    }

    #[test]
    fn small_write_batch_keeps_loose_objects() -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path().join("store");
        fs::create_dir(&store_path)?;
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(&git_repo_path, gix::hash::Kind::default());
        let backend = GitBackend::init_external(&settings, &store_path, git_repo.path())?;

        let write_batch = backend.start_write_batch();
        let file_id = backend
            .write_file_bytes(RepoPath::root(), b"small batch")
            .block_on()?;
        let symlink_id = backend
            .write_symlink(RepoPath::root(), "buffered-target")
            .block_on()?;
        assert_eq!(
            backend
                .read_symlink(RepoPath::root(), &symlink_id)
                .block_on()?,
            "buffered-target"
        );
        let file_oid = validate_git_object_id(&git_repo, &file_id)?;
        write_batch.finish()?;

        assert!(loose_object_path(&backend.objects_dir, &file_oid).exists());
        assert_eq!(fs::read_dir(backend.objects_dir.join("pack"))?.count(), 0);
        let reopened_backend = GitBackend::load(&settings, &store_path)?;
        assert_eq!(
            reopened_backend
                .read_symlink(RepoPath::root(), &symlink_id)
                .block_on()?,
            "buffered-target"
        );
        Ok(())
    }

    #[test]
    fn active_write_batch_buffers_existing_loose_object() -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path().join("store");
        fs::create_dir(&store_path)?;
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(&git_repo_path);
        let backend = GitBackend::init_external(&settings, &store_path, git_repo.path())?;
        let contents = b"already loose";
        let file_id = backend
            .write_file_bytes(RepoPath::root(), contents)
            .block_on()?;
        let file_oid = validate_git_object_id(&file_id)?;
        assert!(loose_object_path(&backend.objects_dir, &file_oid).exists());
        drop(backend);

        // A fresh backend has no in-memory knowledge of the loose object. Active
        // batches should still buffer it without probing the filesystem first.
        let backend = GitBackend::load(&settings, &store_path)?;
        let write_batch = backend.start_write_batch();
        let duplicate_id = backend
            .write_file_bytes(RepoPath::root(), contents)
            .block_on()?;
        assert_eq!(duplicate_id, file_id);
        assert!(
            backend
                .buffered_objects
                .lock()
                .unwrap()
                .contains_key(&file_oid)
        );
        write_batch.finish()?;
        Ok(())
    }

    #[test]
    fn write_conflicted_commit_with_buffered_trees() -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path().join("store");
        fs::create_dir(&store_path)?;
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(&git_repo_path, gix::hash::Kind::default());
        let backend = GitBackend::init_external(&settings, &store_path, git_repo.path())?;

        let write_batch = backend.start_write_batch();
        let mut tree_ids = Vec::new();
        for i in 0..3 {
            let file_id = backend
                .write_file_bytes(RepoPath::root(), format!("side {i}").as_bytes())
                .block_on()?;
            let tree = Tree::from_sorted_entries(vec![(
                RepoPathComponentBuf::new("file").unwrap(),
                TreeValue::File {
                    id: file_id,
                    executable: false,
                    copy_id: CopyId::placeholder(),
                },
            )]);
            tree_ids.push(backend.write_tree(RepoPath::root(), &tree).block_on()?);
        }
        let root_tree = Merge::from_vec(tree_ids);
        let commit = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: root_tree.clone(),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::from_hex("1111eeee1111eeee1111eeee1111eeee"),
            description: "conflicted".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let (commit_id, _) = backend.write_commit(commit, None).block_on()?;
        write_batch.finish()?;

        let stored_commit = backend.read_commit(&commit_id).block_on()?;
        assert_eq!(stored_commit.root_tree, root_tree);
        Ok(())
    }

    fn git_init(directory: impl AsRef<Path>, object_hash: gix::hash::Kind) -> gix::Repository {
        gix::ThreadSafeRepository::init_opts(
            directory,
            gix::create::Kind::WithWorktree,
            gix::create::Options {
                object_hash: Some(object_hash),
                ..Default::default()
            },
            open_options(),
        )
        .unwrap()
        .to_thread_local()
    }

    #[test]
    fn open_git_repo_at_workdir() -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path().join("store");
        fs::create_dir(&store_path)?;

        let git_repo_path = temp_dir.path().join("git1");
        let git_repo = git_init(&git_repo_path, gix::hash::Kind::default());
        let other_git_repo_path = temp_dir.path().join("git2");
        let _other_git_repo = git_init(&other_git_repo_path, gix::hash::Kind::default());

        let worktree_dir = temp_dir.path().join("git1-wt");
        let output = Command::new("git")
            .args(["worktree", "add", "--orphan"])
            .arg(&worktree_dir)
            .current_dir(&git_repo_path)
            .output()?;
        assert!(output.status.success(), "{output:?}");

        let backend = GitBackend::init_external(&settings, &store_path, git_repo.path())?;

        assert_matches!(
            backend.open_git_repo_at_workdir(&git_repo_path),
            Ok(repo) if repo.workdir() == Some(backend.git_repo().workdir().unwrap())
        );
        assert_matches!(
            backend.open_git_repo_at_workdir(&worktree_dir),
            Ok(repo) if repo.workdir() == Some(worktree_dir.as_ref())
        );
        assert_matches!(
            backend.open_git_repo_at_workdir(&temp_dir.path().join("unknown")),
            Err(GitRepoAtWorkdirError::NotFound { .. })
        );
        assert_matches!(
            backend.open_git_repo_at_workdir(&other_git_repo_path),
            Err(GitRepoAtWorkdirError::Unrelated { .. })
        );

        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn read_plain_git_commit(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(git_repo_path, object_hash);

        // Add a commit with some files in
        let blob1 = git_repo.write_blob(b"content1")?.detach();
        let blob2 = git_repo.write_blob(b"normal")?.detach();
        let mut dir_tree_editor = git_repo.empty_tree().edit()?;
        dir_tree_editor.upsert("normal", gix::object::tree::EntryKind::Blob, blob1)?;
        dir_tree_editor.upsert("symlink", gix::object::tree::EntryKind::Link, blob2)?;
        let dir_tree_id = dir_tree_editor.write()?.detach();
        let mut root_tree_builder = git_repo.empty_tree().edit()?;
        root_tree_builder.upsert("dir", gix::object::tree::EntryKind::Tree, dir_tree_id)?;
        let root_tree_id = root_tree_builder.write()?.detach();
        let git_author = gix::actor::Signature {
            name: "git author".into(),
            email: "git.author@example.com".into(),
            time: gix::date::Time::new(1000, 60 * 60),
        };
        let git_committer = gix::actor::Signature {
            name: "git committer".into(),
            email: "git.committer@example.com".into(),
            time: gix::date::Time::new(2000, -480 * 60),
        };
        let git_commit_id = git_repo
            .commit_as(
                git_committer.to_ref(&mut TimeBuf::default()),
                git_author.to_ref(&mut TimeBuf::default()),
                "refs/heads/dummy",
                "git commit message",
                root_tree_id,
                [] as [gix::ObjectId; 0],
            )?
            .detach();
        git_repo.find_reference("refs/heads/dummy")?.delete()?;
        // The change id is the leading reverse bits of the commit id
        let (commit_id, change_id) = match object_hash {
            gix::hash::Kind::Sha1 => (
                CommitId::from_hex("efdcea5ca4b3658149f899ca7feee6876d077263"),
                ChangeId::from_hex("c64ee0b6e16777fe53991f9281a6cd25"),
            ),
            gix::hash::Kind::Sha256 => (
                CommitId::from_hex(
                    "64366022e4938d697015b775945be93aea6d3fc221feeaf7c516420262e3fa54",
                ),
                ChangeId::from_hex("2a5fc746404268a3ef577f8443fcb657"),
            ),
            _ => unreachable!(),
        };
        // Check that the git commit above got the hash we expect
        assert_eq!(
            git_commit_id.as_bytes(),
            commit_id.as_bytes(),
            "{git_commit_id:?} vs {commit_id:?}"
        );

        // Add an empty commit on top
        let git_commit_id2 = git_repo
            .commit_as(
                git_committer.to_ref(&mut TimeBuf::default()),
                git_author.to_ref(&mut TimeBuf::default()),
                "refs/heads/dummy2",
                "git commit message 2",
                root_tree_id,
                [git_commit_id],
            )?
            .detach();
        git_repo.find_reference("refs/heads/dummy2")?.delete()?;
        let commit_id2 = CommitId::from_bytes(git_commit_id2.as_bytes());

        let backend = GitBackend::init_external(&settings, store_path, git_repo.path())?;

        // Import the head commit and its ancestors
        backend.import_head_commits([&commit_id2])?;
        // Ref should be created only for the head commit
        let git_refs = backend
            .git_repo()
            .references()?
            .prefixed("refs/jj/keep/")?
            .map(|git_ref| git_ref.unwrap().id().detach())
            .collect_vec();
        assert_eq!(git_refs, vec![git_commit_id2]);

        let commit = backend.read_commit(&commit_id).block_on()?;
        assert_eq!(&commit.change_id, &change_id);
        assert_eq!(
            commit.parents,
            vec![CommitId::from_bytes(object_hash.null_ref().as_bytes())]
        );
        assert_eq!(commit.predecessors, vec![]);
        assert_eq!(
            commit.root_tree,
            Merge::resolved(TreeId::from_bytes(root_tree_id.as_bytes()))
        );
        assert_eq!(commit.description, "git commit message");
        assert_eq!(commit.author.name, "git author");
        assert_eq!(commit.author.email, "git.author@example.com");
        assert_eq!(
            commit.author.timestamp.timestamp,
            MillisSinceEpoch(1000 * 1000)
        );
        assert_eq!(commit.author.timestamp.tz_offset, 60);
        assert_eq!(commit.committer.name, "git committer");
        assert_eq!(commit.committer.email, "git.committer@example.com");
        assert_eq!(
            commit.committer.timestamp.timestamp,
            MillisSinceEpoch(2000 * 1000)
        );
        assert_eq!(commit.committer.timestamp.tz_offset, -480);

        let root_tree = backend
            .read_tree(
                RepoPath::root(),
                &TreeId::from_bytes(root_tree_id.as_bytes()),
            )
            .block_on()?;
        let mut root_entries = root_tree.entries();
        let dir = root_entries.next().unwrap();
        assert_eq!(root_entries.next(), None);
        assert_eq!(dir.name().as_internal_str(), "dir");
        assert_eq!(
            dir.value(),
            &TreeValue::Tree(TreeId::from_bytes(dir_tree_id.as_bytes()))
        );

        let dir_tree = backend
            .read_tree(
                RepoPath::from_internal_string("dir")?,
                &TreeId::from_bytes(dir_tree_id.as_bytes()),
            )
            .block_on()?;
        let mut entries = dir_tree.entries();
        let file = entries.next().unwrap();
        let symlink = entries.next().unwrap();
        assert_eq!(entries.next(), None);
        assert_eq!(file.name().as_internal_str(), "normal");
        assert_eq!(
            file.value(),
            &TreeValue::File {
                id: FileId::from_bytes(blob1.as_bytes()),
                executable: false,
                copy_id: CopyId::placeholder(),
            }
        );
        assert_eq!(symlink.name().as_internal_str(), "symlink");
        assert_eq!(
            symlink.value(),
            &TreeValue::Symlink(SymlinkId::from_bytes(blob2.as_bytes()))
        );

        let commit2 = backend.read_commit(&commit_id2).block_on()?;
        assert_eq!(commit2.parents, vec![commit_id.clone()]);
        assert_eq!(commit.predecessors, vec![]);
        assert_eq!(
            commit.root_tree,
            Merge::resolved(TreeId::from_bytes(root_tree_id.as_bytes()))
        );
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn read_git_commit_without_importing(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(&git_repo_path, object_hash);

        let signature = gix::actor::Signature {
            name: GIT_USER.into(),
            email: GIT_EMAIL.into(),
            time: gix::date::Time::now_utc(),
        };
        let empty_tree_id = gix::ObjectId::empty_tree(git_repo.object_hash());
        let git_commit_id = git_repo.commit_as(
            signature.to_ref(&mut TimeBuf::default()),
            signature.to_ref(&mut TimeBuf::default()),
            "refs/heads/main",
            "git commit message",
            empty_tree_id,
            [] as [gix::ObjectId; 0],
        )?;

        let backend = GitBackend::init_external(&settings, store_path, git_repo.path())?;

        // read_commit() without import_head_commits() works as of now. This might be
        // changed later.
        assert!(
            backend
                .read_commit(&CommitId::from_bytes(git_commit_id.as_bytes()))
                .block_on()
                .is_ok()
        );
        assert!(
            backend
                .cached_extra_metadata_table()?
                .get_value(git_commit_id.as_bytes())
                .is_some(),
            "extra metadata should have been be created"
        );
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn read_signed_git_commit(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(git_repo_path, object_hash);

        let signature = gix::actor::Signature {
            name: GIT_USER.into(),
            email: GIT_EMAIL.into(),
            time: gix::date::Time::now_utc(),
        };
        let empty_tree_id = gix::ObjectId::empty_tree(git_repo.object_hash());

        let secure_sig =
            "here are some ASCII bytes to be used as a test signature\n\ndefinitely not PGP\n";

        let mut commit = gix::objs::Commit {
            tree: empty_tree_id,
            parents: smallvec::SmallVec::new(),
            author: signature.clone(),
            committer: signature.clone(),
            encoding: None,
            message: "git commit message".into(),
            extra_headers: Vec::new(),
        };

        let mut commit_buf = Vec::new();
        commit.write_to(&mut commit_buf)?;
        let commit_str = str::from_utf8(&commit_buf)?;

        let field = signature_field_name(object_hash);
        commit.extra_headers.push((field.into(), secure_sig.into()));

        let git_commit_id = git_repo.write_object(&commit)?;

        let backend = GitBackend::init_external(&settings, store_path, git_repo.path())?;

        let commit = backend
            .read_commit(&CommitId::from_bytes(git_commit_id.as_bytes()))
            .block_on()?;

        let sig = commit.secure_sig.expect("failed to read the signature");

        // converting to string for nicer assert diff
        assert_eq!(str::from_utf8(&sig.sig)?, secure_sig);
        assert_eq!(str::from_utf8(&sig.data)?, commit_str);
        Ok(())
    }

    #[test]
    fn change_id_parsing() {
        let id = |commit_object_bytes: &[u8]| {
            extract_change_id_from_commit(
                &CommitRef::from_bytes(commit_object_bytes, gix::hash::Kind::Sha1).unwrap(),
            )
        };

        let commit_with_id = indoc! {b"
            tree 126799bf8058d1b5c531e93079f4fe79733920dd
            parent bd50783bdf38406dd6143475cd1a3c27938db2ee
            author JJ Fan <jjfan@example.com> 1757112665 -0700
            committer JJ Fan <jjfan@example.com> 1757359886 -0700
            extra-header blah
            change-id lkonztmnvsxytrwkxpvuutrmompwylqq

            test-commit
        "};
        insta::assert_compact_debug_snapshot!(
            id(commit_with_id),
            @r#"Some(ChangeId("efbc06dc4721683f2a45568dbda31e99"))"#
        );

        let commit_without_id = indoc! {b"
            tree 126799bf8058d1b5c531e93079f4fe79733920dd
            parent bd50783bdf38406dd6143475cd1a3c27938db2ee
            author JJ Fan <jjfan@example.com> 1757112665 -0700
            committer JJ Fan <jjfan@example.com> 1757359886 -0700
            extra-header blah

            no id in header
        "};
        insta::assert_compact_debug_snapshot!(
            id(commit_without_id),
            @"None"
        );

        let commit = indoc! {b"
            tree 126799bf8058d1b5c531e93079f4fe79733920dd
            parent bd50783bdf38406dd6143475cd1a3c27938db2ee
            author JJ Fan <jjfan@example.com> 1757112665 -0700
            committer JJ Fan <jjfan@example.com> 1757359886 -0700
            change-id lkonztmnvsxytrwkxpvuutrmompwylqq
            extra-header blah
            change-id abcabcabcabcabcabcabcabcabcabcab

            valid change id first
        "};
        insta::assert_compact_debug_snapshot!(
            id(commit),
            @r#"Some(ChangeId("efbc06dc4721683f2a45568dbda31e99"))"#
        );

        // We only look at the first change id if multiple are present, so this should
        // error
        let commit = indoc! {b"
            tree 126799bf8058d1b5c531e93079f4fe79733920dd
            parent bd50783bdf38406dd6143475cd1a3c27938db2ee
            author JJ Fan <jjfan@example.com> 1757112665 -0700
            committer JJ Fan <jjfan@example.com> 1757359886 -0700
            change-id abcabcabcabcabcabcabcabcabcabcab
            extra-header blah
            change-id lkonztmnvsxytrwkxpvuutrmompwylqq

            valid change id first
        "};
        insta::assert_compact_debug_snapshot!(
            id(commit),
            @"None"
        );
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn round_trip_change_id_via_git_header(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();

        let store_path = temp_dir.path().join("store");
        fs::create_dir(&store_path)?;
        let empty_store_path = temp_dir.path().join("empty_store");
        fs::create_dir(&empty_store_path)?;
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(git_repo_path, object_hash);

        let backend = GitBackend::init_external(&settings, &store_path, git_repo.path())?;
        let original_change_id = ChangeId::from_hex("1111eeee1111eeee1111eeee1111eeee");
        let commit = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(backend.empty_tree_id().clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: original_change_id.clone(),
            description: "initial".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let write_batch = backend.start_write_batch();
        let (initial_commit_id, _init_commit) = backend.write_commit(commit, None).block_on()?;
        let commit = backend.read_commit(&initial_commit_id).block_on()?;
        assert_eq!(
            commit.change_id, original_change_id,
            "The change-id header did not roundtrip"
        );
        write_batch.finish()?;

        // Because of how change ids are also persisted in extra proto files,
        // initialize a new store without those files, but reuse the same git
        // storage. This change-id must be derived from the git commit header.
        let no_extra_backend =
            GitBackend::init_external(&settings, &empty_store_path, git_repo.path())?;
        let no_extra_commit = no_extra_backend
            .read_commit(&initial_commit_id)
            .block_on()?;

        assert_eq!(
            no_extra_commit.change_id, original_change_id,
            "The change-id header did not roundtrip"
        );
        Ok(())
    }

    #[test]
    fn read_empty_string_placeholder() {
        let git_signature1 = gix::actor::Signature {
            name: EMPTY_STRING_PLACEHOLDER.into(),
            email: "git.author@example.com".into(),
            time: gix::date::Time::new(1000, 60 * 60),
        };
        let signature1 = signature_from_git(git_signature1.to_ref(&mut TimeBuf::default()));
        assert!(signature1.name.is_empty());
        assert_eq!(signature1.email, "git.author@example.com");
        let git_signature2 = gix::actor::Signature {
            name: "git committer".into(),
            email: EMPTY_STRING_PLACEHOLDER.into(),
            time: gix::date::Time::new(2000, -480 * 60),
        };
        let signature2 = signature_from_git(git_signature2.to_ref(&mut TimeBuf::default()));
        assert_eq!(signature2.name, "git committer");
        assert!(signature2.email.is_empty());
    }

    #[test]
    fn write_empty_string_placeholder() {
        let signature1 = Signature {
            name: "".to_string(),
            email: "someone@example.com".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        };
        let git_signature1 = signature_to_git(&signature1);
        assert_eq!(git_signature1.name, EMPTY_STRING_PLACEHOLDER);
        assert_eq!(git_signature1.email, "someone@example.com");
        let signature2 = Signature {
            name: "Someone".to_string(),
            email: "".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        };
        let git_signature2 = signature_to_git(&signature2);
        assert_eq!(git_signature2.name, "Someone");
        assert_eq!(git_signature2.email, EMPTY_STRING_PLACEHOLDER);
    }

    /// Test that parents get written correctly
    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn git_commit_parents(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(&git_repo_path, object_hash);

        let backend = GitBackend::init_external(&settings, store_path, git_repo.path())?;
        let mut commit = Commit {
            parents: vec![],
            predecessors: vec![],
            root_tree: Merge::resolved(backend.empty_tree_id().clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::from_hex("abc123"),
            description: "".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let write_commit = |commit: Commit| -> BackendResult<(CommitId, Commit)> {
            backend.write_commit(commit, None).block_on()
        };

        // No parents
        commit.parents = vec![];
        assert_matches!(
            write_commit(commit.clone()),
            Err(BackendError::Other(err)) if err.to_string().contains("no parents")
        );

        // Only root commit as parent
        commit.parents = vec![backend.root_commit_id().clone()];
        let first_id = write_commit(commit.clone())?.0;
        let first_commit = backend.read_commit(&first_id).block_on()?;
        assert_eq!(first_commit, commit);
        let first_git_commit = git_repo.find_commit(git_id(&first_id))?;
        assert!(first_git_commit.parent_ids().collect_vec().is_empty());

        // Only non-root commit as parent
        commit.parents = vec![first_id.clone()];
        let second_id = write_commit(commit.clone())?.0;
        let second_commit = backend.read_commit(&second_id).block_on()?;
        assert_eq!(second_commit, commit);
        let second_git_commit = git_repo.find_commit(git_id(&second_id))?;
        assert_eq!(
            second_git_commit.parent_ids().collect_vec(),
            vec![git_id(&first_id)]
        );

        // Merge commit
        commit.parents = vec![first_id.clone(), second_id.clone()];
        let merge_id = write_commit(commit.clone())?.0;
        let merge_commit = backend.read_commit(&merge_id).block_on()?;
        assert_eq!(merge_commit, commit);
        let merge_git_commit = git_repo.find_commit(git_id(&merge_id))?;
        assert_eq!(
            merge_git_commit.parent_ids().collect_vec(),
            vec![git_id(&first_id), git_id(&second_id)]
        );

        // Merge commit with root as one parent
        commit.parents = vec![first_id, backend.root_commit_id().clone()];
        assert_matches!(
            write_commit(commit),
            Err(BackendError::Unsupported(message)) if message.contains("root commit")
        );
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn write_tree_conflicts(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(&git_repo_path, object_hash);

        let backend = GitBackend::init_external(&settings, store_path, git_repo.path())?;
        let create_tree = |i| {
            let blob_id = git_repo.write_blob(format!("content {i}")).unwrap();
            let mut tree_builder = git_repo.empty_tree().edit().unwrap();
            tree_builder
                .upsert(
                    format!("file{i}"),
                    gix::object::tree::EntryKind::Blob,
                    blob_id,
                )
                .unwrap();
            TreeId::from_bytes(tree_builder.write().unwrap().as_bytes())
        };

        let root_tree = Merge::from_removes_adds(
            vec![create_tree(0), create_tree(1)],
            vec![create_tree(2), create_tree(3), create_tree(4)],
        );
        let mut commit = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: root_tree.clone(),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::from_hex("abc123"),
            description: "".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let write_commit = |commit: Commit| -> BackendResult<(CommitId, Commit)> {
            backend.write_commit(commit, None).block_on()
        };

        // When writing a tree-level conflict, the root tree on the git side has the
        // individual trees as subtrees.
        let read_commit_id = write_commit(commit.clone())?.0;
        let read_commit = backend.read_commit(&read_commit_id).block_on()?;
        assert_eq!(read_commit, commit);
        let git_commit = git_repo.find_commit(gix::ObjectId::from_bytes_or_panic(
            read_commit_id.as_bytes(),
        ))?;
        let git_tree = git_repo.find_tree(git_commit.tree_id()?)?;
        let jj_conflict_entries = git_tree
            .iter()
            .map(Result::unwrap)
            .filter(|entry| {
                entry.filename().starts_with(b".jjconflict")
                    || entry.filename() == JJ_CONFLICT_README_FILE_NAME
            })
            .collect_vec();
        assert!(
            jj_conflict_entries
                .iter()
                .filter(|entry| entry.filename() != JJ_CONFLICT_README_FILE_NAME)
                .all(|entry| entry.mode().value() == 0o040000)
        );
        let mut iter = jj_conflict_entries.iter();
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b".jjconflict-base-0");
        assert_eq!(
            entry.id().as_bytes(),
            root_tree.get_remove(0).unwrap().as_bytes()
        );
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b".jjconflict-base-1");
        assert_eq!(
            entry.id().as_bytes(),
            root_tree.get_remove(1).unwrap().as_bytes()
        );
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b".jjconflict-side-0");
        assert_eq!(
            entry.id().as_bytes(),
            root_tree.get_add(0).unwrap().as_bytes()
        );
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b".jjconflict-side-1");
        assert_eq!(
            entry.id().as_bytes(),
            root_tree.get_add(1).unwrap().as_bytes()
        );
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b".jjconflict-side-2");
        assert_eq!(
            entry.id().as_bytes(),
            root_tree.get_add(2).unwrap().as_bytes()
        );
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b"JJ-CONFLICT-README");
        assert_eq!(entry.mode().value(), 0o100644);
        assert!(iter.next().is_none());

        // When writing a single tree using the new format, it's represented by a
        // regular git tree.
        commit.root_tree = Merge::resolved(create_tree(5));
        let read_commit_id = write_commit(commit.clone())?.0;
        let read_commit = backend.read_commit(&read_commit_id).block_on()?;
        assert_eq!(read_commit, commit);
        let git_commit = git_repo.find_commit(gix::ObjectId::from_bytes_or_panic(
            read_commit_id.as_bytes(),
        ))?;
        assert_eq!(
            Merge::resolved(TreeId::from_bytes(git_commit.tree_id()?.as_bytes())),
            commit.root_tree
        );
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn commit_has_ref(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let backend = GitBackend::init_internal(&settings, temp_dir.path(), object_hash)?;
        let git_repo = backend.git_repo();
        let signature = Signature {
            name: "Someone".to_string(),
            email: "someone@example.com".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        };
        let commit = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(backend.empty_tree_id().clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::new(vec![42; 16]),
            description: "initial".to_string(),
            author: signature.clone(),
            committer: signature,
            secure_sig: None,
        };
        let commit_id = backend.write_commit(commit, None).block_on()?.0;
        let git_refs = git_repo.references()?;
        let git_ref_ids: Vec<_> = git_refs
            .prefixed("refs/jj/keep/")?
            .map(|x| x.unwrap().id().detach())
            .collect();
        assert!(git_ref_ids.iter().any(|id| *id == git_id(&commit_id)));

        // Concurrently-running GC deletes the ref, leaving the extra metadata.
        for git_ref in git_refs.prefixed("refs/jj/keep/")? {
            git_ref.unwrap().delete().unwrap();
        }
        // Re-imported commit should have new ref.
        backend.import_head_commits([&commit_id])?;
        let git_refs = git_repo.references()?;
        let git_ref_ids: Vec<_> = git_refs
            .prefixed("refs/jj/keep/")?
            .map(|x| x.unwrap().id().detach())
            .collect();
        assert!(git_ref_ids.iter().any(|id| *id == git_id(&commit_id)));
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn import_head_commits_duplicates(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let backend = GitBackend::init_internal(&settings, temp_dir.path(), object_hash)?;
        let git_repo = backend.git_repo();

        let signature = gix::actor::Signature {
            name: GIT_USER.into(),
            email: GIT_EMAIL.into(),
            time: gix::date::Time::now_utc(),
        };
        let empty_tree_id = gix::ObjectId::empty_tree(git_repo.object_hash());
        let git_commit_id = git_repo
            .commit_as(
                signature.to_ref(&mut TimeBuf::default()),
                signature.to_ref(&mut TimeBuf::default()),
                "refs/heads/main",
                "git commit message",
                empty_tree_id,
                [] as [gix::ObjectId; 0],
            )?
            .detach();
        let commit_id = CommitId::from_bytes(git_commit_id.as_bytes());

        // Ref creation shouldn't fail because of duplicated head ids.
        backend.import_head_commits([&commit_id, &commit_id])?;
        assert!(
            git_repo
                .references()?
                .prefixed("refs/jj/keep/")?
                .any(|git_ref| git_ref.unwrap().id().detach() == git_commit_id)
        );
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn overlapping_git_commit_id(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let backend = GitBackend::init_internal(&settings, temp_dir.path(), object_hash)?;
        let commit1 = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(backend.empty_tree_id().clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::from_hex("7f0a7ce70354b22efcccf7bf144017c4"),
            description: "initial".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let write_commit = |commit: Commit| -> BackendResult<(CommitId, Commit)> {
            backend.write_commit(commit, None).block_on()
        };

        let (commit_id1, mut commit2) = write_commit(commit1)?;
        commit2.predecessors.push(commit_id1.clone());
        // `write_commit` should prevent the ids from being the same by changing the
        // committer timestamp of the commit it actually writes.
        let (commit_id2, mut actual_commit2) = write_commit(commit2.clone())?;
        // The returned matches the ID
        assert_eq!(backend.read_commit(&commit_id2).block_on()?, actual_commit2);
        assert_ne!(commit_id2, commit_id1);
        // The committer timestamp should differ
        assert_ne!(
            actual_commit2.committer.timestamp.timestamp,
            commit2.committer.timestamp.timestamp
        );
        // The rest of the commit should be the same
        actual_commit2.committer.timestamp.timestamp = commit2.committer.timestamp.timestamp;
        assert_eq!(actual_commit2, commit2);
        Ok(())
    }

    #[test]
    fn write_signed_commit_sha1() -> TestResult {
        let (obj, sig) = write_signed_commit(gix::hash::Kind::Sha1)?;
        insta::assert_snapshot!(&obj, @"
        tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904
        author Someone <someone@example.com> 0 +0000
        committer Someone <someone@example.com> 0 +0000
        change-id xpxpxpxpxpxpxpxpxpxpxpxpxpxpxpxp
        gpgsig test sig
         hash=03feb0caccbacce2e7b7bca67f4c82292dd487e669ed8a813120c9f82d3fd0801420a1f5d05e1393abfe4e9fc662399ec4a9a1898c5f1e547e0044a52bd4bd29

        initial
        ");
        insta::assert_snapshot!(str::from_utf8(&sig.sig)?, @"
        test sig
        hash=03feb0caccbacce2e7b7bca67f4c82292dd487e669ed8a813120c9f82d3fd0801420a1f5d05e1393abfe4e9fc662399ec4a9a1898c5f1e547e0044a52bd4bd29
        ");
        insta::assert_snapshot!(str::from_utf8(&sig.data)?, @"
        tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904
        author Someone <someone@example.com> 0 +0000
        committer Someone <someone@example.com> 0 +0000
        change-id xpxpxpxpxpxpxpxpxpxpxpxpxpxpxpxp

        initial
        ");
        Ok(())
    }

    #[test]
    fn write_signed_commit_sha256() -> TestResult {
        let (obj, sig) = write_signed_commit(gix::hash::Kind::Sha256)?;
        insta::assert_snapshot!(&obj, @"
        tree 6ef19b41225c5369f1c104d45d8d85efa9b057b53b14b4b9b939dd74decc5321
        author Someone <someone@example.com> 0 +0000
        committer Someone <someone@example.com> 0 +0000
        change-id xpxpxpxpxpxpxpxpxpxpxpxpxpxpxpxp
        gpgsig-sha256 test sig
         hash=d6219e8e5169d409d115848dea4556b3accc76f3cd8dc9b128cc3fe9f71adae275f0e6ce9f98c581a89b960863b61c61b6479cdc20806009d63aecaaa82f4590

        initial
        ");
        insta::assert_snapshot!(str::from_utf8(&sig.sig)?, @"
        test sig
        hash=d6219e8e5169d409d115848dea4556b3accc76f3cd8dc9b128cc3fe9f71adae275f0e6ce9f98c581a89b960863b61c61b6479cdc20806009d63aecaaa82f4590
        ");
        insta::assert_snapshot!(str::from_utf8(&sig.data)?, @"
        tree 6ef19b41225c5369f1c104d45d8d85efa9b057b53b14b4b9b939dd74decc5321
        author Someone <someone@example.com> 0 +0000
        committer Someone <someone@example.com> 0 +0000
        change-id xpxpxpxpxpxpxpxpxpxpxpxpxpxpxpxp

        initial
        ");
        Ok(())
    }

    fn write_signed_commit(object_hash: gix::hash::Kind) -> TestResult<(String, SecureSig)> {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let backend = GitBackend::init_internal(&settings, temp_dir.path(), object_hash)?;

        let commit = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(backend.empty_tree_id().clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::new(vec![42; 16]),
            description: "initial".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let mut signer = |data: &_| {
            let hash: String = hex_util::encode_hex(&blake2b_hash(data));
            Ok(format!("test sig\nhash={hash}\n").into_bytes())
        };

        let (id, commit) = backend
            .write_commit(commit, Some(&mut signer as &mut SigningFn))
            .block_on()?;
        let returned_sig = commit.secure_sig.expect("failed to return the signature");

        let commit = backend.read_commit(&id).block_on()?;
        let sig = commit.secure_sig.expect("failed to read the signature");
        assert_eq!(&sig, &returned_sig);

        let git_repo = backend.git_repo();
        let obj = git_repo.find_object(gix::ObjectId::from_bytes_or_panic(id.as_bytes()))?;
        Ok((String::from_utf8(obj.data.clone())?, sig))
    }

    fn git_id(commit_id: &CommitId) -> gix::ObjectId {
        gix::ObjectId::from_bytes_or_panic(commit_id.as_bytes())
    }

    fn create_signature() -> Signature {
        Signature {
            name: GIT_USER.to_string(),
            email: GIT_EMAIL.to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        }
    }

    // Not using testutils::user_settings() because there is a dependency cycle
    // 'jj_lib (1) -> testutils -> jj_lib (2)' which creates another distinct
    // UserSettings type. testutils returns jj_lib (2)'s UserSettings, whereas
    // our UserSettings type comes from jj_lib (1).
    fn user_settings() -> UserSettings {
        let config = StackedConfig::with_defaults();
        UserSettings::from_config(config).unwrap()
    }
}
