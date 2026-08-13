// Copyright 2023-2025 The Jujutsu Authors
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

//! Merge trees by recursing into entries (subtrees, files)

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::vec;

use futures::FutureExt as _;
use futures::StreamExt as _;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use pollster::FutureExt as _;
use rayon::iter::IntoParallelIterator as _;
use rayon::prelude::ParallelIterator as _;

use crate::backend;
use crate::backend::BackendError;
use crate::backend::BackendResult;
use crate::backend::MergedTreeVal;
use crate::backend::MergedTreeValue;
use crate::backend::MergedTreeValueExt as _;
use crate::backend::TreeId;
use crate::backend::TreeValue;
use crate::config::ConfigGetError;
use crate::files;
use crate::files::FileMergeHunkLevel;
use crate::merge::Merge;
use crate::merge::SameChange;
use crate::merged_tree::all_merged_tree_entries;
use crate::repo_path::RepoPath;
use crate::repo_path::RepoPathBuf;
use crate::repo_path::RepoPathComponentBuf;
use crate::settings::UserSettings;
use crate::store::Store;
use crate::tree::ToTreeMergeExt as _;
use crate::tree::Tree;

/// Options for tree/file conflict resolution.
#[derive(Clone, Debug)]
pub struct MergeOptions {
    /// Granularity of hunks when merging files.
    pub hunk_level: FileMergeHunkLevel,
    /// Whether to resolve conflict that makes the same change at all sides.
    pub same_change: SameChange,
}

impl MergeOptions {
    /// Loads merge options from `settings`.
    pub fn from_settings(settings: &UserSettings) -> Result<Self, ConfigGetError> {
        Ok(Self {
            // Maybe we can add hunk-level=file to disable content merging if
            // needed. It wouldn't be translated to FileMergeHunkLevel.
            hunk_level: settings.get("merge.hunk-level")?,
            same_change: settings.get("merge.same-change")?,
        })
    }
}

/// The returned conflict will either be resolved or have the same number of
/// sides as the input.
pub async fn merge_trees(store: &Arc<Store>, merge: Merge<TreeId>) -> BackendResult<Merge<TreeId>> {
    let merge = match merge.into_resolved() {
        Ok(tree) => return Ok(Merge::resolved(tree)),
        Err(merge) => merge,
    };

    let mut merger = TreeMerger {
        store: store.clone(),
        trees_to_resolve: BTreeMap::new(),
        work: FuturesUnordered::new(),
        unstarted_work: BTreeMap::new(),
    };
    merger.enqueue_tree_read(
        RepoPathBuf::root(),
        merge.map(|tree_id| Some(TreeValue::Tree(tree_id.clone()))),
    );
    let trees = merger.merge().await?;
    Ok(trees.map(|tree| tree.id().clone()))
}

struct MergedTreeInput {
    input_trees: Merge<Tree>,
    /// Input tree terms which may still be identical to the output tree.
    reusable_input_terms: Vec<usize>,
    /// Value by which this tree was referenced from its parent.
    parent_input: Option<MergedTreeValue>,
    resolved: BTreeMap<RepoPathComponentBuf, TreeValue>,
    /// Entries that we're currently waiting for data for in order to resolve
    /// them. When this reaches zero, we're ready to write the tree(s).
    pending_entries: usize,
    conflicts: BTreeMap<RepoPathComponentBuf, MergedTreeValue>,
}

enum ClassifiedTreeEntry<'a> {
    Resolved {
        basename: RepoPathComponentBuf,
        input_value: MergedTreeVal<'a>,
        resolved_value: Option<&'a TreeValue>,
        owned_value: Option<TreeValue>,
    },
    NonTrivial {
        basename: RepoPathComponentBuf,
        value: MergedTreeValue,
    },
}

fn classify_tree_entry<'a>(
    basename: RepoPathComponentBuf,
    input_value: MergedTreeVal<'a>,
    same_change: SameChange,
) -> ClassifiedTreeEntry<'a> {
    if let Some(resolved_value) = input_value.resolve_trivial(same_change) {
        ClassifiedTreeEntry::Resolved {
            basename,
            resolved_value: *resolved_value,
            owned_value: resolved_value.cloned(),
            input_value,
        }
    } else {
        ClassifiedTreeEntry::NonTrivial {
            basename,
            value: input_value.cloned(),
        }
    }
}

fn build_conflicted_backend_trees(
    resolved: BTreeMap<RepoPathComponentBuf, TreeValue>,
    conflicts: BTreeMap<RepoPathComponentBuf, MergedTreeValue>,
    build_terms: &[bool],
) -> Vec<backend::Tree> {
    let resolved_len = resolved.len();
    let active_terms: Vec<_> = build_terms
        .iter()
        .enumerate()
        .filter_map(|(index, build)| build.then_some(index))
        .collect();
    let mut term_entries: Vec<Option<Vec<_>>> = build_terms
        .iter()
        .map(|build| build.then(|| Vec::with_capacity(resolved_len)))
        .collect();

    let mut resolved = resolved.into_iter().peekable();
    let mut conflicts = conflicts.into_iter().peekable();
    loop {
        let take_resolved = match (resolved.peek(), conflicts.peek()) {
            (Some((resolved_name, _)), Some((conflict_name, _))) => {
                match resolved_name.cmp(conflict_name) {
                    Ordering::Less => true,
                    Ordering::Greater => false,
                    Ordering::Equal => unreachable!("entry cannot be both resolved and conflicted"),
                }
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };

        if take_resolved {
            let (basename, value) = resolved.next().unwrap();
            if let Some((&last_term, other_terms)) = active_terms.split_last() {
                for &term in other_terms {
                    term_entries[term]
                        .as_mut()
                        .unwrap()
                        .push((basename.clone(), value.clone()));
                }
                term_entries[last_term]
                    .as_mut()
                    .unwrap()
                    .push((basename, value));
            }
        } else {
            let (basename, conflict) = conflicts.next().unwrap();
            assert_eq!(conflict.iter().count(), build_terms.len());
            let mut values: Vec<_> = conflict.into_iter().collect();
            let Some(last_term) = active_terms
                .iter()
                .rev()
                .copied()
                .find(|&term| values[term].is_some())
            else {
                continue;
            };
            let mut basename = Some(basename);
            for &term in &active_terms {
                let Some(value) = values[term].take() else {
                    continue;
                };
                let term_basename = if term == last_term {
                    basename.take().unwrap()
                } else {
                    basename.as_ref().unwrap().clone()
                };
                term_entries[term]
                    .as_mut()
                    .unwrap()
                    .push((term_basename, value));
            }
        }
    }

    term_entries
        .into_iter()
        .map(|entries| backend::Tree::from_sorted_entries(entries.unwrap_or_default()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::SymlinkId;

    fn name(value: &str) -> RepoPathComponentBuf {
        RepoPathComponentBuf::new(value).unwrap()
    }

    fn value(id: u8) -> TreeValue {
        TreeValue::Symlink(SymlinkId::new(vec![id]))
    }

    #[test]
    fn test_build_conflicted_backend_trees() {
        let resolved = BTreeMap::from([(name("b"), value(2)), (name("d"), value(4))]);
        let conflicts = BTreeMap::from([
            (
                name("a"),
                Merge::from_vec(vec![Some(value(10)), None, Some(value(12))]),
            ),
            (
                name("c"),
                Merge::from_vec(vec![None, Some(value(21)), Some(value(22))]),
            ),
        ]);

        let trees = build_conflicted_backend_trees(resolved, conflicts, &[true, false, true]);

        assert_eq!(
            trees,
            vec![
                backend::Tree::from_sorted_entries(vec![
                    (name("a"), value(10)),
                    (name("b"), value(2)),
                    (name("d"), value(4)),
                ]),
                backend::Tree::default(),
                backend::Tree::from_sorted_entries(vec![
                    (name("a"), value(12)),
                    (name("b"), value(2)),
                    (name("c"), value(22)),
                    (name("d"), value(4)),
                ]),
            ]
        );
    }
}

impl MergedTreeInput {
    fn new(
        input_trees: Merge<Tree>,
        reusable_input_terms: Vec<usize>,
        parent_input: Option<MergedTreeValue>,
        resolved: BTreeMap<RepoPathComponentBuf, TreeValue>,
        pending_entries: usize,
    ) -> Self {
        Self {
            input_trees,
            reusable_input_terms,
            parent_input,
            resolved,
            pending_entries,
            conflicts: BTreeMap::new(),
        }
    }

    fn mark_completed(
        &mut self,
        basename: RepoPathComponentBuf,
        input_value: MergedTreeValue,
        output_value: Option<MergedTreeValue>,
        same_change: SameChange,
    ) {
        self.pending_entries = self
            .pending_entries
            .checked_sub(1)
            .expect("No pending input for {basename:?}");
        let Some(value) = output_value else {
            // Failed file merges return the unchanged input. Move that input directly
            // into the result instead of cloning it just to compare it with itself.
            self.conflicts.insert(basename, input_value);
            return;
        };
        if input_value.num_sides() != value.num_sides() {
            // A nested merge may simplify its sides. The remaining terms no longer have
            // positional correspondence with the input trees, so don't attempt reuse.
            self.reusable_input_terms.clear();
        }
        if let Some(resolved) = value.resolve_trivial(same_change) {
            if input_value.num_sides() == value.num_sides() {
                self.reusable_input_terms
                    .retain(|&index| input_value.as_slice()[index] == *resolved);
            }
            if let Some(resolved) = resolved.as_ref() {
                self.resolved.insert(basename, resolved.clone());
            }
        } else {
            if input_value.num_sides() == value.num_sides() {
                self.reusable_input_terms
                    .retain(|&index| input_value.as_slice()[index] == value.as_slice()[index]);
            }
            self.conflicts.insert(basename, value);
        }
    }

    fn into_backend_trees(
        self,
    ) -> (
        Merge<backend::Tree>,
        Merge<Option<Tree>>,
        Option<MergedTreeValue>,
    ) {
        assert_eq!(self.pending_entries, 0);
        let input_trees = self.input_trees;

        if self.conflicts.is_empty() {
            let reusable_tree = self
                .reusable_input_terms
                .first()
                .and_then(|&index| input_trees.into_iter().nth(index));
            let backend_tree = if reusable_tree.is_some() {
                backend::Tree::default()
            } else {
                backend::Tree::from_sorted_entries(self.resolved.into_iter().collect())
            };
            (
                Merge::resolved(backend_tree),
                Merge::resolved(reusable_tree),
                self.parent_input,
            )
        } else {
            let output_num_terms = self.conflicts.first_key_value().unwrap().1.iter().count();
            let reusable_trees: Vec<_> = if output_num_terms == input_trees.iter().count() {
                let mut reusable_input_terms = self.reusable_input_terms.into_iter().peekable();
                input_trees
                    .into_iter()
                    .enumerate()
                    .map(|(index, tree)| {
                        (reusable_input_terms.next_if_eq(&index).is_some()).then_some(tree)
                    })
                    .collect()
            } else {
                // A nested merge simplified the terms, so output terms no longer have
                // positional correspondence with the input trees.
                vec![None; output_num_terms]
            };
            let build_terms: Vec<_> = reusable_trees.iter().map(Option::is_none).collect();
            let backend_trees =
                build_conflicted_backend_trees(self.resolved, self.conflicts, &build_terms);
            (
                Merge::from_vec(backend_trees),
                Merge::from_vec(reusable_trees),
                self.parent_input,
            )
        }
    }
}

/// The result from an asynchronously scheduled work item.
enum TreeMergerWorkOutput {
    /// Trees that have been read (i.e. `Read` is past tense)
    ReadTrees {
        dir: RepoPathBuf,
        result: BackendResult<(MergedTreeValue, Merge<Tree>)>,
    },
    WrittenTrees {
        dir: RepoPathBuf,
        parent_input: Option<MergedTreeValue>,
        result: BackendResult<Merge<Tree>>,
    },
    MergedFiles {
        path: RepoPathBuf,
        result: BackendResult<(MergedTreeValue, Option<MergedTreeValue>)>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum TreeMergeWorkItemKey {
    // `MergeFiles` variant before `ReadTrees` so files are polled before trees because they
    // typically take longer to process.
    MergeFiles { path: RepoPathBuf },
    ReadTrees { dir: RepoPathBuf },
}

struct TreeMerger {
    store: Arc<Store>,
    // Trees we're currently working on.
    trees_to_resolve: BTreeMap<RepoPathBuf, MergedTreeInput>,
    // Futures we're currently processing. In order to respect the backend's concurrency limit.
    work: FuturesUnordered<BoxFuture<'static, TreeMergerWorkOutput>>,
    // Futures we haven't started polling yet, in order to respect the backend's concurrency limit.
    unstarted_work: BTreeMap<TreeMergeWorkItemKey, BoxFuture<'static, TreeMergerWorkOutput>>,
}

impl TreeMerger {
    async fn merge(mut self) -> BackendResult<Merge<Tree>> {
        while let Some(work_item) = self.work.next().await {
            match work_item {
                TreeMergerWorkOutput::ReadTrees { dir, result } => {
                    let (input_value, tree) = result?;
                    let parent_input = (!dir.is_root()).then_some(input_value);
                    self.process_tree(dir, tree, parent_input);
                }
                TreeMergerWorkOutput::WrittenTrees {
                    dir,
                    parent_input,
                    result,
                } => {
                    let tree = result?;
                    if dir.is_root() {
                        assert!(parent_input.is_none());
                        assert!(self.trees_to_resolve.is_empty());
                        assert!(self.work.is_empty());
                        assert!(self.unstarted_work.is_empty());
                        return Ok(tree);
                    }
                    // Propagate the write to the parent tree, replacing empty trees by `None`.
                    let new_value = tree.map(|tree| {
                        (tree.id() != self.store.empty_tree_id())
                            .then(|| TreeValue::Tree(tree.id().clone()))
                    });
                    self.mark_completed(&dir, parent_input.unwrap(), Some(new_value));
                }
                TreeMergerWorkOutput::MergedFiles { path, result } => {
                    let (input_value, output_value) = result?;
                    self.mark_completed(&path, input_value, output_value);
                }
            }

            while self.work.len() < self.store.concurrency() {
                if let Some((_key, work)) = self.unstarted_work.pop_first() {
                    self.work.push(work);
                } else {
                    break;
                }
            }
        }

        unreachable!("There was no work item for writing the root tree");
    }

    fn process_tree(
        &mut self,
        dir: RepoPathBuf,
        tree: Merge<Tree>,
        parent_input: Option<MergedTreeValue>,
    ) {
        // First resolve trivial merges (those that we don't need to load any more data
        // for)
        let same_change = self.store.merge_options().same_change;
        let mut resolved = vec![];
        let mut non_trivial = vec![];
        let mut reusable_input_terms: Vec<_> = (0..tree.iter().count()).collect();
        // Keep only a small number of term-heavy entry merges alive at once. Large
        // rebases can have thousands of terms, so collecting the whole tree before
        // parallelizing would explode memory.
        let chunk_size = self.store.concurrency().saturating_mul(2).clamp(1, 64);
        let mut entries = all_merged_tree_entries(&tree);
        loop {
            let chunk: Vec<_> = entries
                .by_ref()
                .take(chunk_size)
                .map(|(basename, input_value)| (basename.to_owned(), input_value))
                .collect();
            if chunk.is_empty() {
                break;
            }
            let classified: Vec<_> = chunk
                .into_par_iter()
                .map(|(basename, input_value)| {
                    classify_tree_entry(basename, input_value, same_change)
                })
                .collect();
            for entry in classified {
                match entry {
                    ClassifiedTreeEntry::Resolved {
                        basename,
                        input_value,
                        resolved_value,
                        owned_value,
                    } => {
                        reusable_input_terms
                            .retain(|&index| input_value.as_slice()[index] == resolved_value);
                        if let Some(value) = owned_value {
                            resolved.push((basename, value));
                        }
                    }
                    ClassifiedTreeEntry::NonTrivial { basename, value } => {
                        non_trivial.push((basename, value));
                    }
                }
            }
        }
        drop(entries);

        // If there are no non-trivial merges, we can write the tree now.
        if non_trivial.is_empty() {
            let tree = MergedTreeInput::new(
                tree,
                reusable_input_terms,
                parent_input,
                resolved.into_iter().collect(),
                0,
            );
            let (backend_trees, reusable_trees, parent_input) = tree.into_backend_trees();
            self.enqueue_tree_write(dir, parent_input, reusable_trees, backend_trees);
            return;
        }

        let unmerged_tree = MergedTreeInput::new(
            tree,
            reusable_input_terms,
            parent_input,
            resolved.into_iter().collect(),
            non_trivial.len(),
        );
        for (basename, value) in non_trivial {
            let path = dir.join(&basename);
            if value.is_tree() {
                self.enqueue_tree_read(path, value);
            } else {
                // TODO: If it's e.g. a dir/file conflict, there's no need to try to
                // resolve it as a file. We should mark them to
                // `unmerged_tree.conflicts` instead.
                self.enqueue_file_merge(path, value);
            }
        }

        self.trees_to_resolve.insert(dir, unmerged_tree);
    }

    fn enqueue_tree_read(&mut self, dir: RepoPathBuf, value: MergedTreeValue) {
        let key = TreeMergeWorkItemKey::ReadTrees { dir: dir.clone() };
        let work_fut = read_trees(self.store.clone(), dir.clone(), value)
            .map(|result| TreeMergerWorkOutput::ReadTrees { dir, result });
        if self.work.len() < self.store.concurrency() {
            self.work.push(Box::pin(work_fut));
        } else {
            self.unstarted_work.insert(key, Box::pin(work_fut));
        }
    }

    fn enqueue_tree_write(
        &mut self,
        dir: RepoPathBuf,
        parent_input: Option<MergedTreeValue>,
        reusable_trees: Merge<Option<Tree>>,
        backend_trees: Merge<backend::Tree>,
    ) {
        let work_fut = write_trees(
            self.store.clone(),
            dir.clone(),
            reusable_trees,
            backend_trees,
        )
        .map(|result| TreeMergerWorkOutput::WrittenTrees {
            dir,
            parent_input,
            result,
        });
        // Bypass the `unstarted_work` queue because writing trees usually results in
        // saving memory (each tree gets replaced by a `TreeValue::Tree`)
        self.work.push(Box::pin(work_fut));
    }

    fn enqueue_file_merge(&mut self, path: RepoPathBuf, value: MergedTreeValue) {
        let key = TreeMergeWorkItemKey::MergeFiles { path: path.clone() };
        let work_fut = resolve_file_values_owned_on_worker(self.store.clone(), path.clone(), value)
            .map(|result| TreeMergerWorkOutput::MergedFiles { path, result });
        if self.work.len() < self.store.concurrency() {
            self.work.push(Box::pin(work_fut));
        } else {
            self.unstarted_work.insert(key, Box::pin(work_fut));
        }
    }

    fn mark_completed(
        &mut self,
        path: &RepoPath,
        input_value: MergedTreeValue,
        output_value: Option<MergedTreeValue>,
    ) {
        let (dir, basename) = path.split().unwrap();
        let tree = self.trees_to_resolve.get_mut(dir).unwrap();
        let same_change = self.store.merge_options().same_change;
        tree.mark_completed(basename.to_owned(), input_value, output_value, same_change);
        // If all entries in this tree have been processed (either resolved or still a
        // conflict), schedule the writing of the tree(s) to the backend.
        if tree.pending_entries == 0 {
            let tree = self.trees_to_resolve.remove(dir).unwrap();
            let (backend_trees, reusable_trees, parent_input) = tree.into_backend_trees();
            self.enqueue_tree_write(dir.to_owned(), parent_input, reusable_trees, backend_trees);
        }
    }
}

async fn read_trees(
    store: Arc<Store>,
    dir: RepoPathBuf,
    value: MergedTreeValue,
) -> BackendResult<(MergedTreeValue, Merge<Tree>)> {
    let trees = value
        .to_tree_merge(&store, &dir)
        .await?
        .expect("Should be tree merge");
    Ok((value, trees))
}

async fn write_trees(
    store: Arc<Store>,
    dir: RepoPathBuf,
    reusable_trees: Merge<Option<Tree>>,
    backend_trees: Merge<backend::Tree>,
) -> BackendResult<Merge<Tree>> {
    // Reuse an input tree when the merge produced identical contents. The merge tracks this
    // while it is already walking entries, avoiding another deep tree comparison here.
    let mut trees = Vec::with_capacity(backend_trees.num_sides());
    let mut backend_trees_to_write = Vec::new();
    for (backend_tree, reusable_tree) in backend_trees.into_iter().zip(reusable_trees) {
        match reusable_tree {
            Some(tree) => trees.push(Some(tree)),
            None => {
                trees.push(None);
                backend_trees_to_write.push(backend_tree);
            }
        }
    }
    let mut written_trees = store
        .write_trees(&dir, backend_trees_to_write)
        .await?
        .into_iter();
    for tree in &mut trees {
        if tree.is_none() {
            *tree = Some(written_trees.next().unwrap());
        }
    }
    debug_assert!(written_trees.next().is_none());
    let trees: Vec<_> = trees.into_iter().map(Option::unwrap).collect();
    Ok(Merge::from_vec(trees))
}

async fn resolve_file_values_owned(
    store: Arc<Store>,
    path: RepoPathBuf,
    values: MergedTreeValue,
) -> BackendResult<(MergedTreeValue, Option<MergedTreeValue>)> {
    let maybe_resolved = try_resolve_file_values(&store, &path, &values).await?;
    Ok((values, maybe_resolved))
}

async fn resolve_file_values_owned_on_worker(
    store: Arc<Store>,
    path: RepoPathBuf,
    values: MergedTreeValue,
) -> BackendResult<(MergedTreeValue, Option<MergedTreeValue>)> {
    let (sender, receiver) = oneshot::channel();
    rayon::spawn(move || {
        let result = resolve_file_values_owned(store, path, values).block_on();
        drop(sender.send(result));
    });
    receiver.await.map_err(|_| {
        BackendError::Other(Box::new(std::io::Error::other("file merge task exited")))
    })?
}

/// Tries to resolve file conflicts by merging the file contents. Treats missing
/// files as empty. If the file conflict cannot be resolved, returns the passed
/// `values` unmodified.
pub async fn resolve_file_values(
    store: &Arc<Store>,
    path: &RepoPath,
    values: MergedTreeValue,
) -> BackendResult<MergedTreeValue> {
    let same_change = store.merge_options().same_change;
    if let Some(resolved) = values.resolve_trivial(same_change) {
        return Ok(Merge::resolved(resolved.clone()));
    }

    let maybe_resolved = try_resolve_file_values(store, path, &values).await?;
    Ok(maybe_resolved.unwrap_or(values))
}

async fn try_resolve_file_values<T: Borrow<TreeValue>>(
    store: &Arc<Store>,
    path: &RepoPath,
    values: &Merge<Option<T>>,
) -> BackendResult<Option<MergedTreeValue>> {
    // The values may contain trees canceling each other (notably padded absent
    // trees), so we need to simplify them first.
    let simplified = values
        .map(|value| value.as_ref().map(Borrow::borrow))
        .simplify_with_hash();
    // No fast path for simplified.is_resolved(). If it could be resolved, it would
    // have been caught by values.resolve_trivial() above.
    if let Some(resolved) = try_resolve_file_conflict(store, path, &simplified).await? {
        Ok(Some(Merge::normal(resolved)))
    } else {
        // Failed to merge the files, or the paths are not files
        Ok(None)
    }
}

/// Resolves file-level conflict by merging content hunks.
///
/// The input `conflict` is supposed to be simplified. It shouldn't contain
/// non-file values that cancel each other.
async fn try_resolve_file_conflict(
    store: &Store,
    filename: &RepoPath,
    conflict: &MergedTreeVal<'_>,
) -> BackendResult<Option<TreeValue>> {
    let options = store.merge_options();
    // If there are any non-file or any missing parts in the conflict, we can't
    // merge it. We check early so we don't waste time reading file contents if
    // we can't merge them anyway. At the same time we determine whether the
    // resulting file should be executable.
    let Ok(file_id_conflict) = conflict.try_map(|term| match term {
        Some(TreeValue::File {
            id,
            executable: _,
            copy_id: _,
        }) => Ok(id),
        _ => Err(()),
    }) else {
        return Ok(None);
    };
    let Ok(executable_conflict) = conflict.try_map(|term| match term {
        Some(TreeValue::File {
            id: _,
            executable,
            copy_id: _,
        }) => Ok(executable),
        _ => Err(()),
    }) else {
        return Ok(None);
    };
    let Ok(copy_id_conflict) = conflict.try_map(|term| match term {
        Some(TreeValue::File {
            id: _,
            executable: _,
            copy_id,
        }) => Ok(copy_id),
        _ => Err(()),
    }) else {
        return Ok(None);
    };
    // TODO: Whether to respect options.same_change to merge executable and
    // copy_id? Should also update conflicts::resolve_file_executable().
    let Some(&&executable) = executable_conflict.resolve_trivial(SameChange::Accept) else {
        // We're unable to determine whether the result should be executable
        return Ok(None);
    };
    let Some(&copy_id) = copy_id_conflict.resolve_trivial(SameChange::Accept) else {
        // We're unable to determine the file's copy ID
        return Ok(None);
    };
    if let Some(&resolved_file_id) = file_id_conflict.resolve_trivial(options.same_change) {
        // Don't bother reading the file contents if the conflict can be trivially
        // resolved.
        return Ok(Some(TreeValue::File {
            id: resolved_file_id.clone(),
            executable,
            copy_id: copy_id.clone(),
        }));
    }

    // While the input conflict should be simplified by caller, it might contain
    // terms which only differ in executable bits. Simplify the conflict further
    // for two reasons:
    // 1. Avoid reading unchanged file contents
    // 2. The simplified conflict can sometimes be resolved when the unsimplfied one
    //    cannot
    let file_id_conflict = file_id_conflict.simplify_with_hash();

    let contents = file_id_conflict
        .try_map_async(async |file_id| store.read_file_bytes(filename, file_id).await)
        .await?;
    if let Some(merged_content) = files::try_merge(&contents, options) {
        let id = store.write_file_bytes(filename, &merged_content).await?;
        Ok(Some(TreeValue::File {
            id,
            executable,
            copy_id: copy_id.clone(),
        }))
    } else {
        Ok(None)
    }
}
