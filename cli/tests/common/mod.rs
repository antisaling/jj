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

mod command_output;
mod config_schema_defaults;
mod test_environment;

pub use self::command_output::CommandOutput;
pub use self::config_schema_defaults::default_config_from_schema;
pub use self::test_environment::TestEnvironment;
pub use self::test_environment::TestWorkDir;

pub fn fake_bisector_path() -> String {
    let path = assert_cmd::cargo::cargo_bin!("fake-bisector");
    assert!(path.is_file());
    path.as_os_str().to_str().unwrap().to_owned()
}

pub fn fake_editor_path() -> String {
    let path = assert_cmd::cargo::cargo_bin!("fake-editor");
    assert!(path.is_file());
    path.as_os_str().to_str().unwrap().to_owned()
}

pub fn fake_diff_editor_path() -> String {
    let path = assert_cmd::cargo::cargo_bin!("fake-diff-editor");
    assert!(path.is_file());
    path.as_os_str().to_str().unwrap().to_owned()
}

pub fn fake_git_lfs_path() -> String {
    let path = assert_cmd::cargo::cargo_bin!("git-lfs");
    assert!(path.is_file());
    path.as_os_str().to_str().unwrap().to_owned()
}

pub fn set_up_fake_git_lfs(test_env: &mut TestEnvironment) -> std::path::PathBuf {
    let fake_git_lfs = std::path::PathBuf::from(fake_git_lfs_path());
    let fake_bin_dir = fake_git_lfs.parent().unwrap().to_owned();
    let existing_path = std::env::var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        std::iter::once(fake_bin_dir).chain(std::env::split_paths(&existing_path)),
    )
    .unwrap();
    test_env.add_env_var("PATH", &path);
    let log_path = test_env.env_root().join("fake_git_lfs.log");
    std::fs::write(&log_path, "").unwrap();
    test_env.add_env_var("FAKE_GIT_LFS_LOG", &log_path);
    log_path
}

/// Forcibly enable interactive prompt.
pub fn force_interactive(cmd: &mut assert_cmd::Command) -> &mut assert_cmd::Command {
    cmd.env("JJ_INTERACTIVE", "1")
}

/// Coerces the value type to serialize it as TOML.
pub fn to_toml_value(value: impl Into<toml_edit::Value>) -> toml_edit::Value {
    value.into()
}

pub fn create_commit(work_dir: &TestWorkDir, name: &str, parents: &[&str]) {
    create_commit_with_files(work_dir, name, parents, &[(name, &format!("{name}\n"))]);
}

pub fn create_commit_with_files(
    work_dir: &TestWorkDir,
    name: &str,
    parents: &[&str],
    files: &[(&str, &str)],
) {
    let parents = match parents {
        [] => &["root()"],
        parents => parents,
    };
    work_dir
        .run_jj_with(|cmd| cmd.args(["new", "-m", name]).args(parents))
        .success();
    for (name, content) in files {
        work_dir.write_file(name, content);
    }
    work_dir
        .run_jj(["bookmark", "create", "-r@", name])
        .success();
}
