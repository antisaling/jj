// Copyright 2026 The Jujutsu Authors
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

use std::fs::OpenOptions;
use std::io;
use std::io::Read as _;
use std::io::Write as _;

fn append_log(subcommand: &str, args: &[String]) -> io::Result<()> {
    let Some(log_path) = std::env::var_os("FAKE_GIT_LFS_LOG") else {
        return Ok(());
    };
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    writeln!(file, "{subcommand}\t{}", args.join("\t"))
}

fn should_fail(subcommand: &str) -> bool {
    let fail_all = std::env::var("FAKE_GIT_LFS_FAIL_ALL")
        .map(|value| value == "1")
        .unwrap_or(false);
    if fail_all {
        return true;
    }
    std::env::var("FAKE_GIT_LFS_FAIL_SUBCOMMAND")
        .map(|value| value.split(',').any(|entry| entry.trim() == subcommand))
        .unwrap_or(false)
}

fn require_git_context() {
    let requires_git_dir = std::env::var("FAKE_GIT_LFS_REQUIRE_GIT_DIR")
        .map(|value| value == "1")
        .unwrap_or(false);
    if requires_git_dir && std::env::var_os("GIT_DIR").is_none() {
        eprintln!("fake git-lfs: missing GIT_DIR");
        std::process::exit(19);
    }
}

fn run_clean() -> io::Result<()> {
    let mut data = Vec::new();
    std::io::stdin().read_to_end(&mut data)?;
    let oid = format!("{:064x}", data.len());
    write!(
        std::io::stdout(),
        "version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {}\n",
        data.len()
    )
}

fn main() -> io::Result<()> {
    let mut args = std::env::args();
    let _program = args.next();
    let Some(subcommand) = args.next() else {
        eprintln!("fake git-lfs: missing subcommand");
        std::process::exit(2);
    };
    let remaining_args: Vec<String> = args.collect();
    require_git_context();
    append_log(&subcommand, &remaining_args)?;

    if should_fail(&subcommand) {
        eprintln!("fake git-lfs: forced failure for subcommand `{subcommand}`");
        std::process::exit(17);
    }

    match subcommand.as_str() {
        "clean" => run_clean(),
        "push" | "fetch" | "checkout" => Ok(()),
        _ => Ok(()),
    }
}
