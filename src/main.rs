use std::{
    fs::{self, File},
    io::{self, Write, Read},
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
    thread,
};

use csv::Reader;
use serde::Deserialize;

const CSV_FILE: &str = "LimboBugs.csv";
const TIMEOUT_SECS: u64 = 60; // originally 1 second for testing
const RUNS_PER_ISSUE: usize = 100;

#[derive(Debug, Deserialize)]
struct IssueRow {
    #[serde(rename = "Issue")]
    issue: usize,
    #[serde(rename = "Commit IDs")]
    commit_ids: Option<String>,
    #[serde(rename = "Opts")]
    opts: Option<String>,
}

fn checked_run(command: &str) -> io::Result<()> {
    let status = Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;

    if !status.success() {
        Err(io::Error::new(io::ErrorKind::Other, "Command failed"))
    } else {
        Ok(())
    }
}

fn run_simulation(cmd: &str, timeout_secs: u64, output_dir: &PathBuf) {
    fs::create_dir_all(output_dir).unwrap();

    let stdout_path = output_dir.join("stdout.txt");
    let stderr_path = output_dir.join("stderr.txt");
    let exit_code_path = output_dir.join("exit_code.txt");

    let mut child = match Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            fs::write(&stderr_path, format!("error: {}", e)).unwrap();
            fs::write(&exit_code_path, "-2").unwrap();
            return;
        }
    };

    let output = match child
        .wait_with_output()
    {
        Ok(out) => out,
        Err(_) => {
            let _ = child.kill();
            fs::write(&exit_code_path, "-1 timed out").unwrap();
            return;
        }
    };

    let exit_code = output.status.code().unwrap_or(-2).to_string();

    fs::write(&stdout_path, output.stdout).unwrap();
    fs::write(&stderr_path, output.stderr).unwrap();
    fs::write(&exit_code_path, if output.status.success() {
        exit_code
    } else {
        format!("{}{}", exit_code, if exit_code == "-1" { " timed out" } else { "" })
    }).unwrap();
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = Reader::from_path(CSV_FILE)?;
    let mut records = vec![];

    for result in reader.deserialize() {
        let record: IssueRow = result?;
        records.push(record);
    }

    for record in records {
        let issue_id = record.issue;
        let commit = record
            .commit_ids
            .as_ref()
            .and_then(|s| s.split(", ").next().map(|x| x.to_string()));

        let opts = record.opts.unwrap_or_default();

        if commit.is_none() || commit.as_ref().unwrap().trim().is_empty() {
            println!("Issue {}: Skipped (missing commit ID)", issue_id);
            continue;
        }

        let commit_str = commit.unwrap();
        println!("\n=== Processing Issue {} (commit {}) ===", issue_id, commit_str);

        if checked_run(&format!("git checkout {}", commit_str)).is_err() {
            println!("Issue {}: git checkout failed for commit {}", issue_id, commit_str);
            continue;
        }

        let issue_dir = PathBuf::from(format!("results/{}", issue_id));
        fs::create_dir_all(&issue_dir)?;

        if checked_run("cargo cache -a").is_err() {
            println!("Issue {}: cargo cache failed", issue_id);
            continue;
        }

        fs::write(issue_dir.join("commit.txt"), format!("{}\n", commit_str))?;

        for i in 1..=RUNS_PER_ISSUE {
            println!("Issue {}: Run {}", issue_id, i);
            let run_dir = issue_dir.join(format!("run_{}", i));
            let cmd = format!(
                "RUST_LOG=limbo_sim=info cargo run --bin limbo_sim -- {}",
                opts
            );
            run_simulation(&cmd, TIMEOUT_SECS, &run_dir);
        }
    }

    Ok(())
}
