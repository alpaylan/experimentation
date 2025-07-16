use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    thread,
};

use chrono::Duration;

use process_control::{ChildExt, Control};

use csv::Reader;
use serde::Deserialize;

const CSV_FILE: &str = "LimboBugs.csv";
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

    let stdout_data = Arc::new(Mutex::new(String::new()));
    let stderr_data = Arc::new(Mutex::new(String::new()));

    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();

    let stdout_buf = Arc::clone(&stdout_data);
    let stderr_buf = Arc::clone(&stderr_data);

    let stdout_thread = thread::spawn(move || {
        let mut s = String::new();
        let _ = stdout.read_to_string(&mut s);
        *stdout_buf.lock().unwrap() = s;
    });

    let stderr_thread = thread::spawn(move || {
        let mut s = String::new();
        let _ = stderr.read_to_string(&mut s);
        *stderr_buf.lock().unwrap() = s;
    });

    let result = child
        .controlled()
        .time_limit(
            Duration::seconds(timeout_secs as i64)
                .to_std()
                .expect("duration conversion failed"),
        )
        .terminate_for_timeout()
        .wait();

    match result {
        Ok(Some(status)) => {
            let _ = stdout_thread.join();
            let _ = stderr_thread.join();

            fs::write(&stdout_path, &*stdout_data.lock().unwrap()).unwrap();
            fs::write(&stderr_path, &*stderr_data.lock().unwrap()).unwrap();
            fs::write(
                &exit_code_path,
                status
                    .code()
                    .map(|c| c.to_string())
                    .unwrap_or("-2".to_string()),
            )
            .unwrap();
        }
        Ok(None) => {
            // Timeout occurred, process was killed
            let _ = stdout_thread.join();
            let _ = stderr_thread.join();

            fs::write(&stdout_path, &*stdout_data.lock().unwrap()).unwrap();
            fs::write(&stderr_path, &*stderr_data.lock().unwrap()).unwrap();
            fs::write(&exit_code_path, "-1 timed out").unwrap();
        }
        Err(e) => {
            fs::write(&stderr_path, format!("error: {}", e)).unwrap();
            fs::write(&exit_code_path, "-2").unwrap();
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let TIMEOUT_SECS: u64 = std::env::var("TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(600);

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
        println!(
            "\n=== Processing Issue {} (commit {}) ===",
            issue_id, commit_str
        );

        if checked_run(&format!("git checkout {}", commit_str)).is_err() {
            println!(
                "Issue {}: git checkout failed for commit {}",
                issue_id, commit_str
            );
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
                "RUST_LOG=limbo_sim=debug cargo run --bin limbo_sim -- {}",
                opts
            );
            run_simulation(&cmd, TIMEOUT_SECS, &run_dir);
        }
    }

    Ok(())
}
