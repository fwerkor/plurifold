use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};

use plurifold_core::{
    Accelerator, AcceleratorKind, ResourceDescriptor, TaskShard, TaskShardPartition,
};
use tokio::process::Command;

const MIB: u64 = 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub(crate) struct ExecutorPolicy {
    roots: Vec<PathBuf>,
    wasmtime: Option<PathBuf>,
}

impl ExecutorPolicy {
    pub(crate) fn new(
        roots: Vec<PathBuf>,
        enable_wasi: bool,
        wasmtime: PathBuf,
    ) -> Result<Self, String> {
        let mut canonical_roots = Vec::with_capacity(roots.len());
        for root in roots {
            let canonical = root.canonicalize().map_err(|error| {
                format!("cannot canonicalize exec root {}: {error}", root.display())
            })?;
            if !canonical.is_dir() {
                return Err(format!(
                    "exec root {} is not a directory",
                    canonical.display()
                ));
            }
            canonical_roots.push(canonical);
        }
        canonical_roots.sort();
        canonical_roots.dedup();

        let wasmtime = if enable_wasi {
            if !command_exists(&wasmtime) {
                return Err("--enable-wasi was set but wasmtime is not executable".to_owned());
            }
            Some(wasmtime)
        } else {
            None
        };

        Ok(Self {
            roots: canonical_roots,
            wasmtime,
        })
    }

    pub(crate) fn native_enabled(&self) -> bool {
        !self.roots.is_empty()
    }

    pub(crate) fn wasi_enabled(&self) -> bool {
        self.native_enabled() && self.wasmtime.is_some()
    }

    pub(crate) async fn execute(
        &self,
        artifact: &str,
        entrypoint: &str,
        arguments: &[String],
        inputs: &[Vec<u8>],
        shard: Option<&TaskShard>,
    ) -> Result<Vec<u8>, String> {
        if let Some(path) = artifact.strip_prefix("native:") {
            if !self.native_enabled() {
                return Err(
                    "native executor is disabled; start the agent with at least one --exec-root"
                        .to_owned(),
                );
            }
            let executable = self.authorize_artifact(path)?;
            return execute_native(&executable, entrypoint, arguments, inputs, shard).await;
        }

        if let Some(path) = artifact.strip_prefix("wasi:") {
            let wasmtime = self.wasmtime.as_ref().ok_or_else(|| {
                "WASI executor is disabled; start the agent with --enable-wasi and --exec-root"
                    .to_owned()
            })?;
            let module = self.authorize_artifact(path)?;
            return execute_wasi(wasmtime, &module, entrypoint, arguments, inputs, shard).await;
        }

        Err(format!("unsupported artifact {artifact}"))
    }

    fn authorize_artifact(&self, path: &str) -> Result<PathBuf, String> {
        let path = PathBuf::from(path);
        if !path.is_absolute() {
            return Err(format!(
                "external artifact path must be absolute, got {}",
                path.display()
            ));
        }
        let canonical = path
            .canonicalize()
            .map_err(|error| format!("cannot resolve artifact {}: {error}", path.display()))?;
        if !self.roots.iter().any(|root| canonical.starts_with(root)) {
            return Err(format!(
                "artifact {} is outside configured exec roots",
                canonical.display()
            ));
        }
        if !canonical.is_file() {
            return Err(format!("artifact {} is not a file", canonical.display()));
        }
        Ok(canonical)
    }
}

async fn stage_inputs(inputs: &[Vec<u8>]) -> Result<tempfile::TempDir, String> {
    let workdir = tempfile::tempdir().map_err(|error| error.to_string())?;
    for (index, bytes) in inputs.iter().enumerate() {
        tokio::fs::write(workdir.path().join(format!("input-{index}")), bytes)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(workdir)
}

async fn execute_native(
    executable: &Path,
    entrypoint: &str,
    arguments: &[String],
    inputs: &[Vec<u8>],
    shard: Option<&TaskShard>,
) -> Result<Vec<u8>, String> {
    let workdir = stage_inputs(inputs).await?;
    let mut command = Command::new(executable);
    configure_native_env(
        &mut command,
        workdir.path(),
        entrypoint,
        inputs.len(),
        shard,
    )?;
    command
        .args(arguments)
        .current_dir(workdir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    collect_output(command, "native task").await
}

async fn execute_wasi(
    wasmtime: &Path,
    module: &Path,
    entrypoint: &str,
    arguments: &[String],
    inputs: &[Vec<u8>],
    shard: Option<&TaskShard>,
) -> Result<Vec<u8>, String> {
    let workdir = stage_inputs(inputs).await?;
    let host_dir = workdir
        .path()
        .to_str()
        .ok_or_else(|| "task workdir is not valid UTF-8".to_owned())?;
    let mut command = Command::new(wasmtime);
    command
        .arg("run")
        .arg("--dir")
        .arg(format!("{host_dir}::/work"));
    for (name, value) in wasi_env(entrypoint, inputs.len(), shard)? {
        command.arg("--env").arg(format!("{name}={value}"));
    }
    command
        .arg(module)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    collect_output(command, "WASI task").await
}

async fn collect_output(mut command: Command, kind: &str) -> Result<Vec<u8>, String> {
    let output = command
        .output()
        .await
        .map_err(|error| format!("failed to start {kind}: {error}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "{kind} exited with {}: {}",
            output.status,
            stderr.trim()
        ));
    }
    Ok(output.stdout)
}

fn configure_native_env(
    command: &mut Command,
    workdir: &Path,
    entrypoint: &str,
    input_count: usize,
    shard: Option<&TaskShard>,
) -> Result<(), String> {
    command
        .env("PLURIFOLD_ABI", "1")
        .env("PLURIFOLD_ENTRYPOINT", entrypoint)
        .env("PLURIFOLD_INPUT_COUNT", input_count.to_string());

    for index in 0..input_count {
        command.env(
            format!("PLURIFOLD_INPUT_{index}"),
            workdir.join(format!("input-{index}")),
        );
    }
    for (name, value) in shard_env(shard)? {
        command.env(name, value);
    }
    Ok(())
}

fn wasi_env(
    entrypoint: &str,
    input_count: usize,
    shard: Option<&TaskShard>,
) -> Result<Vec<(String, String)>, String> {
    let mut env = vec![
        ("PLURIFOLD_ABI".to_owned(), "1".to_owned()),
        ("PLURIFOLD_ENTRYPOINT".to_owned(), entrypoint.to_owned()),
        ("PLURIFOLD_INPUT_COUNT".to_owned(), input_count.to_string()),
    ];
    for index in 0..input_count {
        env.push((
            format!("PLURIFOLD_INPUT_{index}"),
            format!("/work/input-{index}"),
        ));
    }
    env.extend(shard_env(shard)?);
    Ok(env)
}

fn shard_env(shard: Option<&TaskShard>) -> Result<Vec<(String, String)>, String> {
    let Some(shard) = shard else {
        return Ok(Vec::new());
    };
    let mut env = vec![
        ("PLURIFOLD_SHARD_INDEX".to_owned(), shard.index.to_string()),
        ("PLURIFOLD_SHARD_COUNT".to_owned(), shard.count.to_string()),
    ];
    if let Some(partition) = &shard.partition {
        env.push((
            "PLURIFOLD_SHARD_PARTITION".to_owned(),
            serde_json::to_string(partition).map_err(|error| error.to_string())?,
        ));
        match partition {
            TaskShardPartition::ByteRange {
                offset,
                length,
                total_bytes,
                ..
            }
            | TaskShardPartition::Records {
                offset,
                length,
                total_bytes,
                ..
            } => {
                env.push(("PLURIFOLD_RANGE_OFFSET".to_owned(), offset.to_string()));
                env.push(("PLURIFOLD_RANGE_LENGTH".to_owned(), length.to_string()));
                env.push((
                    "PLURIFOLD_RANGE_TOTAL_BYTES".to_owned(),
                    total_bytes.to_string(),
                ));
            }
        }
    }
    Ok(env)
}

fn command_exists(program: &Path) -> bool {
    StdCommand::new(program)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

pub(crate) fn detect_accelerators(descriptor: &mut ResourceDescriptor) {
    let mut features = BTreeSet::new();

    if let Some((count, memory_bytes, driver)) = detect_nvidia() {
        descriptor.accelerators.push(Accelerator {
            kind: AcceleratorKind::NvidiaGpu,
            count,
            memory_bytes_per_device: memory_bytes,
        });
        features.insert("accelerator:nvidia".to_owned());
        features.insert("cuda".to_owned());
        if let Some(driver) = driver {
            features.insert(format!("cuda-driver:{driver}"));
        }
    }

    if let Some((count, memory_bytes)) = detect_ascend() {
        descriptor.accelerators.push(Accelerator {
            kind: AcceleratorKind::AscendNpu,
            count,
            memory_bytes_per_device: memory_bytes,
        });
        features.insert("accelerator:ascend".to_owned());
        features.insert("cann".to_owned());
        if let Some(version) = detect_cann_version() {
            features.insert(format!("cann:{version}"));
        }
    }

    descriptor.features.extend(features);
}

fn detect_nvidia() -> Option<(u32, u64, Option<String>)> {
    let output = StdCommand::new("nvidia-smi")
        .args([
            "--query-gpu=memory.total,driver_version",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let mut count = 0u32;
    let mut min_memory_mib = u64::MAX;
    let mut driver = None;
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let mut fields = line.split(',').map(str::trim);
        let memory_mib = fields.next()?.parse::<u64>().ok()?;
        let current_driver = fields.next().filter(|value| !value.is_empty());
        count = count.saturating_add(1);
        min_memory_mib = min_memory_mib.min(memory_mib);
        if driver.is_none() {
            driver = current_driver.map(str::to_owned);
        }
    }
    (count > 0).then_some((count, min_memory_mib.saturating_mul(MIB), driver))
}

fn detect_ascend() -> Option<(u32, u64)> {
    let list = StdCommand::new("npu-smi")
        .args(["info", "-l"])
        .output()
        .ok()?;
    if !list.status.success() {
        return None;
    }
    let list_text = String::from_utf8(list.stdout).ok()?;
    let count = list_text
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(key, _)| key.trim() == "Total Count")
                .and_then(|(_, value)| value.trim().parse::<u32>().ok())
        })
        .filter(|count| *count > 0)?;

    let memory_bytes = StdCommand::new("npu-smi")
        .arg("info")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|text| {
            text.lines()
                .filter_map(|line| {
                    let (_, rhs) = line.rsplit_once('/')?;
                    let value = rhs.split_whitespace().next()?.parse::<u64>().ok()?;
                    (value >= 1024).then_some(value)
                })
                .min()
        })
        .unwrap_or(0)
        .saturating_mul(MIB);

    Some((count, memory_bytes))
}

fn detect_cann_version() -> Option<String> {
    let candidates = [
        "/usr/local/Ascend/ascend-toolkit/latest/version.cfg",
        "/usr/local/Ascend/ascend-toolkit/latest/x86_64-linux/ascend_toolkit_install.info",
        "/usr/local/Ascend/ascend-toolkit/latest/aarch64-linux/ascend_toolkit_install.info",
    ];
    for path in candidates {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        for line in text.lines() {
            let line = line.trim();
            for key in ["toolkit_running_version=", "version="] {
                if let Some(value) = line.strip_prefix(key) {
                    let value = value.trim().trim_matches(&['"', '[', ']'][..]);
                    if !value.is_empty() {
                        return Some(value.to_owned());
                    }
                }
            }
        }
    }
    None
}
