use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::{Parser, Subcommand};
use plurifold_core::{
    Architecture, MembershipLease, ObjectId, ObjectMetadata, PipelineInput, ResourceDescriptor,
    ResourceId, TaskPipeline, TaskShard, TaskSpec,
};
use plurifold_protocol::{
    CompleteExecutionRequest, ErrorResponse, HeartbeatRequest, LinkMeasurement, PutObjectResponse,
    RegisterReplicaRequest, RegisterResourceRequest, RegisterResourceResponse,
    RenewExecutionRequest, ReportLinkMeasurementRequest, ResolvedObject, ResourceListResponse,
    WorkAssignment, WorkPollRequest, WorkPollResponse,
};
use plurifold_store::LocalObjectStore;
use reqwest::Client;
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

const PROBE_BYTES: usize = 256 * 1024;
const MAX_PROBE_BYTES: usize = 1024 * 1024;
const RTT_PROBE_SAMPLES: usize = 3;

#[derive(Parser)]
#[command(name = "plurifold-agent", about = "Plurifold resource agent")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Emit a capability descriptor without joining a fabric.
    Describe(ResourceArgs),
    /// Join a coordinator, serve a local object cache, and execute leased work.
    Run {
        #[command(flatten)]
        resource: ResourceArgs,
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        coordinator: String,
        #[arg(long, default_value = "127.0.0.1:8081")]
        bind: SocketAddr,
        #[arg(long)]
        advertise: Option<String>,
        #[arg(long, default_value = ".plurifold/objects")]
        store_dir: PathBuf,
        #[arg(long, default_value_t = 1_000)]
        heartbeat_interval_ms: u64,
        #[arg(long, default_value_t = 200)]
        poll_interval_ms: u64,
        #[arg(long, default_value_t = 30_000)]
        probe_interval_ms: u64,
    },
}

#[derive(clap::Args, Clone)]
struct ResourceArgs {
    #[arg(long, default_value = "local")]
    name: String,
    #[arg(long, default_value_t = 8)]
    cpu_cores: u32,
    #[arg(long, default_value_t = 16)]
    memory_gib: u64,
    #[arg(long, default_value_t = 1.0)]
    performance: f64,
    #[arg(long = "feature")]
    features: Vec<String>,
}

#[derive(Clone)]
struct DataState {
    store: LocalObjectStore,
    resource_id: ResourceId,
}

#[derive(Clone, Copy)]
struct WorkerPeriods {
    heartbeat: Duration,
    poll: Duration,
    probe: Duration,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "plurifold_agent=info".into()),
        )
        .init();

    match Args::parse().command {
        Command::Describe(resource) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&descriptor(ResourceId::new(), &resource))?
            );
        }
        Command::Run {
            resource,
            coordinator,
            bind,
            advertise,
            store_dir,
            heartbeat_interval_ms,
            poll_interval_ms,
            probe_interval_ms,
        } => {
            let resource_id = ResourceId::new();
            let store = LocalObjectStore::new(store_dir)?;
            let advertise = advertise.unwrap_or_else(|| format!("http://{bind}"));
            let data_state = DataState {
                store: store.clone(),
                resource_id,
            };
            let listener = tokio::net::TcpListener::bind(bind).await?;
            info!(resource_id = %resource_id, address = %listener.local_addr()?, "data plane listening");
            tokio::spawn(async move {
                if let Err(error) = axum::serve(listener, data_router(data_state)).await {
                    warn!(%error, "data plane stopped");
                }
            });

            run_worker(
                Client::new(),
                coordinator.trim_end_matches('/').to_owned(),
                advertise,
                descriptor(resource_id, &resource),
                store,
                WorkerPeriods {
                    heartbeat: Duration::from_millis(heartbeat_interval_ms.max(50)),
                    poll: Duration::from_millis(poll_interval_ms.max(20)),
                    probe: Duration::from_millis(probe_interval_ms.max(50)),
                },
            )
            .await?;
        }
    }
    Ok(())
}

fn descriptor(id: ResourceId, args: &ResourceArgs) -> ResourceDescriptor {
    ResourceDescriptor {
        id,
        epoch: 0,
        architecture: current_architecture(),
        cpu_cores: args.cpu_cores,
        memory_bytes: args.memory_gib << 30,
        accelerators: vec![],
        features: args
            .features
            .iter()
            .cloned()
            .chain([format!("agent-name:{}", args.name)])
            .collect::<BTreeSet<_>>(),
        performance_score: args.performance,
        queue_delay_ms: 0.0,
        startup_delay_ms: 0.0,
        failure_probability: 0.01,
    }
}

async fn run_worker(
    client: Client,
    coordinator: String,
    data_endpoint: String,
    descriptor: ResourceDescriptor,
    store: LocalObjectStore,
    periods: WorkerPeriods,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut lease = register(&client, &coordinator, &data_endpoint, &descriptor).await?;
    info!(resource_id = %lease.resource_id, epoch = lease.epoch, "joined fabric");

    let busy = Arc::new(AtomicBool::new(false));
    let probing = Arc::new(AtomicBool::new(false));
    let mut heartbeat = tokio::time::interval(periods.heartbeat);
    let mut poll = tokio::time::interval(periods.poll);
    let mut probe = tokio::time::interval(periods.probe);
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                match send_heartbeat(&client, &coordinator, &lease).await {
                    Ok(updated) => lease = updated,
                    Err(error) => {
                        warn!(%error, "heartbeat rejected; re-registering resource");
                        match register(&client, &coordinator, &data_endpoint, &descriptor).await {
                            Ok(updated) => lease = updated,
                            Err(register_error) => warn!(%register_error, "resource re-registration failed"),
                        }
                    }
                }
            }
            _ = poll.tick(), if !busy.load(Ordering::Acquire) => {
                match poll_work(&client, &coordinator, &lease).await {
                    Ok(Some(assignment)) => {
                        info!(task_id = %assignment.task.id, execution_id = %assignment.lease.execution_id, "accepted work lease");
                        busy.store(true, Ordering::Release);
                        let busy_flag = busy.clone();
                        let client = client.clone();
                        let coordinator = coordinator.clone();
                        let store = store.clone();
                        let resource_id = descriptor.id;
                        tokio::spawn(async move {
                            if let Err(error) = execute_and_commit(
                                &client,
                                &coordinator,
                                &store,
                                resource_id,
                                assignment,
                            ).await {
                                warn!(%error, "leased task failed; coordinator will recover after lease expiry");
                            }
                            busy_flag.store(false, Ordering::Release);
                        });
                    }
                    Ok(None) => {}
                    Err(error) => warn!(%error, "work poll failed"),
                }
            }
            _ = probe.tick() => {
                if !probing.swap(true, Ordering::AcqRel) {
                    let client = client.clone();
                    let coordinator = coordinator.clone();
                    let lease = lease.clone();
                    let probing = probing.clone();
                    tokio::spawn(async move {
                        if let Err(error) = discover_topology(&client, &coordinator, &lease).await {
                            debug!(%error, "topology discovery pass failed");
                        }
                        probing.store(false, Ordering::Release);
                    });
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown requested");
                return Ok(());
            }
        }
    }
}

async fn register(
    client: &Client,
    coordinator: &str,
    data_endpoint: &str,
    descriptor: &ResourceDescriptor,
) -> Result<MembershipLease, String> {
    let response = client
        .post(format!("{coordinator}/v1/resources/register"))
        .json(&RegisterResourceRequest {
            descriptor: descriptor.clone(),
            data_endpoint: data_endpoint.to_owned(),
        })
        .send()
        .await
        .map_err(|error| error.to_string())?;
    parse_json::<RegisterResourceResponse>(response)
        .await
        .map(|response| response.lease)
}

async fn send_heartbeat(
    client: &Client,
    coordinator: &str,
    lease: &MembershipLease,
) -> Result<MembershipLease, String> {
    let response = client
        .post(format!("{coordinator}/v1/resources/heartbeat"))
        .json(&HeartbeatRequest {
            resource_id: lease.resource_id,
            epoch: lease.epoch,
        })
        .send()
        .await
        .map_err(|error| error.to_string())?;
    parse_json(response).await
}

async fn poll_work(
    client: &Client,
    coordinator: &str,
    lease: &MembershipLease,
) -> Result<Option<WorkAssignment>, String> {
    let response = client
        .post(format!("{coordinator}/v1/work/poll"))
        .json(&WorkPollRequest {
            resource_id: lease.resource_id,
            epoch: lease.epoch,
        })
        .send()
        .await
        .map_err(|error| error.to_string())?;
    parse_json::<WorkPollResponse>(response)
        .await
        .map(|response| response.assignment)
}

async fn discover_topology(
    client: &Client,
    coordinator: &str,
    lease: &MembershipLease,
) -> Result<(), String> {
    let response = client
        .get(format!("{coordinator}/v1/resources"))
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let resources: ResourceListResponse = parse_json(response).await?;

    let self_key = lease.resource_id.to_string();
    for peer in resources.resources {
        if peer.descriptor.id == lease.resource_id || self_key >= peer.descriptor.id.to_string() {
            continue;
        }
        let Some(endpoint) = peer.data_endpoint else {
            continue;
        };
        let measurement = match measure_peer(client, &endpoint).await {
            Ok((rtt_ms, bandwidth_mbps)) => LinkMeasurement::Reachable {
                rtt_ms,
                bandwidth_mbps,
            },
            Err(error) => {
                debug!(peer = %peer.descriptor.id, %error, "peer topology probe failed");
                LinkMeasurement::Unreachable
            }
        };
        report_link_measurement(client, coordinator, lease, peer.descriptor.id, measurement)
            .await?;
    }
    Ok(())
}

async fn report_link_measurement(
    client: &Client,
    coordinator: &str,
    lease: &MembershipLease,
    peer_resource_id: ResourceId,
    measurement: LinkMeasurement,
) -> Result<(), String> {
    let response = client
        .post(format!("{coordinator}/v1/topology/measurement"))
        .json(&ReportLinkMeasurementRequest {
            reporter_resource_id: lease.resource_id,
            reporter_epoch: lease.epoch,
            peer_resource_id,
            measurement,
        })
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    expect_success(response).await
}

async fn measure_peer(client: &Client, endpoint: &str) -> Result<(f64, f64), String> {
    let endpoint = endpoint.trim_end_matches('/');
    let mut rtt_ms = f64::INFINITY;
    for _ in 0..RTT_PROBE_SAMPLES {
        let elapsed = fetch_probe(client, endpoint, 0).await?;
        rtt_ms = rtt_ms.min(elapsed.as_secs_f64() * 1_000.0);
    }

    let elapsed = fetch_probe(client, endpoint, PROBE_BYTES).await?;
    let payload_elapsed_ms = (elapsed.as_secs_f64() * 1_000.0 - rtt_ms).max(0.1);
    let bandwidth_mbps = PROBE_BYTES as f64 * 8.0 / payload_elapsed_ms / 1_000.0;
    Ok((rtt_ms, bandwidth_mbps))
}

async fn fetch_probe(client: &Client, endpoint: &str, bytes: usize) -> Result<Duration, String> {
    let started = Instant::now();
    let response = client
        .get(format!("{endpoint}/v1/probe/{bytes}"))
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(response_error(response).await);
    }
    let body = response.bytes().await.map_err(|error| error.to_string())?;
    if body.len() != bytes {
        return Err(format!(
            "probe returned {} bytes, expected {bytes}",
            body.len()
        ));
    }
    Ok(started.elapsed())
}

async fn execute_and_commit(
    client: &Client,
    coordinator: &str,
    store: &LocalObjectStore,
    resource_id: ResourceId,
    assignment: WorkAssignment,
) -> Result<(), String> {
    let lease_state = Arc::new(tokio::sync::Mutex::new(assignment.lease.clone()));
    let renewal = tokio::spawn(renew_execution_loop(
        client.clone(),
        coordinator.to_owned(),
        lease_state.clone(),
    ));

    let work_result = async {
        let mut input_bytes = Vec::with_capacity(assignment.inputs.len());
        for (input_index, input) in assignment.inputs.iter().enumerate() {
            input_bytes.push(
                materialize_input(
                    client,
                    coordinator,
                    store,
                    resource_id,
                    input,
                    assignment.task.input_byte_range(input_index),
                )
                .await?,
            );
        }

        let output_bytes = execute_task(&assignment.task, &input_bytes).await?;
        let stored = store
            .put(&output_bytes)
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(ObjectMetadata {
            id: ObjectId::new(),
            size_bytes: stored.size_bytes,
            digest: Some(stored.digest),
            encoding: Some("application/octet-stream".into()),
            locations: vec![resource_id],
            producer: Some(assignment.task.id),
        })
    }
    .await;
    renewal.abort();
    let output = work_result?;
    let lease = lease_state.lock().await.clone();
    let response = client
        .post(format!("{coordinator}/v1/work/complete"))
        .json(&CompleteExecutionRequest {
            lease,
            outputs: vec![output],
        })
        .send()
        .await
        .map_err(|error| error.to_string())?;
    expect_success(response).await
}

async fn renew_execution_loop(
    client: Client,
    coordinator: String,
    lease_state: Arc<tokio::sync::Mutex<plurifold_core::ExecutionLease>>,
) {
    loop {
        let sleep_ms = {
            let lease = lease_state.lock().await;
            let remaining = lease.expires_at_unix_ms.saturating_sub(now_unix_ms());
            if remaining == 0 {
                return;
            }
            (remaining / 3).clamp(50, 5_000)
        };
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

        let current = lease_state.lock().await.clone();
        let response = match client
            .post(format!("{coordinator}/v1/work/renew"))
            .json(&RenewExecutionRequest { lease: current })
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                warn!(%error, "execution lease renewal request failed");
                continue;
            }
        };
        match parse_json::<plurifold_core::ExecutionLease>(response).await {
            Ok(renewed) => *lease_state.lock().await = renewed,
            Err(error) => {
                warn!(%error, "execution lease renewal rejected");
                return;
            }
        }
    }
}

async fn materialize_input(
    client: &Client,
    coordinator: &str,
    store: &LocalObjectStore,
    resource_id: ResourceId,
    input: &ResolvedObject,
    range: Option<(u64, u64)>,
) -> Result<Vec<u8>, String> {
    let digest = input
        .metadata
        .digest
        .as_deref()
        .ok_or_else(|| format!("input {} has no digest", input.metadata.id))?;
    let range = match range {
        Some((0, length)) if length == input.metadata.size_bytes => None,
        other => other,
    };
    if let Some((offset, length)) = range {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| "input byte range overflows".to_owned())?;
        if end > input.metadata.size_bytes {
            return Err(format!(
                "input byte range {offset}..{end} exceeds object size {}",
                input.metadata.size_bytes
            ));
        }
        if length == 0 {
            return Ok(Vec::new());
        }
        if store.contains(digest) {
            let bytes = store.get(digest).map_err(|error| error.to_string())?;
            let start = usize::try_from(offset)
                .map_err(|_| "input byte range start does not fit this platform".to_owned())?;
            let end = usize::try_from(end)
                .map_err(|_| "input byte range end does not fit this platform".to_owned())?;
            return Ok(bytes[start..end].to_vec());
        }

        let mut last_error = "no replicas supplied".to_owned();
        for replica in &input.replicas {
            let response = match client
                .get(&replica.url)
                .header(header::RANGE, format!("bytes={offset}-{}", end - 1))
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    last_error = error.to_string();
                    continue;
                }
            };
            if response.status() != StatusCode::PARTIAL_CONTENT {
                last_error = format!(
                    "replica {} returned {} for byte range",
                    replica.url,
                    response.status()
                );
                continue;
            }
            let expected_range_digest = match response
                .headers()
                .get("x-plurifold-range-digest")
                .and_then(|value| value.to_str().ok())
            {
                Some(value) => value.to_owned(),
                None => {
                    last_error = format!("replica {} omitted range digest", replica.url);
                    continue;
                }
            };
            let bytes = response
                .bytes()
                .await
                .map_err(|error| error.to_string())?
                .to_vec();
            if bytes.len() as u64 != length {
                last_error = format!(
                    "replica {} returned {} range bytes, expected {length}",
                    replica.url,
                    bytes.len()
                );
                continue;
            }
            let actual = format!("sha256:{}", hex::encode(Sha256::digest(&bytes)));
            if actual != expected_range_digest {
                last_error = format!(
                    "replica range digest mismatch: expected {expected_range_digest}, received {actual}"
                );
                continue;
            }
            return Ok(bytes);
        }
        return Err(last_error);
    }

    if store.contains(digest) {
        return store.get(digest).map_err(|error| error.to_string());
    }

    let mut last_error = "no replicas supplied".to_owned();
    for replica in &input.replicas {
        let response = match client.get(&replica.url).send().await {
            Ok(response) => response,
            Err(error) => {
                last_error = error.to_string();
                continue;
            }
        };
        if !response.status().is_success() {
            last_error = format!("replica {} returned {}", replica.url, response.status());
            continue;
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|error| error.to_string())?
            .to_vec();
        let stored = store.put(&bytes).map_err(|error| error.to_string())?;
        if stored.digest != digest {
            last_error = format!(
                "replica digest mismatch: expected {digest}, received {}",
                stored.digest
            );
            continue;
        }
        let response = client
            .post(format!("{coordinator}/v1/objects/replica"))
            .json(&RegisterReplicaRequest {
                object_id: input.metadata.id,
                resource_id,
            })
            .send()
            .await
            .map_err(|error| error.to_string())?;
        expect_success(response).await?;
        return Ok(bytes);
    }
    Err(last_error)
}

async fn execute_task(task: &TaskSpec, inputs: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    match &task.pipeline {
        Some(pipeline) => execute_pipeline(pipeline, inputs, task.shard.as_ref()).await,
        None => {
            execute_builtin_operation(&task.artifact, &task.arguments, inputs, task.shard.as_ref())
                .await
        }
    }
}

async fn execute_pipeline(
    pipeline: &TaskPipeline,
    inputs: &[Vec<u8>],
    shard: Option<&TaskShard>,
) -> Result<Vec<u8>, String> {
    if pipeline.stages.len() < 2 {
        return Err("task pipeline requires at least two stages".to_owned());
    }

    let mut previous: Option<Vec<u8>> = None;
    for (stage_index, stage) in pipeline.stages.iter().enumerate() {
        let mut stage_inputs = Vec::with_capacity(stage.inputs.len());
        for binding in &stage.inputs {
            match binding {
                PipelineInput::External { index } => stage_inputs.push(
                    inputs
                        .get(*index)
                        .cloned()
                        .ok_or_else(|| format!("pipeline input index {index} is out of range"))?,
                ),
                PipelineInput::PreviousOutput => {
                    stage_inputs.push(previous.clone().ok_or_else(|| {
                        format!("pipeline stage {stage_index} references a missing previous output")
                    })?)
                }
            }
        }
        previous = Some(
            execute_builtin_operation(&stage.artifact, &stage.arguments, &stage_inputs, shard)
                .await?,
        );
    }
    previous.ok_or_else(|| "task pipeline produced no output".to_owned())
}

async fn execute_builtin_operation(
    artifact: &str,
    arguments: &[String],
    inputs: &[Vec<u8>],
    shard: Option<&TaskShard>,
) -> Result<Vec<u8>, String> {
    match artifact {
        "builtin:identity" => inputs
            .first()
            .cloned()
            .ok_or_else(|| "builtin:identity requires one input".to_owned()),
        "builtin:concat" => Ok(inputs.iter().flatten().copied().collect()),
        "builtin:echo" => Ok(arguments.join(" ").into_bytes()),
        "builtin:shard-echo" => {
            let shard =
                shard.ok_or_else(|| "builtin:shard-echo requires shard context".to_owned())?;
            let prefix = arguments.join(" ");
            Ok(format!("{prefix}:{}:{}", shard.index, shard.count).into_bytes())
        }
        "builtin:shard-sleep" => {
            let shard =
                shard.ok_or_else(|| "builtin:shard-sleep requires shard context".to_owned())?;
            let millis = arguments
                .first()
                .ok_or_else(|| "builtin:shard-sleep requires milliseconds".to_owned())?
                .parse::<u64>()
                .map_err(|_| "builtin:shard-sleep milliseconds must be an integer".to_owned())?;
            let prefix = arguments
                .get(1)
                .cloned()
                .unwrap_or_else(|| "shard".to_owned());
            tokio::time::sleep(Duration::from_millis(millis)).await;
            Ok(format!("{prefix}:{}:{}", shard.index, shard.count).into_bytes())
        }
        "builtin:shard-sleep-input" => {
            shard.ok_or_else(|| "builtin:shard-sleep-input requires shard context".to_owned())?;
            let millis = arguments
                .first()
                .ok_or_else(|| "builtin:shard-sleep-input requires milliseconds".to_owned())?
                .parse::<u64>()
                .map_err(|_| {
                    "builtin:shard-sleep-input milliseconds must be an integer".to_owned()
                })?;
            tokio::time::sleep(Duration::from_millis(millis)).await;
            inputs
                .first()
                .cloned()
                .ok_or_else(|| "builtin:shard-sleep-input requires one input".to_owned())
        }
        "builtin:sum-records-u64" => {
            shard.ok_or_else(|| "builtin:sum-records-u64 requires shard context".to_owned())?;
            optional_builtin_sleep(arguments, "builtin:sum-records-u64").await?;
            let bytes = inputs
                .first()
                .ok_or_else(|| "builtin:sum-records-u64 requires one input".to_owned())?;
            let text = std::str::from_utf8(bytes)
                .map_err(|_| "builtin:sum-records-u64 input must be UTF-8".to_owned())?;
            let mut sum = 0u64;
            for record in text.lines() {
                let record = record.trim();
                if record.is_empty() {
                    continue;
                }
                let value = record
                    .parse::<u64>()
                    .map_err(|_| format!("invalid u64 record {record:?}"))?;
                sum = sum
                    .checked_add(value)
                    .ok_or_else(|| "builtin:sum-records-u64 overflow".to_owned())?;
            }
            Ok(sum.to_le_bytes().to_vec())
        }
        "builtin:sum-u64" => {
            optional_builtin_sleep(arguments, "builtin:sum-u64").await?;
            let mut sum = 0u64;
            for input in inputs {
                let bytes: [u8; 8] = input.as_slice().try_into().map_err(|_| {
                    "builtin:sum-u64 inputs must be 8-byte little-endian u64 values".to_owned()
                })?;
                sum = sum
                    .checked_add(u64::from_le_bytes(bytes))
                    .ok_or_else(|| "builtin:sum-u64 overflow".to_owned())?;
            }
            Ok(sum.to_le_bytes().to_vec())
        }
        "builtin:sleep" => {
            let millis = arguments
                .first()
                .ok_or_else(|| {
                    "builtin:sleep requires milliseconds as its first argument".to_owned()
                })?
                .parse::<u64>()
                .map_err(|_| "builtin:sleep milliseconds must be an integer".to_owned())?;
            tokio::time::sleep(Duration::from_millis(millis)).await;
            Ok(inputs.first().cloned().unwrap_or_else(|| b"done".to_vec()))
        }
        other => Err(format!("unsupported artifact {other}")),
    }
}

async fn optional_builtin_sleep(arguments: &[String], artifact: &str) -> Result<(), String> {
    let Some(value) = arguments.first() else {
        return Ok(());
    };
    let millis = value
        .parse::<u64>()
        .map_err(|_| format!("{artifact} optional milliseconds must be an integer"))?;
    tokio::time::sleep(Duration::from_millis(millis)).await;
    Ok(())
}

fn data_router(state: DataState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
        .route("/v1/probe/{bytes}", get(probe_bytes))
        .route("/v1/objects", post(put_object))
        .route("/v1/blobs/{digest}", get(get_blob))
        .with_state(state)
}

async fn probe_bytes(Path(bytes): Path<usize>) -> Result<Bytes, DataError> {
    if bytes > MAX_PROBE_BYTES {
        return Err(DataError::bad_request(format!(
            "probe payload cannot exceed {MAX_PROBE_BYTES} bytes"
        )));
    }
    Ok(Bytes::from(vec![0; bytes]))
}

async fn put_object(
    State(state): State<DataState>,
    body: Bytes,
) -> Result<Json<PutObjectResponse>, DataError> {
    let stored = state.store.put(&body).map_err(DataError::internal)?;
    Ok(Json(PutObjectResponse {
        object: ObjectMetadata {
            id: ObjectId::new(),
            size_bytes: stored.size_bytes,
            digest: Some(stored.digest),
            encoding: Some("application/octet-stream".into()),
            locations: vec![state.resource_id],
            producer: None,
        },
    }))
}

async fn get_blob(
    State(state): State<DataState>,
    Path(digest): Path<String>,
    headers: HeaderMap,
) -> Result<Response, DataError> {
    let digest = format!("sha256:{digest}");
    let bytes = state.store.get(&digest).map_err(|error| match error {
        plurifold_store::StoreError::Io(ref io_error)
            if io_error.kind() == std::io::ErrorKind::NotFound =>
        {
            DataError::not_found("object not found")
        }
        other => DataError::internal(other),
    })?;
    let total = bytes.len();
    let requested = parse_byte_range(&headers, total)?;
    let (payload, range) = match requested {
        Some((start, end_exclusive)) => (
            bytes[start..end_exclusive].to_vec(),
            Some((start, end_exclusive)),
        ),
        None => (bytes, None),
    };
    let range_digest = range.map(|_| format!("sha256:{}", hex::encode(Sha256::digest(&payload))));
    let mut response = payload.into_response();
    if let Some((start, end_exclusive)) = range {
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {start}-{}/{total}", end_exclusive - 1))
                .map_err(DataError::internal)?,
        );
        response.headers_mut().insert(
            "x-plurifold-range-digest",
            HeaderValue::from_str(range_digest.as_deref().expect("range digest exists"))
                .map_err(DataError::internal)?,
        );
    }
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        "x-plurifold-digest",
        HeaderValue::from_str(&digest).map_err(DataError::internal)?,
    );
    Ok(response)
}

fn parse_byte_range(
    headers: &HeaderMap,
    total: usize,
) -> Result<Option<(usize, usize)>, DataError> {
    let Some(value) = headers.get(header::RANGE) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| DataError::bad_request("Range header is not valid ASCII"))?;
    let Some(spec) = value.strip_prefix("bytes=") else {
        return Err(DataError::bad_request("only byte ranges are supported"));
    };
    if spec.contains(',') {
        return Err(DataError::bad_request(
            "multiple byte ranges are not supported",
        ));
    }
    let (start, end) = spec
        .split_once('-')
        .ok_or_else(|| DataError::bad_request("invalid byte range"))?;
    let start = start
        .parse::<usize>()
        .map_err(|_| DataError::bad_request("byte range start must be an integer"))?;
    let end = end
        .parse::<usize>()
        .map_err(|_| DataError::bad_request("byte range end must be an integer"))?;
    if start > end || end >= total {
        return Err(DataError {
            status: StatusCode::RANGE_NOT_SATISFIABLE,
            message: "byte range is outside the object".into(),
        });
    }
    Ok(Some((start, end + 1)))
}

async fn parse_json<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, String> {
    if !response.status().is_success() {
        return Err(response_error(response).await);
    }
    response
        .json::<T>()
        .await
        .map_err(|error| error.to_string())
}

async fn expect_success(response: reqwest::Response) -> Result<(), String> {
    if response.status().is_success() {
        Ok(())
    } else {
        Err(response_error(response).await)
    }
}

async fn response_error(response: reqwest::Response) -> String {
    let status = response.status();
    match response.json::<ErrorResponse>().await {
        Ok(error) => format!("{status}: {}", error.error),
        Err(_) => status.to_string(),
    }
}

struct DataError {
    status: StatusCode,
    message: String,
}

impl DataError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl IntoResponse for DataError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn current_architecture() -> Architecture {
    match std::env::consts::ARCH {
        "x86_64" => Architecture::X86_64,
        "aarch64" => Architecture::Aarch64,
        "riscv64" => Architecture::RiscV64,
        other => Architecture::Other(other.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use plurifold_core::{
        CostHint, EffectSemantics, ResourceRequirements, TaskId, TaskPipelineStage,
    };

    use super::*;

    #[tokio::test]
    async fn pipeline_keeps_intermediate_output_in_memory() {
        let task = TaskSpec {
            id: TaskId::new(),
            artifact: "plurifold:pipeline".into(),
            entrypoint: "run".into(),
            arguments: vec![],
            inputs: vec![],
            requirements: ResourceRequirements::default(),
            effects: EffectSemantics::Pure,
            cost: CostHint::default(),
            pipeline: Some(TaskPipeline {
                stages: vec![
                    TaskPipelineStage {
                        artifact: "builtin:identity".into(),
                        entrypoint: "run".into(),
                        arguments: vec![],
                        inputs: vec![PipelineInput::External { index: 0 }],
                    },
                    TaskPipelineStage {
                        artifact: "builtin:concat".into(),
                        entrypoint: "run".into(),
                        arguments: vec![],
                        inputs: vec![
                            PipelineInput::PreviousOutput,
                            PipelineInput::External { index: 1 },
                        ],
                    },
                    TaskPipelineStage {
                        artifact: "builtin:concat".into(),
                        entrypoint: "run".into(),
                        arguments: vec![],
                        inputs: vec![
                            PipelineInput::PreviousOutput,
                            PipelineInput::External { index: 2 },
                        ],
                    },
                ],
            }),

            shard: None,
        };

        let output = execute_task(&task, &[b"left".to_vec(), b"right".to_vec(), b"!".to_vec()])
            .await
            .unwrap();
        assert_eq!(output, b"leftright!");
    }
    #[tokio::test]
    async fn shard_echo_observes_task_shard_context() {
        let task = TaskSpec {
            id: TaskId::new(),
            artifact: "builtin:shard-echo".into(),
            entrypoint: "run".into(),
            arguments: vec!["piece".into()],
            inputs: vec![],
            requirements: ResourceRequirements::default(),
            effects: EffectSemantics::Pure,
            cost: CostHint::default(),
            shard: Some(TaskShard {
                index: 1,
                count: 3,
                partition: None,
            }),
            pipeline: None,
        };

        let output = execute_task(&task, &[]).await.unwrap();
        assert_eq!(output, b"piece:1:3");
    }

    #[test]
    fn parses_single_http_byte_range() {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=2-5"));
        assert_eq!(parse_byte_range(&headers, 10).ok(), Some(Some((2, 6))));
    }

    #[tokio::test]
    async fn local_materialization_returns_only_requested_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path()).unwrap();
        let stored = store.put(b"abcdefghij").unwrap();
        let input = ResolvedObject {
            metadata: ObjectMetadata {
                id: ObjectId::new(),
                size_bytes: 10,
                digest: Some(stored.digest),
                encoding: None,
                locations: vec![],
                producer: None,
            },
            replicas: vec![],
        };
        let bytes = materialize_input(
            &Client::new(),
            "http://127.0.0.1:1",
            &store,
            ResourceId::new(),
            &input,
            Some((3, 4)),
        )
        .await
        .unwrap();
        assert_eq!(bytes, b"defg");
    }

    #[tokio::test]
    async fn record_sum_and_u64_reduction_are_associative_over_complete_records() {
        let shard_task = TaskSpec {
            id: TaskId::new(),
            artifact: "builtin:sum-records-u64".into(),
            entrypoint: "run".into(),
            arguments: vec![],
            inputs: vec![],
            requirements: ResourceRequirements::default(),
            effects: EffectSemantics::Pure,
            cost: CostHint::default(),
            shard: Some(TaskShard {
                index: 0,
                count: 2,
                partition: None,
            }),
            pipeline: None,
        };
        let left = execute_task(&shard_task, &[b"1\n20\n".to_vec()])
            .await
            .unwrap();
        let right = execute_task(&shard_task, &[b"300\n4\n".to_vec()])
            .await
            .unwrap();
        let reduction = TaskSpec {
            id: TaskId::new(),
            artifact: "builtin:sum-u64".into(),
            entrypoint: "run".into(),
            arguments: vec![],
            inputs: vec![],
            requirements: ResourceRequirements::default(),
            effects: EffectSemantics::Pure,
            cost: CostHint::default(),
            shard: None,
            pipeline: None,
        };
        let output = execute_task(&reduction, &[left, right]).await.unwrap();
        assert_eq!(u64::from_le_bytes(output.try_into().unwrap()), 325);
    }
}
