use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::{Parser, Subcommand};
use mosaic_core::{
    Architecture, MembershipLease, ObjectId, ObjectMetadata, ResourceDescriptor, ResourceId,
};
use mosaic_protocol::{
    CompleteExecutionRequest, ErrorResponse, HeartbeatRequest, PutObjectResponse,
    RegisterReplicaRequest, RegisterResourceRequest, RegisterResourceResponse,
    RenewExecutionRequest, ResolvedObject, WorkAssignment, WorkPollRequest, WorkPollResponse,
};
use mosaic_store::LocalObjectStore;
use reqwest::Client;
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "mosaic-agent", about = "Mosaic Fabric resource agent")]
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
        #[arg(long, default_value = ".mosaic/objects")]
        store_dir: PathBuf,
        #[arg(long, default_value_t = 1_000)]
        heartbeat_interval_ms: u64,
        #[arg(long, default_value_t = 200)]
        poll_interval_ms: u64,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mosaic_agent=info".into()),
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
                Duration::from_millis(heartbeat_interval_ms.max(50)),
                Duration::from_millis(poll_interval_ms.max(20)),
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
    heartbeat_period: Duration,
    poll_period: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut lease = register(&client, &coordinator, &data_endpoint, &descriptor).await?;
    info!(resource_id = %lease.resource_id, epoch = lease.epoch, "joined fabric");

    let busy = Arc::new(AtomicBool::new(false));
    let mut heartbeat = tokio::time::interval(heartbeat_period);
    let mut poll = tokio::time::interval(poll_period);

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
        for input in &assignment.inputs {
            input_bytes
                .push(materialize_input(client, coordinator, store, resource_id, input).await?);
        }

        let output_bytes = execute_builtin(&assignment, &input_bytes).await?;
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
    lease_state: Arc<tokio::sync::Mutex<mosaic_core::ExecutionLease>>,
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
        match parse_json::<mosaic_core::ExecutionLease>(response).await {
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
) -> Result<Vec<u8>, String> {
    let digest = input
        .metadata
        .digest
        .as_deref()
        .ok_or_else(|| format!("input {} has no digest", input.metadata.id))?;
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

async fn execute_builtin(
    assignment: &WorkAssignment,
    inputs: &[Vec<u8>],
) -> Result<Vec<u8>, String> {
    match assignment.task.artifact.as_str() {
        "builtin:identity" => inputs
            .first()
            .cloned()
            .ok_or_else(|| "builtin:identity requires one input".to_owned()),
        "builtin:concat" => Ok(inputs.iter().flatten().copied().collect()),
        "builtin:echo" => Ok(assignment.task.arguments.join(" ").into_bytes()),
        "builtin:sleep" => {
            let millis = assignment
                .task
                .arguments
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

fn data_router(state: DataState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
        .route("/v1/objects", post(put_object))
        .route("/v1/blobs/{digest}", get(get_blob))
        .with_state(state)
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
) -> Result<Response, DataError> {
    let digest = format!("sha256:{digest}");
    let bytes = state.store.get(&digest).map_err(|error| match error {
        mosaic_store::StoreError::Io(ref io_error)
            if io_error.kind() == std::io::ErrorKind::NotFound =>
        {
            DataError::not_found("object not found")
        }
        other => DataError::internal(other),
    })?;
    let mut response = bytes.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        "x-mosaic-digest",
        HeaderValue::from_str(&digest).map_err(DataError::internal)?,
    );
    Ok(response)
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
