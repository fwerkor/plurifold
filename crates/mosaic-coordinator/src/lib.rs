use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use mosaic_core::{ObjectId, ResourceId, TaskId};
use mosaic_protocol::{
    CompleteExecutionRequest, ErrorResponse, HeartbeatRequest, LinkUpdateRequest, ObjectReplica,
    PublishObjectRequest, RegisterReplicaRequest, RegisterResourceRequest,
    RegisterResourceResponse, RenewExecutionRequest, ResolvedObject, ResourceListResponse,
    ResourceView, SubmitTaskRequest, SubmitTaskResponse, TaskView, WorkAssignment, WorkPollRequest,
    WorkPollResponse,
};
use mosaic_runtime::{Fabric, FabricError};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Coordinator {
    inner: Arc<Mutex<CoordinatorState>>,
}

struct CoordinatorState {
    fabric: Fabric,
    data_endpoints: HashMap<ResourceId, String>,
}

impl Coordinator {
    pub fn new(membership_ttl_ms: u64, execution_ttl_ms: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CoordinatorState {
                fabric: Fabric::new(membership_ttl_ms, execution_ttl_ms),
                data_endpoints: HashMap::new(),
            })),
        }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/healthz", get(health))
            .route("/v1/resources/register", post(register_resource))
            .route("/v1/resources/heartbeat", post(heartbeat))
            .route("/v1/resources", get(list_resources))
            .route("/v1/work/poll", post(poll_work))
            .route("/v1/work/renew", post(renew_execution))
            .route("/v1/work/complete", post(complete_execution))
            .route("/v1/objects/publish", post(publish_object))
            .route("/v1/objects/replica", post(register_replica))
            .route("/v1/tasks", post(submit_task))
            .route("/v1/tasks/{task_id}", get(get_task))
            .route("/v1/topology/link", post(update_link))
            .with_state(self)
    }

    pub async fn maintenance_tick(&self) {
        let now = now_unix_ms();
        let mut state = self.inner.lock().await;
        let expired = state.fabric.expire_resources_at(now);
        for resource_id in expired {
            state.data_endpoints.remove(&resource_id);
        }
        state.fabric.reap_expired_executions_at(now);
    }
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn register_resource(
    State(coordinator): State<Coordinator>,
    Json(mut request): Json<RegisterResourceRequest>,
) -> Result<Json<RegisterResourceResponse>, ApiError> {
    let endpoint = normalize_endpoint(&request.data_endpoint)?;
    let mut state = coordinator.inner.lock().await;
    let lease = state.fabric.register_resource(request.descriptor.clone());
    request.descriptor.epoch = lease.epoch;
    state.data_endpoints.insert(lease.resource_id, endpoint);
    Ok(Json(RegisterResourceResponse { lease }))
}

async fn heartbeat(
    State(coordinator): State<Coordinator>,
    Json(request): Json<HeartbeatRequest>,
) -> Result<Json<mosaic_core::MembershipLease>, ApiError> {
    let mut state = coordinator.inner.lock().await;
    let lease = state
        .fabric
        .heartbeat(request.resource_id, request.epoch)
        .map_err(ApiError::from_fabric)?;
    Ok(Json(lease))
}

async fn list_resources(State(coordinator): State<Coordinator>) -> Json<ResourceListResponse> {
    let state = coordinator.inner.lock().await;
    let resources = state
        .fabric
        .active_resources()
        .into_iter()
        .map(|descriptor| ResourceView {
            data_endpoint: state.data_endpoints.get(&descriptor.id).cloned(),
            descriptor,
        })
        .collect();
    Json(ResourceListResponse { resources })
}

async fn poll_work(
    State(coordinator): State<Coordinator>,
    Json(request): Json<WorkPollRequest>,
) -> Result<Json<WorkPollResponse>, ApiError> {
    let mut state = coordinator.inner.lock().await;
    let current_epoch = state
        .fabric
        .resource_epoch(request.resource_id)
        .ok_or_else(|| ApiError::not_found("resource is not active"))?;
    if current_epoch != request.epoch {
        return Err(ApiError::conflict("resource epoch is stale"));
    }

    for task_id in state.fabric.pending_task_ids() {
        let decision = match state.fabric.schedule_task(task_id) {
            Ok(decision) => decision,
            Err(_) => continue,
        };
        if decision.resource_id != request.resource_id {
            continue;
        }

        let task = state
            .fabric
            .task_spec(task_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("task disappeared during scheduling"))?;
        let inputs = resolve_inputs(&state, &task.inputs)?;
        let lease = state
            .fabric
            .begin_execution(task_id, request.resource_id)
            .map_err(ApiError::from_fabric)?;
        return Ok(Json(WorkPollResponse {
            assignment: Some(WorkAssignment {
                lease,
                task,
                inputs,
            }),
        }));
    }

    Ok(Json(WorkPollResponse { assignment: None }))
}

fn resolve_inputs(
    state: &CoordinatorState,
    input_ids: &[ObjectId],
) -> Result<Vec<ResolvedObject>, ApiError> {
    input_ids
        .iter()
        .map(|object_id| {
            let metadata = state
                .fabric
                .object_metadata(*object_id)
                .cloned()
                .ok_or_else(|| ApiError::not_found(format!("unknown object {object_id}")))?;
            let digest = metadata.digest.as_deref().ok_or_else(|| {
                ApiError::conflict(format!("object {object_id} has no content digest"))
            })?;
            let suffix = digest
                .strip_prefix("sha256:")
                .ok_or_else(|| ApiError::conflict(format!("unsupported digest {digest}")))?;
            let replicas = metadata
                .locations
                .iter()
                .filter_map(|resource_id| {
                    state
                        .data_endpoints
                        .get(resource_id)
                        .map(|endpoint| ObjectReplica {
                            resource_id: *resource_id,
                            url: format!("{endpoint}/v1/blobs/{suffix}"),
                        })
                })
                .collect::<Vec<_>>();
            if replicas.is_empty() {
                return Err(ApiError::conflict(format!(
                    "object {object_id} has no reachable registered replica"
                )));
            }
            Ok(ResolvedObject { metadata, replicas })
        })
        .collect()
}

async fn renew_execution(
    State(coordinator): State<Coordinator>,
    Json(request): Json<RenewExecutionRequest>,
) -> Result<Json<mosaic_core::ExecutionLease>, ApiError> {
    let mut state = coordinator.inner.lock().await;
    let renewed = state
        .fabric
        .renew_execution(&request.lease)
        .map_err(ApiError::from_fabric)?;
    Ok(Json(renewed))
}

async fn complete_execution(
    State(coordinator): State<Coordinator>,
    Json(request): Json<CompleteExecutionRequest>,
) -> Result<StatusCode, ApiError> {
    let mut state = coordinator.inner.lock().await;
    state
        .fabric
        .complete_execution(&request.lease, request.outputs)
        .map_err(ApiError::from_fabric)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn publish_object(
    State(coordinator): State<Coordinator>,
    Json(request): Json<PublishObjectRequest>,
) -> Result<StatusCode, ApiError> {
    if request.object.digest.is_none() {
        return Err(ApiError::bad_request("published objects require a digest"));
    }
    let mut state = coordinator.inner.lock().await;
    state
        .fabric
        .publish_object(request.object)
        .map_err(ApiError::from_fabric)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn register_replica(
    State(coordinator): State<Coordinator>,
    Json(request): Json<RegisterReplicaRequest>,
) -> Result<StatusCode, ApiError> {
    let mut state = coordinator.inner.lock().await;
    if state.fabric.resource_epoch(request.resource_id).is_none() {
        return Err(ApiError::not_found("replica resource is not active"));
    }
    state
        .fabric
        .add_object_location(request.object_id, request.resource_id)
        .map_err(ApiError::from_fabric)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn submit_task(
    State(coordinator): State<Coordinator>,
    Json(request): Json<SubmitTaskRequest>,
) -> Result<Json<SubmitTaskResponse>, ApiError> {
    let mut state = coordinator.inner.lock().await;
    let task_id = state
        .fabric
        .submit(request.task)
        .map_err(ApiError::from_fabric)?;
    Ok(Json(SubmitTaskResponse { task_id }))
}

async fn get_task(
    State(coordinator): State<Coordinator>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskView>, ApiError> {
    let task_id = TaskId::from_str(&task_id)
        .map_err(|_| ApiError::bad_request("task_id is not a valid UUID"))?;
    let state = coordinator.inner.lock().await;
    let task = state
        .fabric
        .task_spec(task_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("task not found"))?;
    let status = state
        .fabric
        .task_status(task_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("task not found"))?;
    Ok(Json(TaskView { task, status }))
}

async fn update_link(
    State(coordinator): State<Coordinator>,
    Json(request): Json<LinkUpdateRequest>,
) -> Result<StatusCode, ApiError> {
    if request.link.rtt_ms < 0.0 || request.link.bandwidth_mbps <= 0.0 {
        return Err(ApiError::bad_request(
            "link requires non-negative RTT and positive bandwidth",
        ));
    }
    let mut state = coordinator.inner.lock().await;
    state.fabric.upsert_link(request.link);
    Ok(StatusCode::NO_CONTENT)
}

fn normalize_endpoint(endpoint: &str) -> Result<String, ApiError> {
    let endpoint = endpoint.trim().trim_end_matches('/');
    if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
        return Err(ApiError::bad_request(
            "data_endpoint must be an http:// or https:// URL",
        ));
    }
    Ok(endpoint.to_owned())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }

    fn from_fabric(error: FabricError) -> Self {
        let status = match error {
            FabricError::UnknownTask(_)
            | FabricError::UnknownObject(_)
            | FabricError::ResourceNotActive(_) => StatusCode::NOT_FOUND,
            FabricError::DuplicateTask(_)
            | FabricError::ObjectConflict(_)
            | FabricError::TaskNotPending(_)
            | FabricError::StaleExecution
            | FabricError::ExecutionExpired
            | FabricError::StaleResourceEpoch(_) => StatusCode::CONFLICT,
            FabricError::Scheduling(_) => StatusCode::UNPROCESSABLE_ENTITY,
        };
        Self {
            status,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
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
