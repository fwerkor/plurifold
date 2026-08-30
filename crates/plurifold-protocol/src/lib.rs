use plurifold_core::{
    CooperativeJobSpec, ExecutionLease, JobDefinition, JobId, LinkProfile, LogicalJobSpec,
    MembershipLease, ObjectId, ObjectMetadata, ResourceDescriptor, ResourceId, TaskId, TaskSpec,
};
use plurifold_runtime::{CooperativeJobStatus, CooperativePlan, CooperativeRoleView, TaskStatus};
use serde::{Deserialize, Serialize};

pub const API_VERSION: u32 = 4;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterResourceRequest {
    pub descriptor: ResourceDescriptor,
    pub data_endpoint: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterResourceResponse {
    pub lease: MembershipLease,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub resource_id: ResourceId,
    pub epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkPollRequest {
    pub resource_id: ResourceId,
    pub epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectReplica {
    pub resource_id: ResourceId,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedObject {
    pub metadata: ObjectMetadata,
    pub replicas: Vec<ObjectReplica>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkAssignment {
    pub lease: ExecutionLease,
    pub task: TaskSpec,
    pub inputs: Vec<ResolvedObject>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkPollResponse {
    pub assignment: Option<WorkAssignment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteExecutionRequest {
    pub lease: ExecutionLease,
    pub outputs: Vec<ObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RenewExecutionRequest {
    pub lease: ExecutionLease,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishObjectRequest {
    pub object: ObjectMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterReplicaRequest {
    pub object_id: ObjectId,
    pub resource_id: ResourceId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubmitTaskRequest {
    pub task: TaskSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubmitTaskResponse {
    pub task_id: TaskId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubmitCooperativeJobRequest {
    pub job: CooperativeJobSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubmitCooperativeJobResponse {
    pub job_id: JobId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanLogicalJobRequest {
    pub job: LogicalJobSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubmitLogicalJobResponse {
    pub job_id: JobId,
    pub initial_plan: Option<CooperativePlan>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CooperativeJobView {
    pub job: JobDefinition,
    pub status: CooperativeJobStatus,
    pub roles: Vec<CooperativeRoleView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskView {
    pub task: TaskSpec,
    pub status: TaskStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceView {
    pub descriptor: ResourceDescriptor,
    pub data_endpoint: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceListResponse {
    pub resources: Vec<ResourceView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LinkUpdateRequest {
    pub link: LinkProfile,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReportLinkMeasurementRequest {
    pub reporter_resource_id: ResourceId,
    pub reporter_epoch: u64,
    pub peer_resource_id: ResourceId,
    pub measurement: LinkMeasurement,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LinkMeasurement {
    Reachable { rtt_ms: f64, bandwidth_mbps: f64 },
    Unreachable,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PutObjectResponse {
    pub object: ObjectMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}
