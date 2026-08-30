use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! opaque_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self)
            }
        }
    };
}

opaque_id!(TaskId);
opaque_id!(JobId);
opaque_id!(ObjectId);
opaque_id!(ResourceId);
opaque_id!(ExecutionId);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Architecture {
    X86_64,
    Aarch64,
    RiscV64,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AcceleratorKind {
    NvidiaGpu,
    AmdGpu,
    AscendNpu,
    Tpu,
    Fpga,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Accelerator {
    pub kind: AcceleratorKind,
    pub count: u32,
    pub memory_bytes_per_device: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AcceleratorRequirement {
    pub kind: AcceleratorKind,
    pub min_count: u32,
    pub min_memory_bytes_per_device: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceRequirements {
    pub architecture: Option<Architecture>,
    pub min_memory_bytes: u64,
    pub accelerator: Option<AcceleratorRequirement>,
    pub required_features: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceDescriptor {
    pub id: ResourceId,
    /// Incremented whenever a logical resource agent restarts/re-registers.
    pub epoch: u64,
    pub architecture: Architecture,
    pub cpu_cores: u32,
    pub memory_bytes: u64,
    pub accelerators: Vec<Accelerator>,
    /// Extensible executor/backend capabilities, e.g. `wasi:0.3`, `cuda:13`, `cann:8`.
    pub features: BTreeSet<String>,
    /// Relative to an arbitrary reference machine. Must be > 0 for scheduling.
    pub performance_score: f64,
    pub queue_delay_ms: f64,
    pub startup_delay_ms: f64,
    /// Estimated probability of resource loss during a representative task horizon.
    pub failure_probability: f64,
}

impl ResourceDescriptor {
    pub fn supports(&self, requirements: &ResourceRequirements) -> bool {
        if self.performance_score <= 0.0 || self.memory_bytes < requirements.min_memory_bytes {
            return false;
        }
        if let Some(architecture) = &requirements.architecture {
            if &self.architecture != architecture {
                return false;
            }
        }
        if !requirements.required_features.is_subset(&self.features) {
            return false;
        }
        if let Some(required) = &requirements.accelerator {
            let found = self.accelerators.iter().any(|candidate| {
                candidate.kind == required.kind
                    && candidate.count >= required.min_count
                    && candidate.memory_bytes_per_device >= required.min_memory_bytes_per_device
            });
            if !found {
                return false;
            }
        }
        true
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EffectSemantics {
    Pure,
    Idempotent {
        key: String,
    },
    /// Non-replay-safe external effects. Automatic retry after an ambiguous loss is forbidden.
    Exclusive,
}

impl EffectSemantics {
    pub fn automatically_retryable(&self) -> bool {
        matches!(self, Self::Pure | Self::Idempotent { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CostHint {
    pub compute_ms_on_reference: f64,
    pub output_bytes: u64,
}

impl Default for CostHint {
    fn default() -> Self {
        Self {
            compute_ms_on_reference: 1.0,
            output_bytes: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: TaskId,
    pub artifact: String,
    pub entrypoint: String,
    #[serde(default)]
    pub arguments: Vec<String>,
    pub inputs: Vec<ObjectId>,
    pub requirements: ResourceRequirements,
    pub effects: EffectSemantics,
    pub cost: CostHint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<TaskShard>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<TaskPipeline>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskShard {
    pub index: u32,
    pub count: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskPipeline {
    pub stages: Vec<TaskPipelineStage>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskPipelineStage {
    pub artifact: String,
    pub entrypoint: String,
    #[serde(default)]
    pub arguments: Vec<String>,
    pub inputs: Vec<PipelineInput>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PipelineInput {
    External { index: usize },
    PreviousOutput,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskTemplate {
    pub artifact: String,
    pub entrypoint: String,
    #[serde(default)]
    pub arguments: Vec<String>,
    #[serde(default)]
    pub inputs: Vec<ObjectId>,
    pub requirements: ResourceRequirements,
    pub effects: EffectSemantics,
    pub cost: CostHint,
}

impl TaskTemplate {
    pub fn instantiate(&self, dependency_inputs: impl IntoIterator<Item = ObjectId>) -> TaskSpec {
        let mut inputs = self.inputs.clone();
        inputs.extend(dependency_inputs);
        TaskSpec {
            id: TaskId::new(),
            artifact: self.artifact.clone(),
            entrypoint: self.entrypoint.clone(),
            arguments: self.arguments.clone(),
            inputs,
            requirements: self.requirements.clone(),
            effects: self.effects.clone(),
            cost: self.cost.clone(),
            shard: None,
            pipeline: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CooperativeRoleSpec {
    pub name: String,
    pub task: TaskTemplate,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CooperativeJobSpec {
    #[serde(default)]
    pub id: JobId,
    pub roles: Vec<CooperativeRoleSpec>,
    /// Outputs from these terminal roles form the logical job result.
    pub outputs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RoleImplementation {
    pub name: String,
    pub task: TaskTemplate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogicalRoleSpec {
    pub name: String,
    pub implementations: Vec<RoleImplementation>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Number of independent contributions required for this logical role. Each shard receives the
    /// same logical inputs plus a TaskShard { index, count } context. Cost hints are per shard.
    #[serde(default = "default_role_shards")]
    pub shards: u32,
}

const fn default_role_shards() -> u32 {
    1
}

/// A higher-level cooperative computation whose role boundaries are declared by the application or
/// a domain library while Plurifold chooses concrete implementations and predicts placements.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogicalJobSpec {
    #[serde(default)]
    pub id: JobId,
    pub roles: Vec<LogicalRoleSpec>,
    pub outputs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "spec", rename_all = "snake_case")]
pub enum JobDefinition {
    Cooperative(CooperativeJobSpec),
    Logical(LogicalJobSpec),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub id: ObjectId,
    pub size_bytes: u64,
    pub digest: Option<String>,
    pub encoding: Option<String>,
    pub locations: Vec<ResourceId>,
    pub producer: Option<TaskId>,
}

impl ObjectMetadata {
    pub fn is_local_to(&self, resource: ResourceId) -> bool {
        self.locations.contains(&resource)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LinkProfile {
    pub from: ResourceId,
    pub to: ResourceId,
    pub rtt_ms: f64,
    pub bandwidth_mbps: f64,
}

impl LinkProfile {
    pub fn transfer_time_ms(&self, bytes: u64) -> Option<f64> {
        if self.bandwidth_mbps <= 0.0 || self.rtt_ms < 0.0 {
            return None;
        }
        let bytes_per_ms = self.bandwidth_mbps * 1_000_000.0 / 8.0 / 1_000.0;
        Some(self.rtt_ms + bytes as f64 / bytes_per_ms)
    }

    pub fn latency_domain(&self) -> LatencyDomain {
        LatencyDomain::from_rtt_ms(self.rtt_ms)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum LatencyDomain {
    L0,
    L1,
    L2,
    L3,
    L4,
    L5,
}

impl LatencyDomain {
    pub fn from_rtt_ms(rtt_ms: f64) -> Self {
        if rtt_ms < 0.01 {
            Self::L0
        } else if rtt_ms < 0.2 {
            Self::L1
        } else if rtt_ms < 2.0 {
            Self::L2
        } else if rtt_ms < 20.0 {
            Self::L3
        } else if rtt_ms < 200.0 {
            Self::L4
        } else {
            Self::L5
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TopologySnapshot {
    pub links: Vec<LinkProfile>,
}

impl TopologySnapshot {
    pub fn link(&self, a: ResourceId, b: ResourceId) -> Option<&LinkProfile> {
        self.links
            .iter()
            .find(|link| (link.from == a && link.to == b) || (link.from == b && link.to == a))
    }

    pub fn transfer_time_ms(&self, a: ResourceId, b: ResourceId, bytes: u64) -> Option<f64> {
        if a == b {
            return Some(0.0);
        }
        self.link(a, b)?.transfer_time_ms(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MembershipLease {
    pub resource_id: ResourceId,
    pub epoch: u64,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionLease {
    pub task_id: TaskId,
    pub execution_id: ExecutionId,
    pub resource_id: ResourceId,
    pub resource_epoch: u64,
    pub expires_at_unix_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_latency_domains() {
        assert_eq!(LatencyDomain::from_rtt_ms(0.001), LatencyDomain::L0);
        assert_eq!(LatencyDomain::from_rtt_ms(1.0), LatencyDomain::L2);
        assert_eq!(LatencyDomain::from_rtt_ms(80.0), LatencyDomain::L4);
        assert_eq!(LatencyDomain::from_rtt_ms(250.0), LatencyDomain::L5);
    }

    #[test]
    fn exclusive_effects_are_not_automatically_retryable() {
        assert!(!EffectSemantics::Exclusive.automatically_retryable());
        assert!(EffectSemantics::Pure.automatically_retryable());
    }
}
