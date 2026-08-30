use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use plurifold_core::{
    CooperativeJobSpec, ExecutionId, ExecutionLease, JobDefinition, JobId, LogicalJobSpec,
    MembershipLease, ObjectId, ObjectMetadata, ResourceDescriptor, ResourceId, TaskId, TaskSpec,
    TopologySnapshot,
};
use plurifold_scheduler::{
    FusionAdvisor, PlacementDecision, ScheduleError, TopologyAwareScheduler,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod planner;

pub use planner::{CooperativePlan, PlanError, PlannedPlacement, PlannedRole};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Running(ExecutionLease),
    Completed(Vec<ObjectId>),
    /// An Exclusive task lost its execution lease; replay requires reconciliation.
    Uncertain,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CooperativeRoleStatus {
    Waiting,
    /// Dependencies are complete, but no implementation is currently feasible.
    Ready,
    Submitted(TaskId),
    Completed(Vec<ObjectId>),
    Uncertain,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CooperativeJobStatus {
    Running,
    Completed(Vec<ObjectId>),
    Uncertain,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CooperativeRoleView {
    pub name: String,
    pub depends_on: Vec<String>,
    pub status: CooperativeRoleStatus,
    pub implementation: Option<String>,
    pub planned_resource: Option<ResourceId>,
    pub estimated_total_ms: Option<f64>,
    pub fusion: Option<RoleFusionView>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RoleFusionView {
    pub chain_roles: Vec<String>,
    pub stage_index: usize,
    pub estimated_avoided_transfer_ms: f64,
    pub estimated_vs_separate_ms: f64,
}

#[derive(Clone, Debug)]
struct RegisteredResource {
    descriptor: ResourceDescriptor,
    expires_at_unix_ms: u64,
}

#[derive(Clone, Debug)]
struct TaskRecord {
    spec: TaskSpec,
    status: TaskStatus,
}

#[derive(Clone, Debug)]
struct CooperativeJobRecord {
    definition: JobDefinition,
    roles: HashMap<String, CooperativeRoleStatus>,
    selections: HashMap<String, RoleSelection>,
}

#[derive(Clone, Debug)]
struct RoleSelection {
    implementation: String,
    planned_resource: ResourceId,
    estimated_total_ms: f64,
    fusion: Option<RoleFusionView>,
}

#[derive(Clone, Debug)]
enum TaskRoleBinding {
    Single {
        job_id: JobId,
        role_name: String,
    },
    FusedChain {
        job_id: JobId,
        role_names: Vec<String>,
    },
}

#[derive(Debug, Error)]
pub enum FabricError {
    #[error("resource {0} is not active")]
    ResourceNotActive(ResourceId),
    #[error("resource {0} epoch does not match current incarnation")]
    StaleResourceEpoch(ResourceId),
    #[error("task {0} does not exist")]
    UnknownTask(TaskId),
    #[error("object {0} does not exist")]
    UnknownObject(ObjectId),
    #[error("object {0} conflicts with already published metadata")]
    ObjectConflict(ObjectId),
    #[error("task {0} already exists")]
    DuplicateTask(TaskId),
    #[error("invalid task: {0}")]
    InvalidTask(String),
    #[error("cooperative job {0} does not exist")]
    UnknownJob(JobId),
    #[error("cooperative job {0} already exists")]
    DuplicateJob(JobId),
    #[error("invalid cooperative job: {0}")]
    InvalidCooperativeJob(String),
    #[error("invalid logical job: {0}")]
    InvalidLogicalJob(String),
    #[error("task {0} is not pending")]
    TaskNotPending(TaskId),
    #[error("execution lease is stale or does not own the task")]
    StaleExecution,
    #[error("execution lease has expired")]
    ExecutionExpired,
    #[error(transparent)]
    Scheduling(#[from] ScheduleError),
}

#[derive(Clone, Debug)]
pub struct Fabric {
    resources: HashMap<ResourceId, RegisteredResource>,
    objects: HashMap<ObjectId, ObjectMetadata>,
    tasks: HashMap<TaskId, TaskRecord>,
    jobs: HashMap<JobId, CooperativeJobRecord>,
    task_roles: HashMap<TaskId, TaskRoleBinding>,
    topology: TopologySnapshot,
    scheduler: TopologyAwareScheduler,
    membership_ttl_ms: u64,
    execution_ttl_ms: u64,
}

impl Default for Fabric {
    fn default() -> Self {
        Self::new(15_000, 60_000)
    }
}

impl Fabric {
    pub fn new(membership_ttl_ms: u64, execution_ttl_ms: u64) -> Self {
        Self {
            resources: HashMap::new(),
            objects: HashMap::new(),
            tasks: HashMap::new(),
            jobs: HashMap::new(),
            task_roles: HashMap::new(),
            topology: TopologySnapshot::default(),
            scheduler: TopologyAwareScheduler::default(),
            membership_ttl_ms,
            execution_ttl_ms,
        }
    }

    pub fn set_topology(&mut self, topology: TopologySnapshot) {
        self.topology = topology;
    }

    pub fn upsert_link(&mut self, link: plurifold_core::LinkProfile) {
        self.remove_link(link.from, link.to);
        self.topology.links.push(link);
    }

    pub fn remove_link(&mut self, from: ResourceId, to: ResourceId) {
        self.topology.links.retain(|existing| {
            !((existing.from == from && existing.to == to)
                || (existing.from == to && existing.to == from))
        });
    }

    pub fn topology(&self) -> &TopologySnapshot {
        &self.topology
    }

    pub fn register_resource(&mut self, mut descriptor: ResourceDescriptor) -> MembershipLease {
        let next_epoch = self
            .resources
            .get(&descriptor.id)
            .map(|existing| existing.descriptor.epoch + 1)
            .unwrap_or_else(|| descriptor.epoch.max(1));
        descriptor.epoch = next_epoch;
        let expires_at_unix_ms = now_unix_ms().saturating_add(self.membership_ttl_ms);
        let lease = MembershipLease {
            resource_id: descriptor.id,
            epoch: descriptor.epoch,
            expires_at_unix_ms,
        };
        self.resources.insert(
            descriptor.id,
            RegisteredResource {
                descriptor,
                expires_at_unix_ms,
            },
        );
        lease
    }

    pub fn heartbeat(
        &mut self,
        resource_id: ResourceId,
        epoch: u64,
    ) -> Result<MembershipLease, FabricError> {
        let registered = self
            .resources
            .get_mut(&resource_id)
            .ok_or(FabricError::ResourceNotActive(resource_id))?;
        if registered.descriptor.epoch != epoch {
            return Err(FabricError::StaleResourceEpoch(resource_id));
        }
        let expires_at_unix_ms = now_unix_ms().saturating_add(self.membership_ttl_ms);
        registered.expires_at_unix_ms = expires_at_unix_ms;
        Ok(MembershipLease {
            resource_id,
            epoch,
            expires_at_unix_ms,
        })
    }

    pub fn unregister_resource(&mut self, resource_id: ResourceId) {
        self.remove_resource(resource_id);
    }

    pub fn expire_resources_at(&mut self, unix_ms: u64) -> Vec<ResourceId> {
        let expired: Vec<_> = self
            .resources
            .iter()
            .filter(|(_, resource)| resource.expires_at_unix_ms <= unix_ms)
            .map(|(id, _)| *id)
            .collect();
        for id in &expired {
            self.remove_resource(*id);
        }
        expired
    }

    fn remove_resource(&mut self, resource_id: ResourceId) {
        self.resources.remove(&resource_id);
        for object in self.objects.values_mut() {
            object.locations.retain(|location| *location != resource_id);
        }
        self.topology
            .links
            .retain(|link| link.from != resource_id && link.to != resource_id);
    }

    pub fn publish_object(&mut self, object: ObjectMetadata) -> Result<(), FabricError> {
        if let Some(existing) = self.objects.get_mut(&object.id) {
            if !same_object_identity(existing, &object) {
                return Err(FabricError::ObjectConflict(object.id));
            }
            for location in object.locations {
                if !existing.locations.contains(&location) {
                    existing.locations.push(location);
                }
            }
            return Ok(());
        }
        self.objects.insert(object.id, object);
        Ok(())
    }

    pub fn add_object_location(
        &mut self,
        object_id: ObjectId,
        resource_id: ResourceId,
    ) -> Result<(), FabricError> {
        let object = self
            .objects
            .get_mut(&object_id)
            .ok_or(FabricError::UnknownObject(object_id))?;
        if !object.locations.contains(&resource_id) {
            object.locations.push(resource_id);
        }
        Ok(())
    }

    pub fn object_metadata(&self, object_id: ObjectId) -> Option<&ObjectMetadata> {
        self.objects.get(&object_id)
    }

    pub fn submit(&mut self, task: TaskSpec) -> Result<TaskId, FabricError> {
        validate_task(&task)?;
        let id = task.id;
        if self.tasks.contains_key(&id) {
            return Err(FabricError::DuplicateTask(id));
        }
        self.tasks.insert(
            id,
            TaskRecord {
                spec: task,
                status: TaskStatus::Pending,
            },
        );
        Ok(id)
    }

    pub fn submit_cooperative(&mut self, job: CooperativeJobSpec) -> Result<JobId, FabricError> {
        validate_cooperative_job(&job)?;
        let job_id = job.id;
        if self.jobs.contains_key(&job_id) {
            return Err(FabricError::DuplicateJob(job_id));
        }
        let roles = job
            .roles
            .iter()
            .map(|role| (role.name.clone(), CooperativeRoleStatus::Waiting))
            .collect();
        self.jobs.insert(
            job_id,
            CooperativeJobRecord {
                definition: JobDefinition::Cooperative(job),
                roles,
                selections: HashMap::new(),
            },
        );
        self.materialize_ready_roles(job_id)?;
        Ok(job_id)
    }

    pub fn submit_logical(&mut self, job: LogicalJobSpec) -> Result<JobId, FabricError> {
        planner::validate_logical_job(&job).map_err(|error| match error {
            PlanError::InvalidLogicalJob(message) => FabricError::InvalidLogicalJob(message),
            PlanError::NoFeasibleImplementation(_) => {
                unreachable!("validation does not place roles")
            }
        })?;
        let job_id = job.id;
        if self.jobs.contains_key(&job_id) {
            return Err(FabricError::DuplicateJob(job_id));
        }
        let roles = job
            .roles
            .iter()
            .map(|role| (role.name.clone(), CooperativeRoleStatus::Waiting))
            .collect();
        self.jobs.insert(
            job_id,
            CooperativeJobRecord {
                definition: JobDefinition::Logical(job),
                roles,
                selections: HashMap::new(),
            },
        );
        self.materialize_ready_roles(job_id)?;
        Ok(job_id)
    }

    pub fn job_definition(&self, job_id: JobId) -> Option<&JobDefinition> {
        self.jobs.get(&job_id).map(|record| &record.definition)
    }

    pub fn cooperative_job_status(&self, job_id: JobId) -> Option<CooperativeJobStatus> {
        let record = self.jobs.get(&job_id)?;
        if record
            .roles
            .values()
            .any(|status| *status == CooperativeRoleStatus::Uncertain)
        {
            return Some(CooperativeJobStatus::Uncertain);
        }
        if !record
            .roles
            .values()
            .all(|status| matches!(status, CooperativeRoleStatus::Completed(_)))
        {
            return Some(CooperativeJobStatus::Running);
        }
        let outputs = job_outputs(&record.definition)
            .iter()
            .flat_map(|role_name| match record.roles.get(role_name) {
                Some(CooperativeRoleStatus::Completed(outputs)) => outputs.clone(),
                _ => Vec::new(),
            })
            .collect();
        Some(CooperativeJobStatus::Completed(outputs))
    }

    pub fn cooperative_role_views(&self, job_id: JobId) -> Option<Vec<CooperativeRoleView>> {
        let record = self.jobs.get(&job_id)?;
        Some(
            job_roles(&record.definition)
                .into_iter()
                .map(|(name, depends_on)| CooperativeRoleView {
                    status: record
                        .roles
                        .get(&name)
                        .cloned()
                        .expect("validated cooperative role has runtime state"),
                    implementation: record
                        .selections
                        .get(&name)
                        .map(|selection| selection.implementation.clone()),
                    planned_resource: record
                        .selections
                        .get(&name)
                        .map(|selection| selection.planned_resource),
                    estimated_total_ms: record
                        .selections
                        .get(&name)
                        .map(|selection| selection.estimated_total_ms),
                    fusion: record
                        .selections
                        .get(&name)
                        .and_then(|selection| selection.fusion.clone()),
                    name,
                    depends_on,
                })
                .collect(),
        )
    }

    pub fn plan_logical_job(&self, spec: &LogicalJobSpec) -> Result<CooperativePlan, PlanError> {
        planner::plan(
            spec,
            &self.scheduler,
            &self.schedulable_resources(),
            &self.objects,
            &self.topology,
        )
    }

    pub fn task_status(&self, task_id: TaskId) -> Option<&TaskStatus> {
        self.tasks.get(&task_id).map(|record| &record.status)
    }

    pub fn task_spec(&self, task_id: TaskId) -> Option<&TaskSpec> {
        self.tasks.get(&task_id).map(|record| &record.spec)
    }

    pub fn pending_task_ids(&self) -> Vec<TaskId> {
        self.tasks
            .iter()
            .filter_map(|(id, record)| (record.status == TaskStatus::Pending).then_some(*id))
            .collect()
    }

    pub fn active_resources(&self) -> Vec<ResourceDescriptor> {
        let now = now_unix_ms();
        self.resources
            .values()
            .filter(|resource| resource.expires_at_unix_ms > now)
            .map(|resource| resource.descriptor.clone())
            .collect()
    }

    pub fn resource_epoch(&self, resource_id: ResourceId) -> Option<u64> {
        let now = now_unix_ms();
        self.resources
            .get(&resource_id)
            .filter(|resource| resource.expires_at_unix_ms > now)
            .map(|resource| resource.descriptor.epoch)
    }

    pub fn schedule_task(&self, task_id: TaskId) -> Result<PlacementDecision, FabricError> {
        let record = self
            .tasks
            .get(&task_id)
            .ok_or(FabricError::UnknownTask(task_id))?;
        if record.status != TaskStatus::Pending {
            return Err(FabricError::TaskNotPending(task_id));
        }
        let active = self.schedulable_resources();
        Ok(self
            .scheduler
            .choose(&record.spec, &active, &self.objects, &self.topology)?)
    }

    pub fn begin_execution(
        &mut self,
        task_id: TaskId,
        resource_id: ResourceId,
    ) -> Result<ExecutionLease, FabricError> {
        let now = now_unix_ms();
        let resource = self
            .resources
            .get(&resource_id)
            .filter(|resource| resource.expires_at_unix_ms > now)
            .ok_or(FabricError::ResourceNotActive(resource_id))?;
        let record = self
            .tasks
            .get_mut(&task_id)
            .ok_or(FabricError::UnknownTask(task_id))?;
        if record.status != TaskStatus::Pending {
            return Err(FabricError::TaskNotPending(task_id));
        }
        let lease = ExecutionLease {
            task_id,
            execution_id: ExecutionId::new(),
            resource_id,
            resource_epoch: resource.descriptor.epoch,
            expires_at_unix_ms: now.saturating_add(self.execution_ttl_ms),
        };
        record.status = TaskStatus::Running(lease.clone());
        Ok(lease)
    }

    pub fn renew_execution(
        &mut self,
        lease: &ExecutionLease,
    ) -> Result<ExecutionLease, FabricError> {
        let now = now_unix_ms();
        let resource = self
            .resources
            .get(&lease.resource_id)
            .filter(|resource| resource.expires_at_unix_ms > now)
            .ok_or(FabricError::ResourceNotActive(lease.resource_id))?;
        if resource.descriptor.epoch != lease.resource_epoch {
            return Err(FabricError::StaleResourceEpoch(lease.resource_id));
        }

        let record = self
            .tasks
            .get_mut(&lease.task_id)
            .ok_or(FabricError::UnknownTask(lease.task_id))?;
        let current = match &record.status {
            TaskStatus::Running(current) if current.execution_id == lease.execution_id => current,
            _ => return Err(FabricError::StaleExecution),
        };
        if current.expires_at_unix_ms <= now {
            return Err(FabricError::ExecutionExpired);
        }
        if current.resource_id != lease.resource_id
            || current.resource_epoch != lease.resource_epoch
        {
            return Err(FabricError::StaleExecution);
        }

        let renewed = ExecutionLease {
            expires_at_unix_ms: now.saturating_add(self.execution_ttl_ms),
            ..current.clone()
        };
        record.status = TaskStatus::Running(renewed.clone());
        Ok(renewed)
    }

    pub fn complete_execution(
        &mut self,
        lease: &ExecutionLease,
        outputs: Vec<ObjectMetadata>,
    ) -> Result<(), FabricError> {
        let now = now_unix_ms();
        let resource = self
            .resources
            .get(&lease.resource_id)
            .filter(|resource| resource.expires_at_unix_ms > now)
            .ok_or(FabricError::ResourceNotActive(lease.resource_id))?;
        if resource.descriptor.epoch != lease.resource_epoch {
            return Err(FabricError::StaleResourceEpoch(lease.resource_id));
        }
        let record = self
            .tasks
            .get(&lease.task_id)
            .ok_or(FabricError::UnknownTask(lease.task_id))?;
        match &record.status {
            TaskStatus::Running(current) if current.execution_id == lease.execution_id => {
                if current.expires_at_unix_ms <= now {
                    return Err(FabricError::ExecutionExpired);
                }
                if current.resource_id != lease.resource_id
                    || current.resource_epoch != lease.resource_epoch
                {
                    return Err(FabricError::StaleExecution);
                }
            }
            _ => return Err(FabricError::StaleExecution),
        }
        for output in &outputs {
            if let Some(existing) = self.objects.get(&output.id) {
                if !same_object_identity(existing, output) {
                    return Err(FabricError::ObjectConflict(output.id));
                }
            }
        }
        let output_ids: Vec<ObjectId> = outputs.iter().map(|object| object.id).collect();
        self.tasks
            .get_mut(&lease.task_id)
            .expect("task existence checked above")
            .status = TaskStatus::Completed(output_ids.clone());
        for object in outputs {
            self.publish_object(object)?;
        }
        self.complete_cooperative_role(lease.task_id, output_ids)?;
        Ok(())
    }

    /// Reap expired task execution leases. Replay-safe work becomes pending; Exclusive work becomes
    /// uncertain and requires application/operator reconciliation.
    pub fn reap_expired_executions_at(&mut self, unix_ms: u64) -> Vec<TaskId> {
        let mut changed = Vec::new();
        let mut uncertain = Vec::new();
        for (task_id, record) in &mut self.tasks {
            let expired = matches!(
                &record.status,
                TaskStatus::Running(lease) if lease.expires_at_unix_ms <= unix_ms
            );
            if !expired {
                continue;
            }
            record.status = if record.spec.effects.automatically_retryable() {
                TaskStatus::Pending
            } else {
                uncertain.push(*task_id);
                TaskStatus::Uncertain
            };
            changed.push(*task_id);
        }
        for task_id in uncertain {
            self.mark_cooperative_role_uncertain(task_id);
        }
        changed
    }

    fn complete_cooperative_role(
        &mut self,
        task_id: TaskId,
        outputs: Vec<ObjectId>,
    ) -> Result<(), FabricError> {
        let Some(binding) = self.task_roles.get(&task_id).cloned() else {
            return Ok(());
        };
        let job_id = match binding {
            TaskRoleBinding::Single { job_id, role_name } => {
                let job = self
                    .jobs
                    .get_mut(&job_id)
                    .ok_or(FabricError::UnknownJob(job_id))?;
                let status = job.roles.get_mut(&role_name).ok_or_else(|| {
                    FabricError::InvalidCooperativeJob(format!(
                        "runtime state is missing role {role_name}"
                    ))
                })?;
                *status = CooperativeRoleStatus::Completed(outputs);
                job_id
            }
            TaskRoleBinding::FusedChain { job_id, role_names } => {
                let job = self
                    .jobs
                    .get_mut(&job_id)
                    .ok_or(FabricError::UnknownJob(job_id))?;
                let Some((last_role, intermediate_roles)) = role_names.split_last() else {
                    return Err(FabricError::InvalidLogicalJob(
                        "runtime state contains an empty fused chain".into(),
                    ));
                };
                for role_name in intermediate_roles {
                    let status = job.roles.get_mut(role_name).ok_or_else(|| {
                        FabricError::InvalidLogicalJob(format!(
                            "runtime state is missing fused role {role_name}"
                        ))
                    })?;
                    *status = CooperativeRoleStatus::Completed(Vec::new());
                }
                let status = job.roles.get_mut(last_role).ok_or_else(|| {
                    FabricError::InvalidLogicalJob(format!(
                        "runtime state is missing fused role {last_role}"
                    ))
                })?;
                *status = CooperativeRoleStatus::Completed(outputs);
                job_id
            }
        };
        self.materialize_ready_roles(job_id)?;
        Ok(())
    }

    fn mark_cooperative_role_uncertain(&mut self, task_id: TaskId) {
        let Some(binding) = self.task_roles.get(&task_id).cloned() else {
            return;
        };
        match binding {
            TaskRoleBinding::Single { job_id, role_name } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    if let Some(status) = job.roles.get_mut(&role_name) {
                        *status = CooperativeRoleStatus::Uncertain;
                    }
                }
            }
            TaskRoleBinding::FusedChain { job_id, role_names } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    for role_name in role_names {
                        if let Some(status) = job.roles.get_mut(&role_name) {
                            *status = CooperativeRoleStatus::Uncertain;
                        }
                    }
                }
            }
        }
    }

    pub fn refresh_ready_roles(&mut self) -> Result<(), FabricError> {
        let job_ids = self.jobs.keys().copied().collect::<Vec<_>>();
        for job_id in job_ids {
            self.materialize_ready_roles(job_id)?;
        }
        Ok(())
    }

    fn materialize_ready_roles(&mut self, job_id: JobId) -> Result<(), FabricError> {
        enum ReadyRole {
            Cooperative {
                name: String,
                task: TaskSpec,
            },
            Logical {
                name: String,
                role: plurifold_core::LogicalRoleSpec,
                dependency_inputs: Vec<ObjectId>,
                fusion_chain: Vec<plurifold_core::LogicalRoleSpec>,
            },
        }

        let ready = {
            let job = self
                .jobs
                .get(&job_id)
                .ok_or(FabricError::UnknownJob(job_id))?;
            match &job.definition {
                JobDefinition::Cooperative(spec) => spec
                    .roles
                    .iter()
                    .filter(|role| {
                        job.roles.get(&role.name) == Some(&CooperativeRoleStatus::Waiting)
                    })
                    .filter_map(|role| {
                        dependency_outputs(&job.roles, &role.depends_on).map(|inputs| {
                            ReadyRole::Cooperative {
                                name: role.name.clone(),
                                task: role.task.instantiate(inputs),
                            }
                        })
                    })
                    .collect::<Vec<_>>(),
                JobDefinition::Logical(spec) => spec
                    .roles
                    .iter()
                    .filter(|role| {
                        matches!(
                            job.roles.get(&role.name),
                            Some(CooperativeRoleStatus::Waiting | CooperativeRoleStatus::Ready)
                        )
                    })
                    .filter_map(|role| {
                        dependency_outputs(&job.roles, &role.depends_on).map(|inputs| {
                            ReadyRole::Logical {
                                name: role.name.clone(),
                                role: role.clone(),
                                dependency_inputs: inputs,
                                fusion_chain: fusion_chain(spec, &job.roles, role),
                            }
                        })
                    })
                    .collect::<Vec<_>>(),
            }
        };

        for ready_role in ready {
            let (role_name, task, selection) = match ready_role {
                ReadyRole::Cooperative { name, task } => (name, task, None),
                ReadyRole::Logical {
                    name,
                    role,
                    dependency_inputs,
                    fusion_chain,
                } => {
                    let resources = self.schedulable_resources();
                    if let Some(selected) = planner::select_ready_fusion(
                        &fusion_chain,
                        &dependency_inputs,
                        &planner::FusionContext {
                            scheduler: &self.scheduler,
                            advisor: &FusionAdvisor::default(),
                            resources: &resources,
                            objects: &self.objects,
                            topology: &self.topology,
                        },
                    ) {
                        let stage_count = selected.implementations.len();
                        let role_names = fusion_chain
                            .iter()
                            .take(stage_count)
                            .map(|role| role.name.clone())
                            .collect::<Vec<_>>();
                        let task_id = selected.task.id;
                        self.submit(selected.task)?;
                        self.task_roles.insert(
                            task_id,
                            TaskRoleBinding::FusedChain {
                                job_id,
                                role_names: role_names.clone(),
                            },
                        );
                        let job = self
                            .jobs
                            .get_mut(&job_id)
                            .expect("logical job existence checked above");
                        for (stage_index, (role_name, implementation)) in
                            role_names.iter().zip(selected.implementations).enumerate()
                        {
                            job.roles.insert(
                                role_name.clone(),
                                CooperativeRoleStatus::Submitted(task_id),
                            );
                            job.selections.insert(
                                role_name.clone(),
                                RoleSelection {
                                    implementation,
                                    planned_resource: selected.resource_id,
                                    estimated_total_ms: selected.cost.total_ms,
                                    fusion: Some(RoleFusionView {
                                        chain_roles: role_names.clone(),
                                        stage_index,
                                        estimated_avoided_transfer_ms: selected
                                            .estimated_avoided_transfer_ms,
                                        estimated_vs_separate_ms: selected.estimated_vs_separate_ms,
                                    }),
                                },
                            );
                        }
                        continue;
                    }
                    let Some(selected) = planner::select_ready_role(
                        &role,
                        &dependency_inputs,
                        &self.scheduler,
                        &resources,
                        &self.objects,
                        &self.topology,
                    ) else {
                        self.jobs
                            .get_mut(&job_id)
                            .expect("logical job existence checked above")
                            .roles
                            .insert(name, CooperativeRoleStatus::Ready);
                        continue;
                    };
                    let selection = RoleSelection {
                        implementation: selected.implementation,
                        planned_resource: selected.resource_id,
                        estimated_total_ms: selected.cost.total_ms,
                        fusion: None,
                    };
                    (name, selected.task, Some(selection))
                }
            };

            let task_id = task.id;
            self.submit(task)?;
            self.task_roles.insert(
                task_id,
                TaskRoleBinding::Single {
                    job_id,
                    role_name: role_name.clone(),
                },
            );
            let job = self
                .jobs
                .get_mut(&job_id)
                .expect("job existence checked above");
            job.roles
                .insert(role_name.clone(), CooperativeRoleStatus::Submitted(task_id));
            if let Some(selection) = selection {
                job.selections.insert(role_name, selection);
            }
        }
        Ok(())
    }

    pub fn active_resource_count(&self) -> usize {
        let now = now_unix_ms();
        self.resources
            .values()
            .filter(|resource| resource.expires_at_unix_ms > now)
            .count()
    }

    fn schedulable_resources(&self) -> Vec<ResourceDescriptor> {
        let now = now_unix_ms();
        let busy_resources: HashSet<_> = self
            .tasks
            .values()
            .filter_map(|task| match &task.status {
                TaskStatus::Running(lease) if lease.expires_at_unix_ms > now => {
                    Some(lease.resource_id)
                }
                _ => None,
            })
            .collect();
        self.resources
            .values()
            .filter(|resource| {
                resource.expires_at_unix_ms > now
                    && !busy_resources.contains(&resource.descriptor.id)
            })
            .map(|resource| resource.descriptor.clone())
            .collect()
    }
}

fn job_outputs(definition: &JobDefinition) -> &[String] {
    match definition {
        JobDefinition::Cooperative(spec) => &spec.outputs,
        JobDefinition::Logical(spec) => &spec.outputs,
    }
}

fn job_roles(definition: &JobDefinition) -> Vec<(String, Vec<String>)> {
    match definition {
        JobDefinition::Cooperative(spec) => spec
            .roles
            .iter()
            .map(|role| (role.name.clone(), role.depends_on.clone()))
            .collect(),
        JobDefinition::Logical(spec) => spec
            .roles
            .iter()
            .map(|role| (role.name.clone(), role.depends_on.clone()))
            .collect(),
    }
}

fn fusion_chain(
    spec: &LogicalJobSpec,
    statuses: &HashMap<String, CooperativeRoleStatus>,
    first: &plurifold_core::LogicalRoleSpec,
) -> Vec<plurifold_core::LogicalRoleSpec> {
    let mut chain = vec![first.clone()];
    let mut current = first;
    loop {
        if spec.outputs.contains(&current.name) {
            break;
        }
        let mut consumers = spec.roles.iter().filter(|candidate| {
            candidate
                .depends_on
                .iter()
                .any(|dependency| dependency == &current.name)
        });
        let Some(consumer) = consumers.next() else {
            break;
        };
        if consumers.next().is_some()
            || consumer.depends_on.as_slice() != [current.name.as_str()]
            || statuses.get(&consumer.name) != Some(&CooperativeRoleStatus::Waiting)
        {
            break;
        }
        chain.push(consumer.clone());
        current = consumer;
    }
    chain
}

fn dependency_outputs(
    statuses: &HashMap<String, CooperativeRoleStatus>,
    dependencies: &[String],
) -> Option<Vec<ObjectId>> {
    let mut inputs = Vec::new();
    for dependency in dependencies {
        match statuses.get(dependency) {
            Some(CooperativeRoleStatus::Completed(outputs)) => {
                inputs.extend(outputs.iter().copied());
            }
            _ => return None,
        }
    }
    Some(inputs)
}

fn same_object_identity(left: &ObjectMetadata, right: &ObjectMetadata) -> bool {
    left.id == right.id
        && left.size_bytes == right.size_bytes
        && left.digest == right.digest
        && left.encoding == right.encoding
        && left.producer == right.producer
}

fn validate_task(task: &TaskSpec) -> Result<(), FabricError> {
    let Some(pipeline) = &task.pipeline else {
        return Ok(());
    };
    if task.artifact != "plurifold:pipeline" || task.entrypoint != "run" {
        return Err(FabricError::InvalidTask(
            "pipeline tasks require artifact plurifold:pipeline and entrypoint run".into(),
        ));
    }
    if task.effects != plurifold_core::EffectSemantics::Pure {
        return Err(FabricError::InvalidTask(
            "pipeline tasks currently require Pure effect semantics".into(),
        ));
    }
    if pipeline.stages.len() < 2 {
        return Err(FabricError::InvalidTask(
            "pipeline tasks require at least two stages".into(),
        ));
    }
    for (stage_index, stage) in pipeline.stages.iter().enumerate() {
        if !stage.artifact.starts_with("builtin:") {
            return Err(FabricError::InvalidTask(format!(
                "pipeline stage {stage_index} uses unsupported artifact family {}",
                stage.artifact
            )));
        }
        if stage.entrypoint != "run" {
            return Err(FabricError::InvalidTask(format!(
                "pipeline stage {stage_index} requires entrypoint run"
            )));
        }
        let previous_count = stage
            .inputs
            .iter()
            .filter(|input| matches!(input, plurifold_core::PipelineInput::PreviousOutput))
            .count();
        if (stage_index == 0 && previous_count != 0) || (stage_index > 0 && previous_count != 1) {
            return Err(FabricError::InvalidTask(format!(
                "pipeline stage {stage_index} has invalid previous-output bindings"
            )));
        }
        for input in &stage.inputs {
            if let plurifold_core::PipelineInput::External { index } = input {
                if *index >= task.inputs.len() {
                    return Err(FabricError::InvalidTask(format!(
                        "pipeline stage {stage_index} references external input index {index}, but the task has {} inputs",
                        task.inputs.len()
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_cooperative_job(job: &CooperativeJobSpec) -> Result<(), FabricError> {
    planner::validate_role_graph(
        job.roles
            .iter()
            .map(|role| (role.name.as_str(), role.depends_on.as_slice())),
        &job.outputs,
    )
    .map_err(FabricError::InvalidCooperativeJob)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use plurifold_core::{
        Architecture, CooperativeJobSpec, CooperativeRoleSpec, CostHint, EffectSemantics,
        LinkProfile, LogicalJobSpec, LogicalRoleSpec, ObjectMetadata, ResourceRequirements,
        RoleImplementation, TaskSpec, TaskTemplate,
    };

    use super::*;

    fn resource(id: ResourceId) -> ResourceDescriptor {
        ResourceDescriptor {
            id,
            epoch: 0,
            architecture: Architecture::X86_64,
            cpu_cores: 8,
            memory_bytes: 16 << 30,
            accelerators: vec![],
            features: BTreeSet::new(),
            performance_score: 1.0,
            queue_delay_ms: 0.0,
            startup_delay_ms: 0.0,
            failure_probability: 0.0,
        }
    }

    fn task(effects: EffectSemantics) -> TaskSpec {
        TaskSpec {
            id: TaskId::new(),
            artifact: "test".into(),
            entrypoint: "run".into(),
            arguments: vec![],
            inputs: vec![],
            requirements: ResourceRequirements::default(),
            effects,
            cost: CostHint::default(),
            pipeline: None,
        }
    }

    fn task_template(effects: EffectSemantics) -> TaskTemplate {
        TaskTemplate {
            artifact: "test".into(),
            entrypoint: "run".into(),
            arguments: vec![],
            inputs: vec![],
            requirements: ResourceRequirements::default(),
            effects,
            cost: CostHint::default(),
        }
    }

    fn logical_implementation(
        name: &str,
        feature: &str,
        compute_ms: f64,
        output_bytes: u64,
    ) -> RoleImplementation {
        RoleImplementation {
            name: name.into(),
            task: TaskTemplate {
                artifact: "builtin:identity".into(),
                entrypoint: "run".into(),
                arguments: vec![],
                inputs: vec![],
                requirements: ResourceRequirements {
                    required_features: BTreeSet::from([feature.to_owned()]),
                    ..ResourceRequirements::default()
                },
                effects: EffectSemantics::Pure,
                cost: CostHint {
                    compute_ms_on_reference: compute_ms,
                    output_bytes,
                },
            },
        }
    }

    fn fusion_resource(
        id: ResourceId,
        performance_score: f64,
        startup_delay_ms: f64,
        features: &[&str],
    ) -> ResourceDescriptor {
        ResourceDescriptor {
            id,
            epoch: 0,
            architecture: Architecture::X86_64,
            cpu_cores: 8,
            memory_bytes: 16 << 30,
            accelerators: vec![],
            features: features
                .iter()
                .map(|feature| (*feature).to_owned())
                .collect(),
            performance_score,
            queue_delay_ms: 0.0,
            startup_delay_ms,
            failure_probability: 0.0,
        }
    }

    fn fusion_job() -> LogicalJobSpec {
        LogicalJobSpec {
            id: JobId::new(),
            roles: vec![
                LogicalRoleSpec {
                    name: "producer".into(),
                    implementations: vec![RoleImplementation {
                        name: "producer-builtin".into(),
                        task: TaskTemplate {
                            artifact: "builtin:echo".into(),
                            entrypoint: "run".into(),
                            arguments: vec!["intermediate".into()],
                            inputs: vec![],
                            requirements: ResourceRequirements {
                                required_features: BTreeSet::from(["fusion:producer".into()]),
                                ..ResourceRequirements::default()
                            },
                            effects: EffectSemantics::Pure,
                            cost: CostHint {
                                compute_ms_on_reference: 100.0,
                                output_bytes: 106_250,
                            },
                        },
                    }],
                    depends_on: vec![],
                },
                LogicalRoleSpec {
                    name: "consumer".into(),
                    implementations: vec![RoleImplementation {
                        name: "consumer-builtin".into(),
                        task: TaskTemplate {
                            artifact: "builtin:identity".into(),
                            entrypoint: "run".into(),
                            arguments: vec![],
                            inputs: vec![],
                            requirements: ResourceRequirements {
                                required_features: BTreeSet::from(["fusion:consumer".into()]),
                                ..ResourceRequirements::default()
                            },
                            effects: EffectSemantics::Pure,
                            cost: CostHint {
                                compute_ms_on_reference: 1_000.0,
                                output_bytes: 12,
                            },
                        },
                    }],
                    depends_on: vec!["producer".into()],
                },
            ],
            outputs: vec!["consumer".into()],
        }
    }

    fn three_stage_fusion_job(middle_output_bytes: u64, final_compute_ms: f64) -> LogicalJobSpec {
        LogicalJobSpec {
            id: JobId::new(),
            roles: vec![
                LogicalRoleSpec {
                    name: "producer".into(),
                    implementations: vec![RoleImplementation {
                        name: "producer-builtin".into(),
                        task: TaskTemplate {
                            artifact: "builtin:echo".into(),
                            entrypoint: "run".into(),
                            arguments: vec!["intermediate".into()],
                            inputs: vec![],
                            requirements: ResourceRequirements {
                                required_features: BTreeSet::from(["fusion:producer".into()]),
                                ..ResourceRequirements::default()
                            },
                            effects: EffectSemantics::Pure,
                            cost: CostHint {
                                compute_ms_on_reference: 100.0,
                                output_bytes: 106_250,
                            },
                        },
                    }],
                    depends_on: vec![],
                },
                LogicalRoleSpec {
                    name: "middle".into(),
                    implementations: vec![RoleImplementation {
                        name: "middle-builtin".into(),
                        task: TaskTemplate {
                            artifact: "builtin:identity".into(),
                            entrypoint: "run".into(),
                            arguments: vec![],
                            inputs: vec![],
                            requirements: ResourceRequirements {
                                required_features: BTreeSet::from(["fusion:middle".into()]),
                                ..ResourceRequirements::default()
                            },
                            effects: EffectSemantics::Pure,
                            cost: CostHint {
                                compute_ms_on_reference: 100.0,
                                output_bytes: middle_output_bytes,
                            },
                        },
                    }],
                    depends_on: vec!["producer".into()],
                },
                LogicalRoleSpec {
                    name: "consumer".into(),
                    implementations: vec![RoleImplementation {
                        name: "consumer-builtin".into(),
                        task: TaskTemplate {
                            artifact: "builtin:identity".into(),
                            entrypoint: "run".into(),
                            arguments: vec![],
                            inputs: vec![],
                            requirements: ResourceRequirements {
                                required_features: BTreeSet::from(["fusion:consumer".into()]),
                                ..ResourceRequirements::default()
                            },
                            effects: EffectSemantics::Pure,
                            cost: CostHint {
                                compute_ms_on_reference: final_compute_ms,
                                output_bytes: 12,
                            },
                        },
                    }],
                    depends_on: vec!["middle".into()],
                },
            ],
            outputs: vec!["consumer".into()],
        }
    }

    #[test]
    fn resources_hot_join_and_leave_without_global_reconfiguration() {
        let mut fabric = Fabric::default();
        let first = ResourceId::new();
        let second = ResourceId::new();
        fabric.register_resource(resource(first));
        fabric.register_resource(resource(second));
        assert_eq!(fabric.active_resource_count(), 2);
        fabric.unregister_resource(first);
        assert_eq!(fabric.active_resource_count(), 1);
    }

    #[test]
    fn malformed_pipeline_is_rejected_at_submission() {
        let mut fabric = Fabric::default();
        let mut pipeline_task = task(EffectSemantics::Pure);
        pipeline_task.artifact = "plurifold:pipeline".into();
        pipeline_task.pipeline = Some(plurifold_core::TaskPipeline {
            stages: vec![
                plurifold_core::TaskPipelineStage {
                    artifact: "builtin:identity".into(),
                    entrypoint: "run".into(),
                    arguments: vec![],
                    inputs: vec![plurifold_core::PipelineInput::External { index: 0 }],
                },
                plurifold_core::TaskPipelineStage {
                    artifact: "builtin:identity".into(),
                    entrypoint: "run".into(),
                    arguments: vec![],
                    inputs: vec![plurifold_core::PipelineInput::PreviousOutput],
                },
            ],
        });

        let error = fabric.submit(pipeline_task).unwrap_err();
        assert!(matches!(error, FabricError::InvalidTask(_)));
    }

    #[test]
    fn high_transfer_cost_fuses_a_linear_logical_pair() {
        let mut fabric = Fabric::default();
        let local = ResourceId::new();
        let remote = ResourceId::new();
        fabric.register_resource(fusion_resource(
            local,
            1.0,
            500.0,
            &["fusion:producer", "fusion:consumer"],
        ));
        fabric.register_resource(fusion_resource(remote, 10.0, 0.0, &["fusion:consumer"]));
        fabric.upsert_link(LinkProfile {
            from: local,
            to: remote,
            rtt_ms: 100.0,
            bandwidth_mbps: 1.0,
        });

        let job_id = fabric.submit_logical(fusion_job()).unwrap();
        let views = fabric.cooperative_role_views(job_id).unwrap();
        let producer = views.iter().find(|role| role.name == "producer").unwrap();
        let consumer = views.iter().find(|role| role.name == "consumer").unwrap();
        let producer_task = match producer.status {
            CooperativeRoleStatus::Submitted(task_id) => task_id,
            ref status => panic!("producer was not submitted: {status:?}"),
        };
        let consumer_task = match consumer.status {
            CooperativeRoleStatus::Submitted(task_id) => task_id,
            ref status => panic!("consumer was not submitted: {status:?}"),
        };
        assert_eq!(producer_task, consumer_task);
        assert_eq!(producer.planned_resource, Some(local));
        assert_eq!(consumer.planned_resource, Some(local));
        assert_eq!(
            producer.fusion.as_ref().unwrap().chain_roles,
            vec!["producer".to_owned(), "consumer".to_owned()]
        );
        assert_eq!(producer.fusion.as_ref().unwrap().stage_index, 0);
        assert_eq!(consumer.fusion.as_ref().unwrap().stage_index, 1);
        assert!(
            producer
                .fusion
                .as_ref()
                .unwrap()
                .estimated_avoided_transfer_ms
                > 20.0
        );
        assert!(producer.fusion.as_ref().unwrap().estimated_vs_separate_ms >= 0.0);
        assert!(fabric.task_spec(producer_task).unwrap().pipeline.is_some());

        let lease = fabric.begin_execution(producer_task, local).unwrap();
        let output = ObjectId::new();
        fabric
            .complete_execution(
                &lease,
                vec![ObjectMetadata {
                    id: output,
                    size_bytes: 12,
                    digest: Some("sha256:test".into()),
                    encoding: Some("application/octet-stream".into()),
                    locations: vec![local],
                    producer: Some(producer_task),
                }],
            )
            .unwrap();
        let completed = fabric.cooperative_role_views(job_id).unwrap();
        let producer = completed
            .iter()
            .find(|role| role.name == "producer")
            .unwrap();
        let consumer = completed
            .iter()
            .find(|role| role.name == "consumer")
            .unwrap();
        assert_eq!(producer.status, CooperativeRoleStatus::Completed(vec![]));
        assert_eq!(
            consumer.status,
            CooperativeRoleStatus::Completed(vec![output])
        );
        assert_eq!(
            fabric.cooperative_job_status(job_id),
            Some(CooperativeJobStatus::Completed(vec![output]))
        );
    }

    #[test]
    fn high_transfer_cost_fuses_a_three_stage_chain() {
        let mut fabric = Fabric::default();
        let local = ResourceId::new();
        let remote = ResourceId::new();
        fabric.register_resource(fusion_resource(
            local,
            1.0,
            500.0,
            &["fusion:producer", "fusion:middle", "fusion:consumer"],
        ));
        fabric.register_resource(fusion_resource(
            remote,
            10.0,
            0.0,
            &["fusion:middle", "fusion:consumer"],
        ));
        fabric.upsert_link(LinkProfile {
            from: local,
            to: remote,
            rtt_ms: 100.0,
            bandwidth_mbps: 1.0,
        });

        let job_id = fabric
            .submit_logical(three_stage_fusion_job(106_250, 100.0))
            .unwrap();
        let views = fabric.cooperative_role_views(job_id).unwrap();
        let producer = views.iter().find(|role| role.name == "producer").unwrap();
        let middle = views.iter().find(|role| role.name == "middle").unwrap();
        let consumer = views.iter().find(|role| role.name == "consumer").unwrap();
        let task_id = match producer.status {
            CooperativeRoleStatus::Submitted(task_id) => task_id,
            ref status => panic!("producer was not submitted: {status:?}"),
        };
        assert_eq!(middle.status, CooperativeRoleStatus::Submitted(task_id));
        assert_eq!(consumer.status, CooperativeRoleStatus::Submitted(task_id));
        let expected_chain = vec![
            "producer".to_owned(),
            "middle".to_owned(),
            "consumer".to_owned(),
        ];
        assert_eq!(
            producer.fusion.as_ref().unwrap().chain_roles,
            expected_chain
        );
        assert_eq!(producer.fusion.as_ref().unwrap().stage_index, 0);
        assert_eq!(middle.fusion.as_ref().unwrap().stage_index, 1);
        assert_eq!(consumer.fusion.as_ref().unwrap().stage_index, 2);
        let pipeline = fabric
            .task_spec(task_id)
            .unwrap()
            .pipeline
            .as_ref()
            .unwrap();
        assert_eq!(pipeline.stages.len(), 3);

        let lease = fabric.begin_execution(task_id, local).unwrap();
        let output = ObjectId::new();
        fabric
            .complete_execution(
                &lease,
                vec![ObjectMetadata {
                    id: output,
                    size_bytes: 12,
                    digest: Some("sha256:three-stage".into()),
                    encoding: Some("application/octet-stream".into()),
                    locations: vec![local],
                    producer: Some(task_id),
                }],
            )
            .unwrap();
        let completed = fabric.cooperative_role_views(job_id).unwrap();
        assert_eq!(
            completed
                .iter()
                .find(|role| role.name == "producer")
                .unwrap()
                .status,
            CooperativeRoleStatus::Completed(vec![])
        );
        assert_eq!(
            completed
                .iter()
                .find(|role| role.name == "middle")
                .unwrap()
                .status,
            CooperativeRoleStatus::Completed(vec![])
        );
        assert_eq!(
            completed
                .iter()
                .find(|role| role.name == "consumer")
                .unwrap()
                .status,
            CooperativeRoleStatus::Completed(vec![output])
        );
    }

    #[test]
    fn chain_fusion_stops_when_leaving_the_tail_separate_is_cheaper() {
        let mut fabric = Fabric::default();
        let local = ResourceId::new();
        let remote = ResourceId::new();
        fabric.register_resource(fusion_resource(
            local,
            1.0,
            500.0,
            &["fusion:producer", "fusion:middle", "fusion:consumer"],
        ));
        fabric.register_resource(fusion_resource(
            remote,
            10.0,
            0.0,
            &["fusion:middle", "fusion:consumer"],
        ));
        fabric.upsert_link(LinkProfile {
            from: local,
            to: remote,
            rtt_ms: 100.0,
            bandwidth_mbps: 1.0,
        });

        let job_id = fabric
            .submit_logical(three_stage_fusion_job(1, 10_000.0))
            .unwrap();
        let views = fabric.cooperative_role_views(job_id).unwrap();
        let producer = views.iter().find(|role| role.name == "producer").unwrap();
        let middle = views.iter().find(|role| role.name == "middle").unwrap();
        let consumer = views.iter().find(|role| role.name == "consumer").unwrap();
        let fused_task = match producer.status {
            CooperativeRoleStatus::Submitted(task_id) => task_id,
            ref status => panic!("producer was not submitted: {status:?}"),
        };
        assert_eq!(middle.status, CooperativeRoleStatus::Submitted(fused_task));
        assert_eq!(consumer.status, CooperativeRoleStatus::Waiting);
        assert_eq!(
            producer.fusion.as_ref().unwrap().chain_roles,
            vec!["producer".to_owned(), "middle".to_owned()]
        );
        assert_eq!(
            fabric
                .task_spec(fused_task)
                .unwrap()
                .pipeline
                .as_ref()
                .unwrap()
                .stages
                .len(),
            2
        );

        let lease = fabric.begin_execution(fused_task, local).unwrap();
        let intermediate = ObjectId::new();
        fabric
            .complete_execution(
                &lease,
                vec![ObjectMetadata {
                    id: intermediate,
                    size_bytes: 1,
                    digest: Some("sha256:prefix".into()),
                    encoding: Some("application/octet-stream".into()),
                    locations: vec![local],
                    producer: Some(fused_task),
                }],
            )
            .unwrap();

        let after_prefix = fabric.cooperative_role_views(job_id).unwrap();
        let consumer = after_prefix
            .iter()
            .find(|role| role.name == "consumer")
            .unwrap();
        let consumer_task = match consumer.status {
            CooperativeRoleStatus::Submitted(task_id) => task_id,
            ref status => panic!("consumer was not submitted after prefix completion: {status:?}"),
        };
        assert_ne!(consumer_task, fused_task);
        assert!(consumer.fusion.is_none());
        assert_eq!(consumer.planned_resource, Some(remote));
    }

    #[test]
    fn non_fusable_tail_does_not_block_a_safe_prefix() {
        let mut fabric = Fabric::default();
        let local = ResourceId::new();
        let remote = ResourceId::new();
        fabric.register_resource(fusion_resource(
            local,
            1.0,
            500.0,
            &["fusion:producer", "fusion:middle", "fusion:consumer"],
        ));
        fabric.register_resource(fusion_resource(
            remote,
            10.0,
            0.0,
            &["fusion:middle", "fusion:consumer"],
        ));
        fabric.upsert_link(LinkProfile {
            from: local,
            to: remote,
            rtt_ms: 100.0,
            bandwidth_mbps: 1.0,
        });
        let mut job = three_stage_fusion_job(1, 10_000.0);
        job.roles[2].implementations[0].task.effects = EffectSemantics::Exclusive;

        let job_id = fabric.submit_logical(job).unwrap();
        let views = fabric.cooperative_role_views(job_id).unwrap();
        let producer = views.iter().find(|role| role.name == "producer").unwrap();
        let middle = views.iter().find(|role| role.name == "middle").unwrap();
        let consumer = views.iter().find(|role| role.name == "consumer").unwrap();
        let task_id = match producer.status {
            CooperativeRoleStatus::Submitted(task_id) => task_id,
            ref status => panic!("producer was not submitted: {status:?}"),
        };
        assert_eq!(middle.status, CooperativeRoleStatus::Submitted(task_id));
        assert_eq!(consumer.status, CooperativeRoleStatus::Waiting);
        assert_eq!(
            producer.fusion.as_ref().unwrap().chain_roles,
            vec!["producer".to_owned(), "middle".to_owned()]
        );
    }

    #[test]
    fn fast_link_keeps_a_linear_logical_pair_separate() {
        let mut fabric = Fabric::default();
        let local = ResourceId::new();
        let remote = ResourceId::new();
        fabric.register_resource(fusion_resource(
            local,
            1.0,
            500.0,
            &["fusion:producer", "fusion:consumer"],
        ));
        fabric.register_resource(fusion_resource(remote, 10.0, 0.0, &["fusion:consumer"]));
        fabric.upsert_link(LinkProfile {
            from: local,
            to: remote,
            rtt_ms: 1.0,
            bandwidth_mbps: 10_000.0,
        });

        let job_id = fabric.submit_logical(fusion_job()).unwrap();
        let views = fabric.cooperative_role_views(job_id).unwrap();
        let producer = views.iter().find(|role| role.name == "producer").unwrap();
        let consumer = views.iter().find(|role| role.name == "consumer").unwrap();
        let producer_task = match producer.status {
            CooperativeRoleStatus::Submitted(task_id) => task_id,
            ref status => panic!("producer was not submitted: {status:?}"),
        };
        assert_eq!(consumer.status, CooperativeRoleStatus::Waiting);
        assert!(producer.fusion.is_none());
        assert!(fabric.task_spec(producer_task).unwrap().pipeline.is_none());
    }

    #[test]
    fn branching_producer_is_not_fusion_eligible() {
        let producer = LogicalRoleSpec {
            name: "producer".into(),
            implementations: vec![logical_implementation("producer", "p", 1.0, 10)],
            depends_on: vec![],
        };
        let first_consumer = LogicalRoleSpec {
            name: "first".into(),
            implementations: vec![logical_implementation("first", "c", 1.0, 1)],
            depends_on: vec!["producer".into()],
        };
        let second_consumer = LogicalRoleSpec {
            name: "second".into(),
            implementations: vec![logical_implementation("second", "c", 1.0, 1)],
            depends_on: vec!["producer".into()],
        };
        let spec = LogicalJobSpec {
            id: JobId::new(),
            roles: vec![
                producer.clone(),
                first_consumer.clone(),
                second_consumer.clone(),
            ],
            outputs: vec!["first".into(), "second".into()],
        };
        let statuses = HashMap::from([
            ("producer".into(), CooperativeRoleStatus::Waiting),
            ("first".into(), CooperativeRoleStatus::Waiting),
            ("second".into(), CooperativeRoleStatus::Waiting),
        ]);

        assert_eq!(fusion_chain(&spec, &statuses, &producer), vec![producer]);
    }

    #[test]
    fn declared_intermediate_output_stops_chain_growth() {
        let mut spec = three_stage_fusion_job(10, 10.0);
        spec.outputs = vec!["middle".into(), "consumer".into()];
        let producer = spec
            .roles
            .iter()
            .find(|role| role.name == "producer")
            .unwrap();
        let statuses = spec
            .roles
            .iter()
            .map(|role| (role.name.clone(), CooperativeRoleStatus::Waiting))
            .collect::<HashMap<_, _>>();

        let chain = fusion_chain(&spec, &statuses, producer);
        assert_eq!(
            chain
                .iter()
                .map(|role| role.name.as_str())
                .collect::<Vec<_>>(),
            vec!["producer", "middle"]
        );
    }

    #[test]
    fn scheduler_spreads_work_away_from_busy_resources() {
        let mut fabric = Fabric::new(10_000, 10_000);
        let fast = ResourceId::new();
        let slow = ResourceId::new();
        let mut fast_resource = resource(fast);
        fast_resource.performance_score = 10.0;
        fabric.register_resource(fast_resource);
        fabric.register_resource(resource(slow));

        let first = task(EffectSemantics::Pure);
        let first_id = first.id;
        fabric.submit(first).unwrap();
        assert_eq!(fabric.schedule_task(first_id).unwrap().resource_id, fast);
        fabric.begin_execution(first_id, fast).unwrap();

        let second = task(EffectSemantics::Pure);
        let second_id = second.id;
        fabric.submit(second).unwrap();
        assert_eq!(fabric.schedule_task(second_id).unwrap().resource_id, slow);
    }

    #[test]
    fn expired_pure_work_is_replayable_but_exclusive_work_is_uncertain() {
        let mut fabric = Fabric::new(10_000, 0);
        let worker = ResourceId::new();
        fabric.register_resource(resource(worker));

        let pure = task(EffectSemantics::Pure);
        let pure_id = pure.id;
        fabric.submit(pure).unwrap();
        let pure_lease = fabric.begin_execution(pure_id, worker).unwrap();

        let exclusive = task(EffectSemantics::Exclusive);
        let exclusive_id = exclusive.id;
        fabric.submit(exclusive).unwrap();
        let exclusive_lease = fabric.begin_execution(exclusive_id, worker).unwrap();

        let reap_at = pure_lease
            .expires_at_unix_ms
            .max(exclusive_lease.expires_at_unix_ms);
        fabric.reap_expired_executions_at(reap_at);

        assert_eq!(fabric.task_status(pure_id), Some(&TaskStatus::Pending));
        assert_eq!(
            fabric.task_status(exclusive_id),
            Some(&TaskStatus::Uncertain)
        );
    }

    #[test]
    fn completion_publishes_immutable_output_metadata() {
        let mut fabric = Fabric::new(10_000, 10_000);
        let worker = ResourceId::new();
        fabric.register_resource(resource(worker));
        let task = task(EffectSemantics::Pure);
        let task_id = task.id;
        fabric.submit(task).unwrap();
        let lease = fabric.begin_execution(task_id, worker).unwrap();
        let output = ObjectMetadata {
            id: ObjectId::new(),
            size_bytes: 42,
            digest: Some("sha256:test".into()),
            encoding: Some("application/octet-stream".into()),
            locations: vec![worker],
            producer: Some(task_id),
        };
        let output_id = output.id;
        fabric.complete_execution(&lease, vec![output]).unwrap();
        assert_eq!(
            fabric.task_status(task_id),
            Some(&TaskStatus::Completed(vec![output_id]))
        );
    }

    #[test]
    fn old_resource_epoch_cannot_commit_after_reregistration() {
        let mut fabric = Fabric::new(10_000, 10_000);
        let worker = ResourceId::new();
        fabric.register_resource(resource(worker));
        let task = task(EffectSemantics::Pure);
        let task_id = task.id;
        fabric.submit(task).unwrap();
        let stale_execution = fabric.begin_execution(task_id, worker).unwrap();

        let new_membership = fabric.register_resource(resource(worker));
        assert!(new_membership.epoch > stale_execution.resource_epoch);

        let error = fabric
            .complete_execution(&stale_execution, Vec::new())
            .unwrap_err();
        assert!(matches!(error, FabricError::StaleResourceEpoch(id) if id == worker));
    }

    #[test]
    fn conflicting_object_publication_is_rejected() {
        let mut fabric = Fabric::default();
        let object_id = ObjectId::new();
        let first = ObjectMetadata {
            id: object_id,
            size_bytes: 4,
            digest: Some("sha256:first".into()),
            encoding: Some("application/octet-stream".into()),
            locations: vec![],
            producer: None,
        };
        fabric.publish_object(first).unwrap();

        let conflicting = ObjectMetadata {
            id: object_id,
            size_bytes: 5,
            digest: Some("sha256:second".into()),
            encoding: Some("application/octet-stream".into()),
            locations: vec![],
            producer: None,
        };
        let error = fabric.publish_object(conflicting).unwrap_err();
        assert!(matches!(error, FabricError::ObjectConflict(id) if id == object_id));
    }

    #[test]
    fn expired_resource_is_pruned_from_objects_and_topology() {
        let mut fabric = Fabric::new(100, 10_000);
        let expired = ResourceId::new();
        let survivor = ResourceId::new();
        let expired_lease = fabric.register_resource(resource(expired));
        std::thread::sleep(std::time::Duration::from_millis(2));
        let survivor_lease = fabric.register_resource(resource(survivor));
        assert!(survivor_lease.expires_at_unix_ms > expired_lease.expires_at_unix_ms);

        let object_id = ObjectId::new();
        fabric
            .publish_object(ObjectMetadata {
                id: object_id,
                size_bytes: 4,
                digest: Some("sha256:test".into()),
                encoding: None,
                locations: vec![expired, survivor],
                producer: None,
            })
            .unwrap();
        fabric.set_topology(TopologySnapshot {
            links: vec![plurifold_core::LinkProfile {
                from: expired,
                to: survivor,
                rtt_ms: 10.0,
                bandwidth_mbps: 100.0,
            }],
        });

        fabric.expire_resources_at(expired_lease.expires_at_unix_ms);

        assert_eq!(
            fabric.object_metadata(object_id).unwrap().locations,
            vec![survivor]
        );
        assert!(fabric.topology.links.is_empty());
    }

    #[test]
    fn cooperative_job_runs_independent_roles_before_join_role() {
        let mut fabric = Fabric::new(10_000, 10_000);
        let worker = ResourceId::new();
        fabric.register_resource(resource(worker));
        let job_id = JobId::new();
        fabric
            .submit_cooperative(CooperativeJobSpec {
                id: job_id,
                roles: vec![
                    CooperativeRoleSpec {
                        name: "left".into(),
                        task: task_template(EffectSemantics::Pure),
                        depends_on: vec![],
                    },
                    CooperativeRoleSpec {
                        name: "right".into(),
                        task: task_template(EffectSemantics::Pure),
                        depends_on: vec![],
                    },
                    CooperativeRoleSpec {
                        name: "join".into(),
                        task: task_template(EffectSemantics::Pure),
                        depends_on: vec!["left".into(), "right".into()],
                    },
                ],
                outputs: vec!["join".into()],
            })
            .unwrap();

        let roots = fabric.pending_task_ids();
        assert_eq!(roots.len(), 2);
        let views = fabric.cooperative_role_views(job_id).unwrap();
        assert!(views
            .iter()
            .any(|role| { role.name == "join" && role.status == CooperativeRoleStatus::Waiting }));

        let first = roots[0];
        let first_lease = fabric.begin_execution(first, worker).unwrap();
        let first_output = ObjectMetadata {
            id: ObjectId::new(),
            size_bytes: 1,
            digest: Some("sha256:first".into()),
            encoding: None,
            locations: vec![worker],
            producer: Some(first),
        };
        fabric
            .complete_execution(&first_lease, vec![first_output])
            .unwrap();
        assert_eq!(fabric.pending_task_ids().len(), 1);

        let second = roots[1];
        let second_lease = fabric.begin_execution(second, worker).unwrap();
        let second_output = ObjectMetadata {
            id: ObjectId::new(),
            size_bytes: 1,
            digest: Some("sha256:second".into()),
            encoding: None,
            locations: vec![worker],
            producer: Some(second),
        };
        fabric
            .complete_execution(&second_lease, vec![second_output])
            .unwrap();

        let pending = fabric.pending_task_ids();
        assert_eq!(pending.len(), 1);
        let join = fabric.task_spec(pending[0]).unwrap();
        assert_eq!(join.inputs.len(), 2);
        let join_id = join.id;
        assert!(matches!(
            fabric.cooperative_job_status(job_id),
            Some(CooperativeJobStatus::Running)
        ));

        let join_lease = fabric.begin_execution(join_id, worker).unwrap();
        let final_output = ObjectMetadata {
            id: ObjectId::new(),
            size_bytes: 2,
            digest: Some("sha256:joined".into()),
            encoding: None,
            locations: vec![worker],
            producer: Some(join_id),
        };
        let final_id = final_output.id;
        fabric
            .complete_execution(&join_lease, vec![final_output])
            .unwrap();
        assert_eq!(
            fabric.cooperative_job_status(job_id),
            Some(CooperativeJobStatus::Completed(vec![final_id]))
        );
    }

    #[test]
    fn cooperative_job_rejects_dependency_cycles() {
        let mut fabric = Fabric::default();
        let error = fabric
            .submit_cooperative(CooperativeJobSpec {
                id: JobId::new(),
                roles: vec![
                    CooperativeRoleSpec {
                        name: "a".into(),
                        task: task_template(EffectSemantics::Pure),
                        depends_on: vec!["b".into()],
                    },
                    CooperativeRoleSpec {
                        name: "b".into(),
                        task: task_template(EffectSemantics::Pure),
                        depends_on: vec!["a".into()],
                    },
                ],
                outputs: vec!["b".into()],
            })
            .unwrap_err();
        assert!(matches!(error, FabricError::InvalidCooperativeJob(_)));
    }

    #[test]
    fn exclusive_role_uncertainty_propagates_to_cooperative_job() {
        let mut fabric = Fabric::new(10_000, 0);
        let worker = ResourceId::new();
        fabric.register_resource(resource(worker));
        let job_id = JobId::new();
        fabric
            .submit_cooperative(CooperativeJobSpec {
                id: job_id,
                roles: vec![CooperativeRoleSpec {
                    name: "side-effect".into(),
                    task: task_template(EffectSemantics::Exclusive),
                    depends_on: vec![],
                }],
                outputs: vec!["side-effect".into()],
            })
            .unwrap();
        let task_id = fabric.pending_task_ids()[0];
        let lease = fabric.begin_execution(task_id, worker).unwrap();
        fabric.reap_expired_executions_at(lease.expires_at_unix_ms);
        assert_eq!(
            fabric.cooperative_job_status(job_id),
            Some(CooperativeJobStatus::Uncertain)
        );
    }

    #[test]
    fn logical_role_waits_until_a_compatible_resource_joins() {
        let mut fabric = Fabric::new(10_000, 10_000);
        let job_id = JobId::new();
        fabric
            .submit_logical(LogicalJobSpec {
                id: job_id,
                roles: vec![LogicalRoleSpec {
                    name: "compute".into(),
                    implementations: vec![logical_implementation(
                        "specialized",
                        "backend:special",
                        10.0,
                        1,
                    )],
                    depends_on: vec![],
                }],
                outputs: vec!["compute".into()],
            })
            .unwrap();

        let view = fabric.cooperative_role_views(job_id).unwrap();
        assert_eq!(view[0].status, CooperativeRoleStatus::Ready);
        assert!(fabric.pending_task_ids().is_empty());

        let worker = ResourceId::new();
        let mut descriptor = resource(worker);
        descriptor.features.insert("backend:special".into());
        fabric.register_resource(descriptor);
        fabric.refresh_ready_roles().unwrap();

        let view = fabric.cooperative_role_views(job_id).unwrap();
        assert!(matches!(
            view[0].status,
            CooperativeRoleStatus::Submitted(_)
        ));
        assert_eq!(view[0].implementation.as_deref(), Some("specialized"));
        assert_eq!(view[0].planned_resource, Some(worker));
    }

    #[test]
    fn downstream_logical_role_replans_when_a_better_resource_joins_before_readiness() {
        let mut fabric = Fabric::new(10_000, 10_000);
        let left = ResourceId::new();
        let mut left_resource = resource(left);
        left_resource
            .features
            .extend(["role:root".to_owned(), "join:slow".to_owned()]);
        fabric.register_resource(left_resource);

        let job_id = JobId::new();
        let logical = LogicalJobSpec {
            id: job_id,
            roles: vec![
                LogicalRoleSpec {
                    name: "root".into(),
                    implementations: vec![logical_implementation(
                        "root-left",
                        "role:root",
                        100.0,
                        1,
                    )],
                    depends_on: vec![],
                },
                LogicalRoleSpec {
                    name: "join".into(),
                    implementations: vec![
                        logical_implementation("slow-left", "join:slow", 1_000.0, 1),
                        logical_implementation("fast-new", "join:fast", 10.0, 1),
                    ],
                    depends_on: vec!["root".into()],
                },
            ],
            outputs: vec!["join".into()],
        };

        let preview = fabric.plan_logical_job(&logical).unwrap();
        assert_eq!(preview.roles[1].implementation, "slow-left");
        fabric.submit_logical(logical).unwrap();

        let root_task = fabric.pending_task_ids()[0];
        let root_lease = fabric.begin_execution(root_task, left).unwrap();

        let newcomer = ResourceId::new();
        let mut newcomer_resource = resource(newcomer);
        newcomer_resource.features.insert("join:fast".into());
        fabric.register_resource(newcomer_resource);
        fabric.upsert_link(plurifold_core::LinkProfile {
            from: left,
            to: newcomer,
            rtt_ms: 1.0,
            bandwidth_mbps: 10_000.0,
        });

        fabric
            .complete_execution(
                &root_lease,
                vec![ObjectMetadata {
                    id: ObjectId::new(),
                    size_bytes: 1,
                    digest: Some("sha256:root".into()),
                    encoding: None,
                    locations: vec![left],
                    producer: Some(root_task),
                }],
            )
            .unwrap();

        let join = fabric
            .cooperative_role_views(job_id)
            .unwrap()
            .into_iter()
            .find(|role| role.name == "join")
            .unwrap();
        assert_eq!(join.implementation.as_deref(), Some("fast-new"));
        assert_eq!(join.planned_resource, Some(newcomer));
        assert!(matches!(join.status, CooperativeRoleStatus::Submitted(_)));
    }
}
