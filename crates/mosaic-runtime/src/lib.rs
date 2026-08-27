use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use mosaic_core::{
    ExecutionId, ExecutionLease, MembershipLease, ObjectId, ObjectMetadata, ResourceDescriptor,
    ResourceId, TaskId, TaskSpec, TopologySnapshot,
};
use mosaic_scheduler::{PlacementDecision, ScheduleError, TopologyAwareScheduler};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running(ExecutionLease),
    Completed(Vec<ObjectId>),
    /// An Exclusive task lost its execution lease; replay requires reconciliation.
    Uncertain,
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

#[derive(Debug, Error)]
pub enum FabricError {
    #[error("resource {0} is not active")]
    ResourceNotActive(ResourceId),
    #[error("resource {0} epoch does not match current incarnation")]
    StaleResourceEpoch(ResourceId),
    #[error("task {0} does not exist")]
    UnknownTask(TaskId),
    #[error("task {0} already exists")]
    DuplicateTask(TaskId),
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
            topology: TopologySnapshot::default(),
            scheduler: TopologyAwareScheduler::default(),
            membership_ttl_ms,
            execution_ttl_ms,
        }
    }

    pub fn set_topology(&mut self, topology: TopologySnapshot) {
        self.topology = topology;
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
        self.resources.remove(&resource_id);
    }

    pub fn expire_resources_at(&mut self, unix_ms: u64) -> Vec<ResourceId> {
        let expired: Vec<_> = self
            .resources
            .iter()
            .filter(|(_, resource)| resource.expires_at_unix_ms <= unix_ms)
            .map(|(id, _)| *id)
            .collect();
        for id in &expired {
            self.resources.remove(id);
        }
        expired
    }

    pub fn publish_object(&mut self, object: ObjectMetadata) {
        self.objects.insert(object.id, object);
    }

    pub fn submit(&mut self, task: TaskSpec) -> Result<TaskId, FabricError> {
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

    pub fn task_status(&self, task_id: TaskId) -> Option<&TaskStatus> {
        self.tasks.get(&task_id).map(|record| &record.status)
    }

    pub fn schedule_task(&self, task_id: TaskId) -> Result<PlacementDecision, FabricError> {
        let record = self
            .tasks
            .get(&task_id)
            .ok_or(FabricError::UnknownTask(task_id))?;
        if record.status != TaskStatus::Pending {
            return Err(FabricError::TaskNotPending(task_id));
        }
        let now = now_unix_ms();
        let active: Vec<_> = self
            .resources
            .values()
            .filter(|resource| resource.expires_at_unix_ms > now)
            .map(|resource| resource.descriptor.clone())
            .collect();
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

    pub fn complete_execution(
        &mut self,
        lease: &ExecutionLease,
        outputs: Vec<ObjectMetadata>,
    ) -> Result<(), FabricError> {
        if lease.expires_at_unix_ms <= now_unix_ms() {
            return Err(FabricError::ExecutionExpired);
        }
        let record = self
            .tasks
            .get_mut(&lease.task_id)
            .ok_or(FabricError::UnknownTask(lease.task_id))?;
        match &record.status {
            TaskStatus::Running(current) if current.execution_id == lease.execution_id => {}
            _ => return Err(FabricError::StaleExecution),
        }
        let output_ids = outputs.iter().map(|object| object.id).collect();
        record.status = TaskStatus::Completed(output_ids);
        for object in outputs {
            self.objects.insert(object.id, object);
        }
        Ok(())
    }

    /// Reap expired task execution leases. Replay-safe work becomes pending; Exclusive work becomes
    /// uncertain and requires application/operator reconciliation.
    pub fn reap_expired_executions_at(&mut self, unix_ms: u64) -> Vec<TaskId> {
        let mut changed = Vec::new();
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
                TaskStatus::Uncertain
            };
            changed.push(*task_id);
        }
        changed
    }

    pub fn active_resource_count(&self) -> usize {
        let now = now_unix_ms();
        self.resources
            .values()
            .filter(|resource| resource.expires_at_unix_ms > now)
            .count()
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use mosaic_core::{
        Architecture, CostHint, EffectSemantics, ObjectMetadata, ResourceRequirements, TaskSpec,
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
            inputs: vec![],
            requirements: ResourceRequirements::default(),
            effects,
            cost: CostHint::default(),
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
}
