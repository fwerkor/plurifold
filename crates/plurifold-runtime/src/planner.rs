use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use plurifold_core::{
    AcceleratorRequirement, Architecture, AutoShardPolicy, EffectSemantics, LogicalJobSpec,
    ObjectId, ObjectMetadata, PipelineInput, ResourceDescriptor, ResourceId, ResourceRequirements,
    RoleReductionSpec, ShardPartitionSpec, ShardPolicy, ShardPolicySpec, TaskId, TaskPipeline,
    TaskPipelineStage, TaskShard, TaskShardPartition, TaskSpec, TaskTemplate, TopologySnapshot,
};
use plurifold_scheduler::{
    FusionAdvisor, PlacementBreakdown, ScheduleError, TopologyAwareScheduler,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_AUTO_SHARDS: u32 = 256;
const MAX_REDUCTION_FAN_IN: u32 = 64;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlannedPlacement {
    pub resource_id: ResourceId,
    pub start_ms: f64,
    pub finish_ms: f64,
    pub compute_ms: f64,
    pub input_transfer_ms: f64,
    pub queue_ms: f64,
    pub startup_ms: f64,
    pub risk_penalty_ms: f64,
    pub total_ms: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlannedShard {
    pub index: u32,
    pub implementation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<TaskShardPartition>,
    pub placement: PlannedPlacement,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlannedReduction {
    pub input_count: usize,
    pub placement: PlannedPlacement,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlannedReductionLevel {
    pub level: u32,
    pub reducer: String,
    pub reductions: Vec<PlannedReduction>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlannedRole {
    pub name: String,
    pub shards: Vec<PlannedShard>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reductions: Vec<PlannedReductionLevel>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CooperativePlan {
    pub roles: Vec<PlannedRole>,
    pub estimated_makespan_ms: f64,
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum PlanError {
    #[error("invalid logical job: {0}")]
    InvalidLogicalJob(String),
    #[error("role {0} has no feasible implementation on the current resources/topology")]
    NoFeasibleImplementation(String),
}

#[derive(Clone)]
struct SelectedRole {
    outputs: Vec<ObjectId>,
    finish_ms: f64,
}

pub(crate) struct ReadyRoleSelection {
    pub implementation: String,
    pub task: TaskSpec,
    pub resource_id: ResourceId,
    pub cost: PlacementBreakdown,
}

pub(crate) struct ReadyShardSelection {
    pub index: u32,
    pub implementation: String,
    pub task: TaskSpec,
    pub resource_id: ResourceId,
    pub start_ms: f64,
    pub cost: PlacementBreakdown,
}

pub(crate) enum ReadyReductionItem {
    Carry(ObjectId),
    Reduce(Box<ReadyReductionSelection>),
}

pub(crate) struct ReadyReductionSelection {
    pub input_count: usize,
    pub task: TaskSpec,
    pub resource_id: ResourceId,
    pub start_ms: f64,
    pub cost: PlacementBreakdown,
}

pub(crate) struct ReadyFusionSelection {
    pub implementations: Vec<String>,
    pub task: TaskSpec,
    pub resource_id: ResourceId,
    pub cost: PlacementBreakdown,
    pub estimated_avoided_transfer_ms: f64,
    pub estimated_vs_separate_ms: f64,
}

#[derive(Clone)]
struct SeparateChainState {
    implementation_index: usize,
    resource_id: ResourceId,
    output_bytes: u64,
    total_ms: f64,
    cross_transfer_ms: f64,
    has_cross_resource_edge: bool,
    has_fusion_trigger_edge: bool,
}

struct SeparateChainPaths {
    best: SeparateChainState,
    best_cross_resource: Option<SeparateChainState>,
}

struct FusedImplementationChoice {
    names: Vec<String>,
    tasks: Vec<TaskTemplate>,
    requirements: ResourceRequirements,
}

pub(crate) struct FusionContext<'a> {
    pub scheduler: &'a TopologyAwareScheduler,
    pub advisor: &'a FusionAdvisor,
    pub resources: &'a [ResourceDescriptor],
    pub objects: &'a HashMap<ObjectId, ObjectMetadata>,
    pub topology: &'a TopologySnapshot,
}

pub(crate) struct ShardPlanningContext<'a> {
    pub scheduler: &'a TopologyAwareScheduler,
    pub resources: &'a [ResourceDescriptor],
    pub objects: &'a HashMap<ObjectId, ObjectMetadata>,
    pub topology: &'a TopologySnapshot,
}

pub(crate) fn select_ready_role(
    role: &plurifold_core::LogicalRoleSpec,
    dependency_inputs: &[ObjectId],
    scheduler: &TopologyAwareScheduler,
    resources: &[ResourceDescriptor],
    objects: &HashMap<ObjectId, ObjectMetadata>,
    topology: &TopologySnapshot,
) -> Option<ReadyRoleSelection> {
    let mut best: Option<ReadyRoleSelection> = None;
    for implementation in &role.implementations {
        let task = implementation
            .task
            .instantiate(dependency_inputs.iter().copied());
        let Ok(decision) = scheduler.choose(&task, resources, objects, topology) else {
            continue;
        };
        let replace = best
            .as_ref()
            .map(|current| decision.cost.total_ms < current.cost.total_ms)
            .unwrap_or(true);
        if replace {
            best = Some(ReadyRoleSelection {
                implementation: implementation.name.clone(),
                task,
                resource_id: decision.resource_id,
                cost: decision.cost,
            });
        }
    }
    best
}

pub(crate) fn select_ready_shards(
    role: &plurifold_core::LogicalRoleSpec,
    dependency_inputs: &[ObjectId],
    context: &ShardPlanningContext<'_>,
) -> Option<Vec<ReadyShardSelection>> {
    let mut availability = HashMap::<ResourceId, f64>::new();
    choose_role_shards(role, dependency_inputs, 0.0, &mut availability, context)
}

fn choose_role_shards(
    role: &plurifold_core::LogicalRoleSpec,
    dependency_inputs: &[ObjectId],
    dependency_ready_ms: f64,
    resource_available_ms: &mut HashMap<ResourceId, f64>,
    context: &ShardPlanningContext<'_>,
) -> Option<Vec<ReadyShardSelection>> {
    match &role.shards {
        ShardPolicy::Fixed(count) => choose_shards(
            role,
            dependency_inputs,
            *count,
            None,
            dependency_ready_ms,
            resource_available_ms,
            context,
        ),
        ShardPolicy::Policy(ShardPolicySpec::Auto(policy)) => choose_auto_shards(
            role,
            dependency_inputs,
            policy,
            dependency_ready_ms,
            resource_available_ms,
            context,
        ),
    }
}

fn choose_auto_shards(
    role: &plurifold_core::LogicalRoleSpec,
    dependency_inputs: &[ObjectId],
    policy: &AutoShardPolicy,
    dependency_ready_ms: f64,
    resource_available_ms: &mut HashMap<ResourceId, f64>,
    context: &ShardPlanningContext<'_>,
) -> Option<Vec<ReadyShardSelection>> {
    let partition_units = partition_units(role, &policy.partition, context.objects)?;
    let max_shards = if partition_units == 0 {
        1
    } else {
        policy
            .max_shards
            .min(partition_units.min(u32::MAX as u64) as u32)
    };
    let base_availability = resource_available_ms.clone();
    let mut best: Option<(Vec<ReadyShardSelection>, HashMap<ResourceId, f64>, f64)> = None;

    for count in 1..=max_shards {
        let mut candidate_availability = base_availability.clone();
        let Some(selections) = choose_shards(
            role,
            dependency_inputs,
            count,
            Some(policy),
            dependency_ready_ms,
            &mut candidate_availability,
            context,
        ) else {
            continue;
        };
        let finish_ms = selections
            .iter()
            .map(|selection| selection.start_ms + selection.cost.total_ms)
            .fold(dependency_ready_ms, f64::max);
        let incremental_ms = (finish_ms - dependency_ready_ms).max(0.0);
        let replace = best
            .as_ref()
            .map(|(current, _, current_incremental)| {
                match incremental_ms.total_cmp(current_incremental) {
                    Ordering::Less => {
                        let gain_ratio = if *current_incremental <= 0.0 {
                            0.0
                        } else {
                            (*current_incremental - incremental_ms) / *current_incremental
                        };
                        gain_ratio >= policy.min_gain_ratio
                    }
                    Ordering::Equal => selections.len() < current.len(),
                    Ordering::Greater => false,
                }
            })
            .unwrap_or(true);
        if replace {
            best = Some((selections, candidate_availability, incremental_ms));
        }
    }

    let (selections, availability, _) = best?;
    *resource_available_ms = availability;
    Some(selections)
}

fn choose_shards(
    role: &plurifold_core::LogicalRoleSpec,
    dependency_inputs: &[ObjectId],
    shard_count: u32,
    auto_policy: Option<&AutoShardPolicy>,
    dependency_ready_ms: f64,
    resource_available_ms: &mut HashMap<ResourceId, f64>,
    context: &ShardPlanningContext<'_>,
) -> Option<Vec<ReadyShardSelection>> {
    let mut selections = Vec::with_capacity(shard_count as usize);
    for index in 0..shard_count {
        let mut best: Option<ReadyShardSelection> = None;
        for implementation in &role.implementations {
            let task = instantiate_shard_task(
                &implementation.task,
                dependency_inputs,
                index,
                shard_count,
                auto_policy,
                context.objects,
            )?;
            for resource in context.resources {
                let Some(cost) = placement_cost(
                    context.scheduler,
                    &task,
                    resource,
                    context.objects,
                    context.topology,
                ) else {
                    continue;
                };
                let start_ms = dependency_ready_ms.max(
                    resource_available_ms
                        .get(&resource.id)
                        .copied()
                        .unwrap_or(0.0),
                );
                let finish_ms = start_ms + cost.total_ms;
                let replace = best
                    .as_ref()
                    .map(|current| {
                        match finish_ms.total_cmp(&(current.start_ms + current.cost.total_ms)) {
                            Ordering::Less => true,
                            Ordering::Equal => cost.total_ms < current.cost.total_ms,
                            Ordering::Greater => false,
                        }
                    })
                    .unwrap_or(true);
                if replace {
                    best = Some(ReadyShardSelection {
                        index,
                        implementation: implementation.name.clone(),
                        task: task.clone(),
                        resource_id: resource.id,
                        start_ms,
                        cost,
                    });
                }
            }
        }
        let selected = best?;
        resource_available_ms.insert(
            selected.resource_id,
            selected.start_ms + selected.cost.total_ms,
        );
        selections.push(selected);
    }
    Some(selections)
}

fn instantiate_shard_task(
    template: &TaskTemplate,
    dependency_inputs: &[ObjectId],
    index: u32,
    count: u32,
    auto_policy: Option<&AutoShardPolicy>,
    objects: &HashMap<ObjectId, ObjectMetadata>,
) -> Option<TaskSpec> {
    let mut task = template.instantiate(dependency_inputs.iter().copied());
    let partition = match auto_policy {
        Some(policy) => Some(concrete_partition(
            template,
            &policy.partition,
            index,
            count,
            objects,
        )?),
        None => None,
    };
    if let Some(policy) = auto_policy {
        scale_auto_shard_cost(&mut task, partition.as_ref()?, policy);
    }
    task.shard = Some(TaskShard {
        index,
        count,
        partition,
    });
    Some(task)
}

fn partition_units(
    role: &plurifold_core::LogicalRoleSpec,
    partition: &ShardPartitionSpec,
    objects: &HashMap<ObjectId, ObjectMetadata>,
) -> Option<u64> {
    let template = &role.implementations.first()?.task;
    let object_id = template.inputs.get(partition.input())?;
    let object = objects.get(object_id)?;
    match partition {
        ShardPartitionSpec::ByteRange { .. } => Some(object.size_bytes),
        ShardPartitionSpec::Records { offsets, .. } => {
            valid_record_offsets(offsets, object.size_bytes).then_some((offsets.len() - 1) as u64)
        }
    }
}

fn concrete_partition(
    template: &TaskTemplate,
    partition: &ShardPartitionSpec,
    index: u32,
    count: u32,
    objects: &HashMap<ObjectId, ObjectMetadata>,
) -> Option<TaskShardPartition> {
    match partition {
        ShardPartitionSpec::ByteRange { input } => {
            let object_id = template.inputs.get(*input)?;
            let total_bytes = objects.get(object_id)?.size_bytes;
            let (offset, length) = partition_bounds(total_bytes, index, count);
            Some(TaskShardPartition::ByteRange {
                input: *input,
                offset,
                length,
                total_bytes,
            })
        }
        ShardPartitionSpec::Records { input, offsets } => {
            let object_id = template.inputs.get(*input)?;
            let total_bytes = objects.get(object_id)?.size_bytes;
            if !valid_record_offsets(offsets, total_bytes) {
                return None;
            }
            let total_records = (offsets.len() - 1) as u64;
            let (record_start, record_count) = partition_bounds(total_records, index, count);
            let first = usize::try_from(record_start).ok()?;
            let last = usize::try_from(record_start.checked_add(record_count)?).ok()?;
            let offset = *offsets.get(first)?;
            let end = *offsets.get(last)?;
            Some(TaskShardPartition::Records {
                input: *input,
                record_start,
                record_count,
                total_records,
                offset,
                length: end.checked_sub(offset)?,
                total_bytes,
            })
        }
    }
}

fn valid_record_offsets(offsets: &[u64], total_bytes: u64) -> bool {
    offsets.len() >= 2
        && offsets.first() == Some(&0)
        && offsets.last() == Some(&total_bytes)
        && offsets.windows(2).all(|pair| pair[0] < pair[1])
}

fn partition_bounds(total: u64, index: u32, count: u32) -> (u64, u64) {
    let start = (total as u128 * index as u128 / count as u128) as u64;
    let end = (total as u128 * (index + 1) as u128 / count as u128) as u64;
    (start, end - start)
}

fn scale_auto_shard_cost(
    task: &mut TaskSpec,
    partition: &TaskShardPartition,
    policy: &AutoShardPolicy,
) {
    let fraction = partition.work_fraction();
    task.cost.compute_ms_on_reference =
        task.cost.compute_ms_on_reference * fraction + policy.per_shard_overhead_ms;
    task.cost.output_bytes = partition.scaled_output_bytes(task.cost.output_bytes);
}

pub(crate) fn select_reduction_level(
    reduction: &RoleReductionSpec,
    inputs: &[ObjectId],
    dependency_ready_ms: f64,
    resource_available_ms: &mut HashMap<ResourceId, f64>,
    context: &ShardPlanningContext<'_>,
) -> Option<Vec<ReadyReductionItem>> {
    if inputs.len() <= 1 {
        return Some(
            inputs
                .iter()
                .copied()
                .map(ReadyReductionItem::Carry)
                .collect(),
        );
    }
    if reduction.max_fan_in < 2
        || !reduction.locality_rtt_ms.is_finite()
        || reduction.locality_rtt_ms < 0.0
    {
        return None;
    }
    let groups = reduction_groups(
        inputs,
        reduction.max_fan_in as usize,
        reduction.locality_rtt_ms,
        context.objects,
        context.topology,
    );
    let mut items = Vec::with_capacity(groups.len());
    for group in groups {
        if group.len() == 1 {
            items.push(ReadyReductionItem::Carry(group[0]));
            continue;
        }
        let task = reduction.task.instantiate(group.iter().copied());
        let mut best: Option<ReadyReductionSelection> = None;
        for resource in context.resources {
            let Some(cost) = placement_cost(
                context.scheduler,
                &task,
                resource,
                context.objects,
                context.topology,
            ) else {
                continue;
            };
            let start_ms = dependency_ready_ms.max(
                resource_available_ms
                    .get(&resource.id)
                    .copied()
                    .unwrap_or(0.0),
            );
            let finish_ms = start_ms + cost.total_ms;
            let replace = best
                .as_ref()
                .map(|current| {
                    match finish_ms.total_cmp(&(current.start_ms + current.cost.total_ms)) {
                        Ordering::Less => true,
                        Ordering::Equal => cost.total_ms < current.cost.total_ms,
                        Ordering::Greater => false,
                    }
                })
                .unwrap_or(true);
            if replace {
                best = Some(ReadyReductionSelection {
                    input_count: group.len(),
                    task: task.clone(),
                    resource_id: resource.id,
                    start_ms,
                    cost,
                });
            }
        }
        let selected = best?;
        resource_available_ms.insert(
            selected.resource_id,
            selected.start_ms + selected.cost.total_ms,
        );
        items.push(ReadyReductionItem::Reduce(Box::new(selected)));
    }
    Some(items)
}

fn reduction_groups(
    inputs: &[ObjectId],
    max_fan_in: usize,
    locality_rtt_ms: f64,
    objects: &HashMap<ObjectId, ObjectMetadata>,
    topology: &TopologySnapshot,
) -> Vec<Vec<ObjectId>> {
    let mut local_groups = Vec::<Vec<ObjectId>>::new();
    let mut current = Vec::<ObjectId>::new();
    for input in inputs.iter().copied() {
        let extend = !current.is_empty()
            && current.len() < max_fan_in
            && current.iter().copied().all(|existing| {
                reduction_inputs_are_local(existing, input, locality_rtt_ms, objects, topology)
            });
        if !current.is_empty() && !extend {
            local_groups.push(std::mem::take(&mut current));
        }
        current.push(input);
    }
    if !current.is_empty() {
        local_groups.push(current);
    }
    if local_groups.iter().any(|group| group.len() > 1) {
        return local_groups;
    }
    inputs
        .chunks(max_fan_in)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn reduction_inputs_are_local(
    left: ObjectId,
    right: ObjectId,
    locality_rtt_ms: f64,
    objects: &HashMap<ObjectId, ObjectMetadata>,
    topology: &TopologySnapshot,
) -> bool {
    let Some(left) = objects.get(&left) else {
        return false;
    };
    let Some(right) = objects.get(&right) else {
        return false;
    };
    left.locations.iter().any(|left_resource| {
        right.locations.iter().any(|right_resource| {
            left_resource == right_resource
                || topology
                    .link(*left_resource, *right_resource)
                    .is_some_and(|link| link.rtt_ms <= locality_rtt_ms)
        })
    })
}

pub(crate) fn select_ready_fusion(
    chain: &[plurifold_core::LogicalRoleSpec],
    dependency_inputs: &[ObjectId],
    context: &FusionContext<'_>,
) -> Option<ReadyFusionSelection> {
    let fusable_prefix_len = chain
        .iter()
        .take_while(|role| {
            role.implementations
                .iter()
                .all(|implementation| fusable_template(&implementation.task))
        })
        .count();
    if fusable_prefix_len < 2 {
        return None;
    }

    let full_separate = separate_chain_paths(chain, dependency_inputs, context)?;
    let mut best: Option<ReadyFusionSelection> = None;
    for prefix_len in 2..=fusable_prefix_len {
        let prefix = &chain[..prefix_len];
        let separate = separate_chain_paths(prefix, dependency_inputs, context)?;
        let Some(cross_resource) = separate.best_cross_resource.as_ref() else {
            continue;
        };
        if !cross_resource.has_fusion_trigger_edge {
            continue;
        }

        for choice in fused_implementation_choices(prefix) {
            let task = fused_pipeline_task(&choice.tasks, dependency_inputs, choice.requirements);
            let Ok(decision) = context.scheduler.choose(
                &task,
                context.resources,
                context.objects,
                context.topology,
            ) else {
                continue;
            };
            let projected_total_ms = if prefix_len == chain.len() {
                decision.cost.total_ms
            } else {
                let Some(projected) = best_chain_tail_total(
                    &chain[prefix_len..],
                    choice
                        .tasks
                        .last()
                        .expect("fusion prefix has at least two tasks")
                        .cost
                        .output_bytes,
                    decision.resource_id,
                    decision.cost.total_ms,
                    context,
                ) else {
                    continue;
                };
                projected
            };
            let estimated_vs_separate_ms = full_separate.best.total_ms - projected_total_ms;
            if estimated_vs_separate_ms < 0.0 {
                continue;
            }
            let candidate = ReadyFusionSelection {
                implementations: choice.names,
                task,
                resource_id: decision.resource_id,
                cost: decision.cost,
                estimated_avoided_transfer_ms: cross_resource.cross_transfer_ms,
                estimated_vs_separate_ms,
            };
            if fusion_candidate_is_better(&candidate, best.as_ref()) {
                best = Some(candidate);
            }
        }
    }
    best
}

fn best_chain_tail_total(
    tail: &[plurifold_core::LogicalRoleSpec],
    previous_output_bytes: u64,
    previous_resource: ResourceId,
    initial_total_ms: f64,
    context: &FusionContext<'_>,
) -> Option<f64> {
    let seed = SeparateChainState {
        implementation_index: usize::MAX,
        resource_id: previous_resource,
        output_bytes: previous_output_bytes,
        total_ms: initial_total_ms,
        cross_transfer_ms: 0.0,
        has_cross_resource_edge: false,
        has_fusion_trigger_edge: false,
    };
    let mut states = HashMap::from([((usize::MAX, previous_resource, false, false), seed)]);
    for role in tail {
        states = advance_chain_states(states, role, context)?;
    }
    states
        .into_values()
        .map(|state| state.total_ms)
        .min_by(f64::total_cmp)
}

fn fusion_candidate_is_better(
    candidate: &ReadyFusionSelection,
    current: Option<&ReadyFusionSelection>,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    match candidate
        .estimated_vs_separate_ms
        .total_cmp(&current.estimated_vs_separate_ms)
    {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => match candidate
            .estimated_avoided_transfer_ms
            .total_cmp(&current.estimated_avoided_transfer_ms)
        {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => candidate.implementations.len() > current.implementations.len(),
        },
    }
}

fn separate_chain_paths(
    chain: &[plurifold_core::LogicalRoleSpec],
    dependency_inputs: &[ObjectId],
    context: &FusionContext<'_>,
) -> Option<SeparateChainPaths> {
    let first = chain.first()?;
    let mut states = HashMap::<(usize, ResourceId, bool, bool), SeparateChainState>::new();
    for (implementation_index, implementation) in first.implementations.iter().enumerate() {
        let task = implementation
            .task
            .instantiate(dependency_inputs.iter().copied());
        for resource in context.resources {
            let Some(cost) = placement_cost(
                context.scheduler,
                &task,
                resource,
                context.objects,
                context.topology,
            ) else {
                continue;
            };
            let state = SeparateChainState {
                implementation_index,
                resource_id: resource.id,
                output_bytes: implementation.task.cost.output_bytes,
                total_ms: cost.total_ms,
                cross_transfer_ms: 0.0,
                has_cross_resource_edge: false,
                has_fusion_trigger_edge: false,
            };
            insert_better_chain_state(&mut states, state);
        }
    }
    if states.is_empty() {
        return None;
    }

    for role in chain.iter().skip(1) {
        states = advance_chain_states(states, role, context)?;
    }

    let best = states
        .values()
        .min_by(|left, right| left.total_ms.total_cmp(&right.total_ms))?
        .clone();
    let best_cross_resource = states
        .values()
        .filter(|state| state.has_cross_resource_edge)
        .min_by(|left, right| left.total_ms.total_cmp(&right.total_ms))
        .cloned();
    Some(SeparateChainPaths {
        best,
        best_cross_resource,
    })
}

fn advance_chain_states(
    states: HashMap<(usize, ResourceId, bool, bool), SeparateChainState>,
    role: &plurifold_core::LogicalRoleSpec,
    context: &FusionContext<'_>,
) -> Option<HashMap<(usize, ResourceId, bool, bool), SeparateChainState>> {
    let mut next_states = HashMap::<(usize, ResourceId, bool, bool), SeparateChainState>::new();
    for previous in states.into_values() {
        let intermediate = ObjectId::new();
        let mut predicted_objects = context.objects.clone();
        predicted_objects.insert(
            intermediate,
            ObjectMetadata {
                id: intermediate,
                size_bytes: previous.output_bytes,
                digest: None,
                encoding: None,
                locations: vec![previous.resource_id],
                producer: None,
            },
        );
        for (implementation_index, implementation) in role.implementations.iter().enumerate() {
            let task = implementation.task.instantiate([intermediate]);
            for resource in context.resources {
                let Some(cost) = placement_cost(
                    context.scheduler,
                    &task,
                    resource,
                    &predicted_objects,
                    context.topology,
                ) else {
                    continue;
                };
                let mut cross_transfer_ms = previous.cross_transfer_ms;
                let mut has_cross_resource_edge = previous.has_cross_resource_edge;
                let mut has_fusion_trigger_edge = previous.has_fusion_trigger_edge;
                if previous.resource_id != resource.id {
                    let link = context.topology.link(previous.resource_id, resource.id)?;
                    let recommendation = context.advisor.evaluate(previous.output_bytes, link);
                    cross_transfer_ms += recommendation.estimated_saved_transfer_ms;
                    has_cross_resource_edge = true;
                    has_fusion_trigger_edge |= recommendation.should_fuse;
                }
                insert_better_chain_state(
                    &mut next_states,
                    SeparateChainState {
                        implementation_index,
                        resource_id: resource.id,
                        output_bytes: implementation.task.cost.output_bytes,
                        total_ms: previous.total_ms + cost.total_ms,
                        cross_transfer_ms,
                        has_cross_resource_edge,
                        has_fusion_trigger_edge,
                    },
                );
            }
        }
    }
    (!next_states.is_empty()).then_some(next_states)
}

fn insert_better_chain_state(
    states: &mut HashMap<(usize, ResourceId, bool, bool), SeparateChainState>,
    candidate: SeparateChainState,
) {
    let key = (
        candidate.implementation_index,
        candidate.resource_id,
        candidate.has_cross_resource_edge,
        candidate.has_fusion_trigger_edge,
    );
    let replace = states
        .get(&key)
        .map(|current| {
            candidate.total_ms < current.total_ms
                || (candidate.total_ms == current.total_ms
                    && candidate.cross_transfer_ms > current.cross_transfer_ms)
        })
        .unwrap_or(true);
    if replace {
        states.insert(key, candidate);
    }
}

fn fused_implementation_choices(
    chain: &[plurifold_core::LogicalRoleSpec],
) -> Vec<FusedImplementationChoice> {
    let mut choices = Vec::<FusedImplementationChoice>::new();
    let Some(first) = chain.first() else {
        return choices;
    };
    for implementation in &first.implementations {
        choices.push(FusedImplementationChoice {
            names: vec![implementation.name.clone()],
            tasks: vec![implementation.task.clone()],
            requirements: implementation.task.requirements.clone(),
        });
    }
    for role in chain.iter().skip(1) {
        let previous = std::mem::take(&mut choices);
        for choice in previous {
            for implementation in &role.implementations {
                let Some(requirements) =
                    combine_requirements(&choice.requirements, &implementation.task.requirements)
                else {
                    continue;
                };
                let mut names = choice.names.clone();
                names.push(implementation.name.clone());
                let mut tasks = choice.tasks.clone();
                tasks.push(implementation.task.clone());
                choices.push(FusedImplementationChoice {
                    names,
                    tasks,
                    requirements,
                });
            }
        }
        if choices.is_empty() {
            break;
        }
    }
    choices
}

fn fusable_template(task: &TaskTemplate) -> bool {
    task.effects == EffectSemantics::Pure && task.artifact.starts_with("builtin:")
}

fn combine_requirements(
    left: &ResourceRequirements,
    right: &ResourceRequirements,
) -> Option<ResourceRequirements> {
    let architecture = combine_architecture(&left.architecture, &right.architecture)?;
    let accelerator = combine_accelerator(&left.accelerator, &right.accelerator)?;
    let mut required_features = left.required_features.clone();
    required_features.extend(right.required_features.iter().cloned());
    Some(ResourceRequirements {
        architecture,
        min_memory_bytes: left.min_memory_bytes.max(right.min_memory_bytes),
        accelerator,
        required_features,
    })
}

fn combine_architecture(
    left: &Option<Architecture>,
    right: &Option<Architecture>,
) -> Option<Option<Architecture>> {
    match (left, right) {
        (Some(left), Some(right)) if left != right => None,
        (Some(value), _) | (_, Some(value)) => Some(Some(value.clone())),
        (None, None) => Some(None),
    }
}

fn combine_accelerator(
    left: &Option<AcceleratorRequirement>,
    right: &Option<AcceleratorRequirement>,
) -> Option<Option<AcceleratorRequirement>> {
    match (left, right) {
        (Some(left), Some(right)) if left.kind != right.kind => None,
        (Some(left), Some(right)) => Some(Some(AcceleratorRequirement {
            kind: left.kind.clone(),
            min_count: left.min_count.max(right.min_count),
            min_memory_bytes_per_device: left
                .min_memory_bytes_per_device
                .max(right.min_memory_bytes_per_device),
        })),
        (Some(value), None) | (None, Some(value)) => Some(Some(value.clone())),
        (None, None) => Some(None),
    }
}

fn fused_pipeline_task(
    tasks: &[TaskTemplate],
    dependency_inputs: &[ObjectId],
    requirements: ResourceRequirements,
) -> TaskSpec {
    let first = tasks
        .first()
        .expect("fusion selection requires at least two stages")
        .instantiate(dependency_inputs.iter().copied());
    let mut external_inputs = Vec::<ObjectId>::new();
    let mut external_indexes = HashMap::<ObjectId, usize>::new();
    let first_inputs =
        pipeline_external_bindings(&first.inputs, &mut external_inputs, &mut external_indexes);

    let mut stages = Vec::with_capacity(tasks.len());
    stages.push(TaskPipelineStage {
        artifact: first.artifact,
        entrypoint: first.entrypoint,
        arguments: first.arguments,
        inputs: first_inputs,
    });
    for task in tasks.iter().skip(1) {
        let mut inputs =
            pipeline_external_bindings(&task.inputs, &mut external_inputs, &mut external_indexes);
        inputs.push(PipelineInput::PreviousOutput);
        stages.push(TaskPipelineStage {
            artifact: task.artifact.clone(),
            entrypoint: task.entrypoint.clone(),
            arguments: task.arguments.clone(),
            inputs,
        });
    }

    TaskSpec {
        id: TaskId::new(),
        artifact: "plurifold:pipeline".into(),
        entrypoint: "run".into(),
        arguments: vec![],
        inputs: external_inputs,
        requirements,
        effects: EffectSemantics::Pure,
        cost: plurifold_core::CostHint {
            compute_ms_on_reference: tasks
                .iter()
                .map(|task| task.cost.compute_ms_on_reference)
                .sum(),
            output_bytes: tasks.last().map(|task| task.cost.output_bytes).unwrap_or(0),
        },
        shard: None,
        pipeline: Some(TaskPipeline { stages }),
    }
}

fn pipeline_external_bindings(
    object_ids: &[ObjectId],
    external_inputs: &mut Vec<ObjectId>,
    external_indexes: &mut HashMap<ObjectId, usize>,
) -> Vec<PipelineInput> {
    object_ids
        .iter()
        .map(|object_id| {
            let index = match external_indexes.get(object_id) {
                Some(index) => *index,
                None => {
                    let index = external_inputs.len();
                    external_inputs.push(*object_id);
                    external_indexes.insert(*object_id, index);
                    index
                }
            };
            PipelineInput::External { index }
        })
        .collect()
}

pub(crate) fn validate_logical_job_objects(
    spec: &LogicalJobSpec,
    objects: &HashMap<ObjectId, ObjectMetadata>,
) -> Result<(), PlanError> {
    for role in &spec.roles {
        let Some(policy) = role.shards.auto() else {
            continue;
        };
        let ShardPartitionSpec::Records { input, offsets } = &policy.partition else {
            continue;
        };
        let object_id = role
            .implementations
            .first()
            .and_then(|implementation| implementation.task.inputs.get(*input))
            .copied()
            .ok_or_else(|| {
                PlanError::InvalidLogicalJob(format!(
                    "role {} record partition input {} is missing",
                    role.name, input
                ))
            })?;
        let object = objects.get(&object_id).ok_or_else(|| {
            PlanError::InvalidLogicalJob(format!(
                "role {} record partition requires published object {object_id}",
                role.name
            ))
        })?;
        if !valid_record_offsets(offsets, object.size_bytes) {
            return Err(PlanError::InvalidLogicalJob(format!(
                "role {} record offsets must end at object size {}",
                role.name, object.size_bytes
            )));
        }
    }
    Ok(())
}

pub(crate) fn plan(
    spec: &LogicalJobSpec,
    scheduler: &TopologyAwareScheduler,
    resources: &[ResourceDescriptor],
    objects: &HashMap<ObjectId, ObjectMetadata>,
    topology: &TopologySnapshot,
) -> Result<CooperativePlan, PlanError> {
    validate_logical_job(spec)?;
    validate_logical_job_objects(spec, objects)?;

    let mut predicted_objects = objects.clone();
    let mut selected = HashMap::<String, SelectedRole>::new();
    let mut resource_available_ms = HashMap::<ResourceId, f64>::new();
    let mut planned_roles = HashMap::<String, PlannedRole>::with_capacity(spec.roles.len());

    while selected.len() < spec.roles.len() {
        let ready = spec
            .roles
            .iter()
            .filter(|role| !selected.contains_key(&role.name))
            .filter(|role| {
                role.depends_on
                    .iter()
                    .all(|dependency| selected.contains_key(dependency))
            })
            .collect::<Vec<_>>();

        let Some(role) = ready.into_iter().min_by_key(|role| {
            feasible_candidate_count(
                role,
                scheduler,
                resources,
                &predicted_objects,
                topology,
                &selected,
            )
        }) else {
            return Err(PlanError::InvalidLogicalJob(
                "role dependency graph contains a cycle".into(),
            ));
        };

        let dependency_inputs = role
            .depends_on
            .iter()
            .flat_map(|dependency| selected[dependency].outputs.iter().copied())
            .collect::<Vec<_>>();
        let dependency_ready_ms = role
            .depends_on
            .iter()
            .map(|dependency| selected[dependency].finish_ms)
            .fold(0.0_f64, f64::max);

        let Some(shards) = choose_role_shards(
            role,
            &dependency_inputs,
            dependency_ready_ms,
            &mut resource_available_ms,
            &ShardPlanningContext {
                scheduler,
                resources,
                objects: &predicted_objects,
                topology,
            },
        ) else {
            return Err(PlanError::NoFeasibleImplementation(role.name.clone()));
        };

        let mut outputs = Vec::with_capacity(shards.len());
        let mut planned_shards = Vec::with_capacity(shards.len());
        let mut finish_ms = dependency_ready_ms;
        for shard in shards {
            let shard_finish_ms = shard.start_ms + shard.cost.total_ms;
            finish_ms = finish_ms.max(shard_finish_ms);
            let output = ObjectId::new();
            predicted_objects.insert(
                output,
                ObjectMetadata {
                    id: output,
                    size_bytes: shard.task.cost.output_bytes,
                    digest: None,
                    encoding: None,
                    locations: vec![shard.resource_id],
                    producer: None,
                },
            );
            outputs.push(output);
            planned_shards.push(PlannedShard {
                index: shard.index,
                implementation: shard.implementation,
                partition: shard
                    .task
                    .shard
                    .as_ref()
                    .and_then(|shard| shard.partition.clone()),
                placement: PlannedPlacement {
                    resource_id: shard.resource_id,
                    start_ms: shard.start_ms,
                    finish_ms: shard_finish_ms,
                    compute_ms: shard.cost.compute_ms,
                    input_transfer_ms: shard.cost.input_transfer_ms,
                    queue_ms: shard.cost.queue_ms,
                    startup_ms: shard.cost.startup_ms,
                    risk_penalty_ms: shard.cost.risk_penalty_ms,
                    total_ms: shard.cost.total_ms,
                },
            });
        }

        let mut planned_reductions = Vec::new();
        if let Some(reduction) = &role.reduction {
            let mut level = 0u32;
            while outputs.len() > 1 {
                let level_ready_ms = finish_ms;
                let Some(items) = select_reduction_level(
                    reduction,
                    &outputs,
                    level_ready_ms,
                    &mut resource_available_ms,
                    &ShardPlanningContext {
                        scheduler,
                        resources,
                        objects: &predicted_objects,
                        topology,
                    },
                ) else {
                    return Err(PlanError::NoFeasibleImplementation(role.name.clone()));
                };
                let mut next_outputs = Vec::with_capacity(items.len());
                let mut reductions = Vec::new();
                let mut level_finish_ms = level_ready_ms;
                let before = outputs.len();
                for item in items {
                    match item {
                        ReadyReductionItem::Carry(object) => next_outputs.push(object),
                        ReadyReductionItem::Reduce(reduction_selection) => {
                            let reduction_finish_ms =
                                reduction_selection.start_ms + reduction_selection.cost.total_ms;
                            level_finish_ms = level_finish_ms.max(reduction_finish_ms);
                            let output = ObjectId::new();
                            predicted_objects.insert(
                                output,
                                ObjectMetadata {
                                    id: output,
                                    size_bytes: reduction_selection.task.cost.output_bytes,
                                    digest: None,
                                    encoding: None,
                                    locations: vec![reduction_selection.resource_id],
                                    producer: None,
                                },
                            );
                            next_outputs.push(output);
                            reductions.push(PlannedReduction {
                                input_count: reduction_selection.input_count,
                                placement: PlannedPlacement {
                                    resource_id: reduction_selection.resource_id,
                                    start_ms: reduction_selection.start_ms,
                                    finish_ms: reduction_finish_ms,
                                    compute_ms: reduction_selection.cost.compute_ms,
                                    input_transfer_ms: reduction_selection.cost.input_transfer_ms,
                                    queue_ms: reduction_selection.cost.queue_ms,
                                    startup_ms: reduction_selection.cost.startup_ms,
                                    risk_penalty_ms: reduction_selection.cost.risk_penalty_ms,
                                    total_ms: reduction_selection.cost.total_ms,
                                },
                            });
                        }
                    }
                }
                if next_outputs.len() >= before {
                    return Err(PlanError::InvalidLogicalJob(format!(
                        "role {} reduction made no progress",
                        role.name
                    )));
                }
                outputs = next_outputs;
                finish_ms = level_finish_ms;
                planned_reductions.push(PlannedReductionLevel {
                    level,
                    reducer: reduction.name.clone(),
                    reductions,
                });
                level += 1;
            }
        }

        selected.insert(role.name.clone(), SelectedRole { outputs, finish_ms });
        planned_roles.insert(
            role.name.clone(),
            PlannedRole {
                name: role.name.clone(),
                shards: planned_shards,
                reductions: planned_reductions,
            },
        );
    }

    let estimated_makespan_ms = spec
        .outputs
        .iter()
        .map(|role| selected[role].finish_ms)
        .fold(0.0_f64, f64::max);

    Ok(CooperativePlan {
        roles: spec
            .roles
            .iter()
            .map(|role| planned_roles[&role.name].clone())
            .collect(),
        estimated_makespan_ms,
    })
}

fn placement_cost(
    scheduler: &TopologyAwareScheduler,
    task: &plurifold_core::TaskSpec,
    resource: &ResourceDescriptor,
    objects: &HashMap<ObjectId, ObjectMetadata>,
    topology: &TopologySnapshot,
) -> Option<PlacementBreakdown> {
    match scheduler.choose(task, std::slice::from_ref(resource), objects, topology) {
        Ok(decision) => Some(decision.cost),
        Err(ScheduleError::MissingObject(_) | ScheduleError::NoCandidate) => None,
    }
}

fn feasible_candidate_count(
    role: &plurifold_core::LogicalRoleSpec,
    scheduler: &TopologyAwareScheduler,
    resources: &[ResourceDescriptor],
    objects: &HashMap<ObjectId, ObjectMetadata>,
    topology: &TopologySnapshot,
    selected: &HashMap<String, SelectedRole>,
) -> usize {
    let dependency_inputs = role
        .depends_on
        .iter()
        .flat_map(|dependency| {
            selected
                .get(dependency)
                .into_iter()
                .flat_map(|role| role.outputs.iter().copied())
        })
        .collect::<Vec<_>>();

    role.implementations
        .iter()
        .map(|implementation| {
            let task = implementation
                .task
                .instantiate(dependency_inputs.iter().copied());
            resources
                .iter()
                .filter(|resource| {
                    placement_cost(scheduler, &task, resource, objects, topology).is_some()
                })
                .count()
        })
        .sum()
}

pub(crate) fn validate_logical_job(spec: &LogicalJobSpec) -> Result<(), PlanError> {
    validate_role_graph(
        spec.roles
            .iter()
            .map(|role| (role.name.as_str(), role.depends_on.as_slice())),
        &spec.outputs,
    )
    .map_err(PlanError::InvalidLogicalJob)?;

    for role in &spec.roles {
        match &role.shards {
            ShardPolicy::Fixed(0) => {
                return Err(PlanError::InvalidLogicalJob(format!(
                    "role {} must request at least one shard",
                    role.name
                )));
            }
            ShardPolicy::Policy(ShardPolicySpec::Auto(policy)) => {
                if policy.max_shards == 0 || policy.max_shards > MAX_AUTO_SHARDS {
                    return Err(PlanError::InvalidLogicalJob(format!(
                        "role {} auto max_shards must be between 1 and {MAX_AUTO_SHARDS}",
                        role.name
                    )));
                }
                if !policy.per_shard_overhead_ms.is_finite() || policy.per_shard_overhead_ms < 0.0 {
                    return Err(PlanError::InvalidLogicalJob(format!(
                        "role {} auto per_shard_overhead_ms must be finite and non-negative",
                        role.name
                    )));
                }
                if !policy.min_gain_ratio.is_finite()
                    || policy.min_gain_ratio < 0.0
                    || policy.min_gain_ratio >= 1.0
                {
                    return Err(PlanError::InvalidLogicalJob(format!(
                        "role {} auto min_gain_ratio must be in [0, 1)",
                        role.name
                    )));
                }
            }
            ShardPolicy::Fixed(_) => {}
        }
        if let Some(reduction) = &role.reduction {
            if role.shards.fixed_count() == Some(1) {
                return Err(PlanError::InvalidLogicalJob(format!(
                    "role {} cannot reduce an unsharded fixed role",
                    role.name
                )));
            }
            if reduction.name.trim().is_empty() {
                return Err(PlanError::InvalidLogicalJob(format!(
                    "role {} reduction name cannot be empty",
                    role.name
                )));
            }
            if reduction.max_fan_in < 2 || reduction.max_fan_in > MAX_REDUCTION_FAN_IN {
                return Err(PlanError::InvalidLogicalJob(format!(
                    "role {} reduction max_fan_in must be between 2 and {MAX_REDUCTION_FAN_IN}",
                    role.name
                )));
            }
            if !reduction.locality_rtt_ms.is_finite() || reduction.locality_rtt_ms < 0.0 {
                return Err(PlanError::InvalidLogicalJob(format!(
                    "role {} reduction locality_rtt_ms must be finite and non-negative",
                    role.name
                )));
            }
            if !reduction.task.inputs.is_empty() {
                return Err(PlanError::InvalidLogicalJob(format!(
                    "role {} reduction task cannot declare static inputs",
                    role.name
                )));
            }
            if reduction.task.effects != EffectSemantics::Pure {
                return Err(PlanError::InvalidLogicalJob(format!(
                    "role {} reduction task must be Pure",
                    role.name
                )));
            }
        }
        if role.implementations.is_empty() {
            return Err(PlanError::InvalidLogicalJob(format!(
                "role {} has no implementations",
                role.name
            )));
        }
        if let Some(policy) = role.shards.auto() {
            let input_index = policy.partition.input();
            if let ShardPartitionSpec::Records { offsets, .. } = &policy.partition {
                if offsets.len() < 2
                    || offsets.first() != Some(&0)
                    || !offsets.windows(2).all(|pair| pair[0] < pair[1])
                {
                    return Err(PlanError::InvalidLogicalJob(format!(
                        "role {} record offsets must start at 0 and be strictly increasing",
                        role.name
                    )));
                }
            }
            let expected_input = role.implementations[0]
                .task
                .inputs
                .get(input_index)
                .copied()
                .ok_or_else(|| {
                    PlanError::InvalidLogicalJob(format!(
                        "role {} auto partition input {} is not an explicit TaskTemplate input",
                        role.name, input_index
                    ))
                })?;
            if role.implementations.iter().any(|implementation| {
                implementation.task.inputs.get(input_index).copied() != Some(expected_input)
            }) {
                return Err(PlanError::InvalidLogicalJob(format!(
                    "role {} auto partition input {} must reference the same Object across implementations",
                    role.name, input_index
                )));
            }
        }
        let mut implementation_names = HashSet::with_capacity(role.implementations.len());
        for implementation in &role.implementations {
            if implementation.name.trim().is_empty() {
                return Err(PlanError::InvalidLogicalJob(format!(
                    "role {} has an implementation with an empty name",
                    role.name
                )));
            }
            if !implementation_names.insert(implementation.name.as_str()) {
                return Err(PlanError::InvalidLogicalJob(format!(
                    "role {} has duplicate implementation {}",
                    role.name, implementation.name
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_role_graph<'a>(
    roles: impl IntoIterator<Item = (&'a str, &'a [String])>,
    outputs: &[String],
) -> Result<(), String> {
    let roles = roles.into_iter().collect::<Vec<_>>();
    if roles.is_empty() {
        return Err("at least one role is required".into());
    }
    if outputs.is_empty() {
        return Err("at least one output role is required".into());
    }

    let mut names = HashSet::with_capacity(roles.len());
    for (name, _) in &roles {
        if name.trim().is_empty() {
            return Err("role names cannot be empty".into());
        }
        if !names.insert(*name) {
            return Err(format!("duplicate role {name}"));
        }
    }

    for (name, dependencies) in &roles {
        let mut seen = HashSet::with_capacity(dependencies.len());
        for dependency in *dependencies {
            if !names.contains(dependency.as_str()) {
                return Err(format!("role {name} depends on unknown role {dependency}"));
            }
            if !seen.insert(dependency.as_str()) {
                return Err(format!(
                    "role {name} lists dependency {dependency} more than once"
                ));
            }
        }
    }

    let mut output_names = HashSet::with_capacity(outputs.len());
    for output in outputs {
        if !names.contains(output.as_str()) {
            return Err(format!("unknown output role {output}"));
        }
        if !output_names.insert(output.as_str()) {
            return Err(format!("output role {output} is listed more than once"));
        }
    }

    let mut resolved = HashSet::with_capacity(roles.len());
    loop {
        let before = resolved.len();
        for (name, dependencies) in &roles {
            if resolved.contains(*name) {
                continue;
            }
            if dependencies
                .iter()
                .all(|dependency| resolved.contains(dependency.as_str()))
            {
                resolved.insert(*name);
            }
        }
        if resolved.len() == roles.len() {
            return Ok(());
        }
        if resolved.len() == before {
            return Err("role dependency graph contains a cycle".into());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use plurifold_core::{
        Architecture, AutoShardPolicy, CostHint, EffectSemantics, LinkProfile, LogicalRoleSpec,
        ResourceRequirements, RoleImplementation, RoleReductionSpec, ShardPartitionSpec,
        ShardPolicy, ShardPolicySpec,
    };

    use super::*;

    fn resource(id: ResourceId, performance: f64, feature: &str) -> ResourceDescriptor {
        ResourceDescriptor {
            id,
            epoch: 1,
            architecture: Architecture::X86_64,
            cpu_cores: 16,
            memory_bytes: 64 << 30,
            accelerators: vec![],
            features: BTreeSet::from([feature.to_owned()]),
            performance_score: performance,
            queue_delay_ms: 0.0,
            startup_delay_ms: 0.0,
            failure_probability: 0.0,
        }
    }

    fn implementation(
        name: &str,
        feature: &str,
        compute_ms: f64,
        output_bytes: u64,
    ) -> RoleImplementation {
        RoleImplementation {
            name: name.into(),
            task: TaskTemplate {
                artifact: "builtin:sleep".into(),
                entrypoint: "run".into(),
                arguments: vec!["1".into()],
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

    #[test]
    fn chooses_the_best_heterogeneous_implementation() {
        let cpu = ResourceId::new();
        let accelerator = ResourceId::new();
        let logical = LogicalJobSpec {
            id: Default::default(),
            roles: vec![LogicalRoleSpec {
                name: "compute".into(),
                implementations: vec![
                    implementation("cpu", "backend:cpu", 10_000.0, 1),
                    implementation("accelerator", "backend:accel", 1_000.0, 1),
                ],
                depends_on: vec![],
                shards: 1.into(),
                reduction: None,
            }],
            outputs: vec!["compute".into()],
        };

        let plan = plan(
            &logical,
            &TopologyAwareScheduler::default(),
            &[
                resource(cpu, 1.0, "backend:cpu"),
                resource(accelerator, 2.0, "backend:accel"),
            ],
            &HashMap::new(),
            &TopologySnapshot::default(),
        )
        .unwrap();

        assert_eq!(plan.roles[0].shards[0].implementation, "accelerator");
        assert_eq!(plan.roles[0].shards[0].placement.resource_id, accelerator);
    }

    #[test]
    fn downstream_placement_accounts_for_predicted_intermediate_transfer() {
        let left = ResourceId::new();
        let right = ResourceId::new();
        let logical = LogicalJobSpec {
            id: Default::default(),
            roles: vec![
                LogicalRoleSpec {
                    name: "large".into(),
                    implementations: vec![implementation(
                        "large-left",
                        "site:left",
                        100.0,
                        100 << 20,
                    )],
                    depends_on: vec![],
                    shards: 1.into(),
                    reduction: None,
                },
                LogicalRoleSpec {
                    name: "small".into(),
                    implementations: vec![implementation(
                        "small-right",
                        "site:right",
                        100.0,
                        1 << 20,
                    )],
                    depends_on: vec![],
                    shards: 1.into(),
                    reduction: None,
                },
                LogicalRoleSpec {
                    name: "join".into(),
                    implementations: vec![
                        implementation("join-left", "site:left", 100.0, 1),
                        implementation("join-right", "site:right", 100.0, 1),
                    ],
                    depends_on: vec!["large".into(), "small".into()],
                    shards: 1.into(),
                    reduction: None,
                },
            ],
            outputs: vec!["join".into()],
        };
        let topology = TopologySnapshot {
            links: vec![LinkProfile {
                from: left,
                to: right,
                rtt_ms: 80.0,
                bandwidth_mbps: 100.0,
            }],
        };

        let plan = plan(
            &logical,
            &TopologyAwareScheduler::default(),
            &[
                resource(left, 1.0, "site:left"),
                resource(right, 1.0, "site:right"),
            ],
            &HashMap::new(),
            &topology,
        )
        .unwrap();
        let join = plan.roles.iter().find(|role| role.name == "join").unwrap();

        assert_eq!(join.shards[0].implementation, "join-left");
        assert_eq!(join.shards[0].placement.resource_id, left);
        assert!(join.shards[0].placement.input_transfer_ms < 500.0);
    }

    #[test]
    fn independent_roles_are_predicted_to_run_in_parallel_when_resources_are_available() {
        let first = ResourceId::new();
        let second = ResourceId::new();
        let generic = |name: &str| LogicalRoleSpec {
            name: name.into(),
            implementations: vec![RoleImplementation {
                name: "generic".into(),
                task: TaskTemplate {
                    artifact: "builtin:sleep".into(),
                    entrypoint: "run".into(),
                    arguments: vec!["1".into()],
                    inputs: vec![],
                    requirements: ResourceRequirements::default(),
                    effects: EffectSemantics::Pure,
                    cost: CostHint {
                        compute_ms_on_reference: 1_000.0,
                        output_bytes: 1,
                    },
                },
            }],
            depends_on: vec![],
            shards: 1.into(),
            reduction: None,
        };
        let logical = LogicalJobSpec {
            id: Default::default(),
            roles: vec![generic("a"), generic("b")],
            outputs: vec!["a".into(), "b".into()],
        };

        let plan = plan(
            &logical,
            &TopologyAwareScheduler::default(),
            &[
                resource(first, 1.0, "unused:first"),
                resource(second, 1.0, "unused:second"),
            ],
            &HashMap::new(),
            &TopologySnapshot::default(),
        )
        .unwrap();

        assert_ne!(
            plan.roles[0].shards[0].placement.resource_id,
            plan.roles[1].shards[0].placement.resource_id
        );
        assert_eq!(plan.roles[0].shards[0].placement.start_ms, 0.0);
        assert_eq!(plan.roles[1].shards[0].placement.start_ms, 0.0);
        assert_eq!(plan.estimated_makespan_ms, 1_000.0);
    }

    #[test]
    fn sharded_preview_spreads_then_reuses_resources_when_needed() {
        let first = ResourceId::new();
        let second = ResourceId::new();
        let logical = LogicalJobSpec {
            id: Default::default(),
            roles: vec![LogicalRoleSpec {
                name: "map".into(),
                implementations: vec![implementation("worker", "shard:work", 1_000.0, 4)],
                depends_on: vec![],
                shards: 3.into(),
                reduction: None,
            }],
            outputs: vec!["map".into()],
        };

        let plan = plan(
            &logical,
            &TopologyAwareScheduler::default(),
            &[
                resource(first, 1.0, "shard:work"),
                resource(second, 1.0, "shard:work"),
            ],
            &HashMap::new(),
            &TopologySnapshot::default(),
        )
        .unwrap();

        let shards = &plan.roles[0].shards;
        assert_eq!(shards.len(), 3);
        assert_ne!(
            shards[0].placement.resource_id,
            shards[1].placement.resource_id
        );
        assert_eq!(shards[0].placement.start_ms, 0.0);
        assert_eq!(shards[1].placement.start_ms, 0.0);
        assert!(shards[2].placement.start_ms >= 1_000.0);
        assert_eq!(plan.estimated_makespan_ms, 2_000.0);
    }

    #[test]
    fn sharded_preview_can_mix_implementations_across_resources() {
        let cpu = ResourceId::new();
        let accelerator = ResourceId::new();
        let logical = LogicalJobSpec {
            id: Default::default(),
            roles: vec![LogicalRoleSpec {
                name: "map".into(),
                implementations: vec![
                    implementation("cpu", "backend:cpu", 1_000.0, 4),
                    implementation("accelerator", "backend:accel", 1_000.0, 4),
                ],
                depends_on: vec![],
                shards: 2.into(),
                reduction: None,
            }],
            outputs: vec!["map".into()],
        };

        let plan = plan(
            &logical,
            &TopologyAwareScheduler::default(),
            &[
                resource(cpu, 1.0, "backend:cpu"),
                resource(accelerator, 1.0, "backend:accel"),
            ],
            &HashMap::new(),
            &TopologySnapshot::default(),
        )
        .unwrap();

        let shards = &plan.roles[0].shards;
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].implementation, "cpu");
        assert_eq!(shards[0].placement.resource_id, cpu);
        assert_eq!(shards[1].implementation, "accelerator");
        assert_eq!(shards[1].placement.resource_id, accelerator);
        assert_eq!(shards[0].placement.start_ms, 0.0);
        assert_eq!(shards[1].placement.start_ms, 0.0);
    }

    #[test]
    fn auto_sharding_selects_two_workers_and_concrete_byte_ranges() {
        let first = ResourceId::new();
        let second = ResourceId::new();
        let input = ObjectId::new();
        let role = LogicalRoleSpec {
            name: "map".into(),
            implementations: vec![RoleImplementation {
                name: "worker".into(),
                task: TaskTemplate {
                    artifact: "builtin:identity".into(),
                    entrypoint: "run".into(),
                    arguments: vec![],
                    inputs: vec![input],
                    requirements: ResourceRequirements {
                        required_features: BTreeSet::from(["auto:work".into()]),
                        ..ResourceRequirements::default()
                    },
                    effects: EffectSemantics::Pure,
                    cost: CostHint {
                        compute_ms_on_reference: 2_000.0,
                        output_bytes: 1_000,
                    },
                },
            }],
            depends_on: vec![],
            shards: ShardPolicy::Policy(ShardPolicySpec::Auto(AutoShardPolicy {
                max_shards: 4,
                partition: ShardPartitionSpec::ByteRange { input: 0 },
                per_shard_overhead_ms: 0.0,
                min_gain_ratio: 0.05,
            })),
            reduction: None,
        };
        let objects = HashMap::from([(
            input,
            ObjectMetadata {
                id: input,
                size_bytes: 1_000,
                digest: None,
                encoding: None,
                locations: vec![first],
                producer: None,
            },
        )]);
        let resources = [
            resource(first, 1.0, "auto:work"),
            resource(second, 1.0, "auto:work"),
        ];
        let topology = TopologySnapshot {
            links: vec![LinkProfile {
                from: first,
                to: second,
                rtt_ms: 0.1,
                bandwidth_mbps: 10_000.0,
            }],
        };
        let context = ShardPlanningContext {
            scheduler: &TopologyAwareScheduler::default(),
            resources: &resources,
            objects: &objects,
            topology: &topology,
        };
        let shards = select_ready_shards(&role, &[], &context).unwrap();
        assert_eq!(shards.len(), 2);
        assert_ne!(shards[0].resource_id, shards[1].resource_id);
        assert_eq!(shards[0].task.cost.compute_ms_on_reference, 1_000.0);
        assert_eq!(shards[1].task.cost.compute_ms_on_reference, 1_000.0);
        assert_eq!(shards[0].task.cost.output_bytes, 500);
        assert_eq!(shards[1].task.cost.output_bytes, 500);
        assert_eq!(
            shards[0].task.shard.as_ref().unwrap().partition,
            Some(TaskShardPartition::ByteRange {
                input: 0,
                offset: 0,
                length: 500,
                total_bytes: 1_000,
            })
        );
        assert_eq!(
            shards[1].task.shard.as_ref().unwrap().partition,
            Some(TaskShardPartition::ByteRange {
                input: 0,
                offset: 500,
                length: 500,
                total_bytes: 1_000,
            })
        );
    }

    #[test]
    fn reduction_locality_does_not_merge_a_chain_with_a_distant_endpoint() {
        let a = ResourceId::new();
        let b = ResourceId::new();
        let c = ResourceId::new();
        let oa = ObjectId::new();
        let ob = ObjectId::new();
        let oc = ObjectId::new();
        let objects = HashMap::from([
            (
                oa,
                ObjectMetadata {
                    id: oa,
                    size_bytes: 8,
                    digest: None,
                    encoding: None,
                    locations: vec![a],
                    producer: None,
                },
            ),
            (
                ob,
                ObjectMetadata {
                    id: ob,
                    size_bytes: 8,
                    digest: None,
                    encoding: None,
                    locations: vec![b],
                    producer: None,
                },
            ),
            (
                oc,
                ObjectMetadata {
                    id: oc,
                    size_bytes: 8,
                    digest: None,
                    encoding: None,
                    locations: vec![c],
                    producer: None,
                },
            ),
        ]);
        let topology = TopologySnapshot {
            links: vec![
                LinkProfile {
                    from: a,
                    to: b,
                    rtt_ms: 0.1,
                    bandwidth_mbps: 10_000.0,
                },
                LinkProfile {
                    from: b,
                    to: c,
                    rtt_ms: 0.1,
                    bandwidth_mbps: 10_000.0,
                },
                LinkProfile {
                    from: a,
                    to: c,
                    rtt_ms: 100.0,
                    bandwidth_mbps: 100.0,
                },
            ],
        };
        assert_eq!(
            reduction_groups(&[oa, ob, oc], 3, 1.0, &objects, &topology),
            vec![vec![oa, ob], vec![oc]]
        );
    }

    #[test]
    fn reduction_level_groups_adjacent_local_outputs_before_cross_domain_fallback() {
        let a = ResourceId::new();
        let b = ResourceId::new();
        let c = ResourceId::new();
        let oa = ObjectId::new();
        let ob = ObjectId::new();
        let oc = ObjectId::new();
        let objects = HashMap::from([
            (
                oa,
                ObjectMetadata {
                    id: oa,
                    size_bytes: 8,
                    digest: None,
                    encoding: None,
                    locations: vec![a],
                    producer: None,
                },
            ),
            (
                ob,
                ObjectMetadata {
                    id: ob,
                    size_bytes: 8,
                    digest: None,
                    encoding: None,
                    locations: vec![b],
                    producer: None,
                },
            ),
            (
                oc,
                ObjectMetadata {
                    id: oc,
                    size_bytes: 8,
                    digest: None,
                    encoding: None,
                    locations: vec![c],
                    producer: None,
                },
            ),
        ]);
        let topology = TopologySnapshot {
            links: vec![
                LinkProfile {
                    from: a,
                    to: b,
                    rtt_ms: 0.1,
                    bandwidth_mbps: 10_000.0,
                },
                LinkProfile {
                    from: a,
                    to: c,
                    rtt_ms: 100.0,
                    bandwidth_mbps: 100.0,
                },
                LinkProfile {
                    from: b,
                    to: c,
                    rtt_ms: 100.0,
                    bandwidth_mbps: 100.0,
                },
            ],
        };
        let resources = [
            resource(a, 1.0, "reduce"),
            resource(b, 1.0, "reduce"),
            resource(c, 1.0, "reduce"),
        ];
        let reduction = RoleReductionSpec {
            name: "sum".into(),
            task: TaskTemplate {
                artifact: "builtin:sum-u64".into(),
                entrypoint: "run".into(),
                arguments: vec![],
                inputs: vec![],
                requirements: ResourceRequirements {
                    required_features: BTreeSet::from(["reduce".into()]),
                    ..ResourceRequirements::default()
                },
                effects: EffectSemantics::Pure,
                cost: CostHint {
                    compute_ms_on_reference: 10.0,
                    output_bytes: 8,
                },
            },
            max_fan_in: 2,
            locality_rtt_ms: 1.0,
        };
        let context = ShardPlanningContext {
            scheduler: &TopologyAwareScheduler::default(),
            resources: &resources,
            objects: &objects,
            topology: &topology,
        };
        let mut availability = HashMap::new();
        let level =
            select_reduction_level(&reduction, &[oa, ob, oc], 0.0, &mut availability, &context)
                .unwrap();
        assert_eq!(level.len(), 2);
        match &level[0] {
            ReadyReductionItem::Reduce(selected) => {
                assert_eq!(selected.input_count, 2);
                assert_eq!(selected.task.inputs, vec![oa, ob]);
                assert!(selected.resource_id == a || selected.resource_id == b);
            }
            ReadyReductionItem::Carry(_) => panic!("local A/B pair should reduce"),
        }
        assert!(matches!(level[1], ReadyReductionItem::Carry(object) if object == oc));
    }

    #[test]
    fn record_partition_object_size_mismatch_is_rejected() {
        let input = ObjectId::new();
        let spec = LogicalJobSpec {
            id: plurifold_core::JobId::new(),
            roles: vec![LogicalRoleSpec {
                name: "records".into(),
                implementations: vec![RoleImplementation {
                    name: "worker".into(),
                    task: TaskTemplate {
                        artifact: "builtin:identity".into(),
                        entrypoint: "run".into(),
                        arguments: vec![],
                        inputs: vec![input],
                        requirements: ResourceRequirements::default(),
                        effects: EffectSemantics::Pure,
                        cost: CostHint::default(),
                    },
                }],
                depends_on: vec![],
                shards: ShardPolicy::Policy(ShardPolicySpec::Auto(AutoShardPolicy {
                    max_shards: 2,
                    partition: ShardPartitionSpec::Records {
                        input: 0,
                        offsets: vec![0, 4, 9],
                    },
                    per_shard_overhead_ms: 0.0,
                    min_gain_ratio: 0.05,
                })),
                reduction: None,
            }],
            outputs: vec!["records".into()],
        };
        let objects = HashMap::from([(
            input,
            ObjectMetadata {
                id: input,
                size_bytes: 10,
                digest: Some("sha256:record-layout".into()),
                encoding: None,
                locations: vec![],
                producer: None,
            },
        )]);
        assert!(matches!(
            validate_logical_job_objects(&spec, &objects),
            Err(PlanError::InvalidLogicalJob(message))
                if message.contains("record offsets must end at object size 10")
        ));
    }

    #[test]
    fn record_partitioning_preserves_boundaries_and_scales_work_by_record_count() {
        let first = ResourceId::new();
        let second = ResourceId::new();
        let input = ObjectId::new();
        let role = LogicalRoleSpec {
            name: "records".into(),
            implementations: vec![RoleImplementation {
                name: "worker".into(),
                task: TaskTemplate {
                    artifact: "builtin:identity".into(),
                    entrypoint: "run".into(),
                    arguments: vec![],
                    inputs: vec![input],
                    requirements: ResourceRequirements {
                        required_features: BTreeSet::from(["records:work".into()]),
                        ..ResourceRequirements::default()
                    },
                    effects: EffectSemantics::Pure,
                    cost: CostHint {
                        compute_ms_on_reference: 400.0,
                        output_bytes: 100,
                    },
                },
            }],
            depends_on: vec![],
            shards: ShardPolicy::Policy(ShardPolicySpec::Auto(AutoShardPolicy {
                max_shards: 2,
                partition: ShardPartitionSpec::Records {
                    input: 0,
                    offsets: vec![0, 2, 8, 9, 12],
                },
                per_shard_overhead_ms: 0.0,
                min_gain_ratio: 0.0,
            })),
            reduction: None,
        };
        let objects = HashMap::from([(
            input,
            ObjectMetadata {
                id: input,
                size_bytes: 12,
                digest: None,
                encoding: None,
                locations: vec![first, second],
                producer: None,
            },
        )]);
        let resources = [
            resource(first, 1.0, "records:work"),
            resource(second, 1.0, "records:work"),
        ];
        let context = ShardPlanningContext {
            scheduler: &TopologyAwareScheduler::default(),
            resources: &resources,
            objects: &objects,
            topology: &TopologySnapshot::default(),
        };
        let shards = select_ready_shards(&role, &[], &context).unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].task.cost.compute_ms_on_reference, 200.0);
        assert_eq!(shards[1].task.cost.compute_ms_on_reference, 200.0);
        assert_eq!(shards[0].task.cost.output_bytes, 50);
        assert_eq!(shards[1].task.cost.output_bytes, 50);
        assert_eq!(
            shards[0].task.shard.as_ref().unwrap().partition,
            Some(TaskShardPartition::Records {
                input: 0,
                record_start: 0,
                record_count: 2,
                total_records: 4,
                offset: 0,
                length: 8,
                total_bytes: 12,
            })
        );
        assert_eq!(
            shards[1].task.shard.as_ref().unwrap().partition,
            Some(TaskShardPartition::Records {
                input: 0,
                record_start: 2,
                record_count: 2,
                total_records: 4,
                offset: 8,
                length: 4,
                total_bytes: 12,
            })
        );
    }

    #[test]
    fn auto_sharding_stays_single_when_remote_topology_is_too_expensive() {
        let local = ResourceId::new();
        let remote = ResourceId::new();
        let input = ObjectId::new();
        let role = LogicalRoleSpec {
            name: "map".into(),
            implementations: vec![RoleImplementation {
                name: "worker".into(),
                task: TaskTemplate {
                    artifact: "builtin:identity".into(),
                    entrypoint: "run".into(),
                    arguments: vec![],
                    inputs: vec![input],
                    requirements: ResourceRequirements {
                        required_features: BTreeSet::from(["auto:work".into()]),
                        ..ResourceRequirements::default()
                    },
                    effects: EffectSemantics::Pure,
                    cost: CostHint {
                        compute_ms_on_reference: 100.0,
                        output_bytes: 1_000,
                    },
                },
            }],
            depends_on: vec![],
            shards: ShardPolicy::Policy(ShardPolicySpec::Auto(AutoShardPolicy {
                max_shards: 4,
                partition: ShardPartitionSpec::ByteRange { input: 0 },
                per_shard_overhead_ms: 0.0,
                min_gain_ratio: 0.05,
            })),
            reduction: None,
        };
        let objects = HashMap::from([(
            input,
            ObjectMetadata {
                id: input,
                size_bytes: 1_000,
                digest: None,
                encoding: None,
                locations: vec![local],
                producer: None,
            },
        )]);
        let resources = [
            resource(local, 1.0, "auto:work"),
            resource(remote, 1.0, "auto:work"),
        ];
        let topology = TopologySnapshot {
            links: vec![LinkProfile {
                from: local,
                to: remote,
                rtt_ms: 500.0,
                bandwidth_mbps: 1.0,
            }],
        };
        let context = ShardPlanningContext {
            scheduler: &TopologyAwareScheduler::default(),
            resources: &resources,
            objects: &objects,
            topology: &topology,
        };
        let shards = select_ready_shards(&role, &[], &context).unwrap();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].resource_id, local);
    }
}
