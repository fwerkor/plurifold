use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use plurifold_core::{
    AcceleratorRequirement, Architecture, CooperativeJobSpec, CooperativeRoleSpec, EffectSemantics,
    LogicalJobSpec, ObjectId, ObjectMetadata, PipelineInput, ResourceDescriptor, ResourceId,
    ResourceRequirements, TaskId, TaskPipeline, TaskPipelineStage, TaskSpec, TaskTemplate,
    TopologySnapshot,
};
use plurifold_scheduler::{
    FusionAdvisor, PlacementBreakdown, ScheduleError, TopologyAwareScheduler,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
pub struct PlannedRole {
    pub name: String,
    pub implementation: String,
    pub placement: PlannedPlacement,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CooperativePlan {
    pub job: CooperativeJobSpec,
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
    task: TaskTemplate,
    output: ObjectId,
    finish_ms: f64,
}

struct Candidate<'a> {
    implementation_name: &'a str,
    task: &'a TaskTemplate,
    resource_id: ResourceId,
    start_ms: f64,
    cost: PlacementBreakdown,
}

pub(crate) struct ReadyRoleSelection {
    pub implementation: String,
    pub task: plurifold_core::TaskSpec,
    pub resource_id: ResourceId,
    pub cost: PlacementBreakdown,
}

pub(crate) struct ReadyFusionSelection {
    pub producer_implementation: String,
    pub consumer_implementation: String,
    pub task: TaskSpec,
    pub resource_id: ResourceId,
    pub cost: PlacementBreakdown,
    pub estimated_avoided_transfer_ms: f64,
    pub estimated_vs_separate_ms: f64,
}

#[derive(Clone, Copy)]
struct SeparatePath {
    producer_resource: ResourceId,
    consumer_resource: ResourceId,
    intermediate_bytes: u64,
    total_ms: f64,
}

struct SeparatePaths {
    best: SeparatePath,
    best_cross_resource: Option<SeparatePath>,
}

pub(crate) struct FusionContext<'a> {
    pub scheduler: &'a TopologyAwareScheduler,
    pub advisor: &'a FusionAdvisor,
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

pub(crate) fn select_ready_fusion(
    producer: &plurifold_core::LogicalRoleSpec,
    producer_dependency_inputs: &[ObjectId],
    consumer: &plurifold_core::LogicalRoleSpec,
    context: &FusionContext<'_>,
) -> Option<ReadyFusionSelection> {
    if !producer
        .implementations
        .iter()
        .all(|implementation| fusable_template(&implementation.task))
        || !consumer
            .implementations
            .iter()
            .all(|implementation| fusable_template(&implementation.task))
    {
        return None;
    }
    let separate = separate_paths(producer, producer_dependency_inputs, consumer, context)?;
    let cross_resource = separate.best_cross_resource.as_ref()?;
    let link = context.topology.link(
        cross_resource.producer_resource,
        cross_resource.consumer_resource,
    )?;
    let recommendation = context
        .advisor
        .evaluate(cross_resource.intermediate_bytes, link);
    if !recommendation.should_fuse {
        return None;
    }

    let mut best: Option<ReadyFusionSelection> = None;
    for producer_impl in &producer.implementations {
        let producer_task = producer_impl
            .task
            .instantiate(producer_dependency_inputs.iter().copied());
        for consumer_impl in &consumer.implementations {
            let Some(requirements) = combine_requirements(
                &producer_impl.task.requirements,
                &consumer_impl.task.requirements,
            ) else {
                continue;
            };
            let task = fused_pipeline_task(&producer_task, &consumer_impl.task, requirements);
            let Ok(decision) = context.scheduler.choose(
                &task,
                context.resources,
                context.objects,
                context.topology,
            ) else {
                continue;
            };
            let estimated_vs_separate_ms = separate.best.total_ms - decision.cost.total_ms;
            if estimated_vs_separate_ms < 0.0 {
                continue;
            }
            let replace = best
                .as_ref()
                .map(|current| decision.cost.total_ms < current.cost.total_ms)
                .unwrap_or(true);
            if replace {
                best = Some(ReadyFusionSelection {
                    producer_implementation: producer_impl.name.clone(),
                    consumer_implementation: consumer_impl.name.clone(),
                    task,
                    resource_id: decision.resource_id,
                    cost: decision.cost,
                    estimated_avoided_transfer_ms: recommendation.estimated_saved_transfer_ms,
                    estimated_vs_separate_ms,
                });
            }
        }
    }
    best
}

fn separate_paths(
    producer: &plurifold_core::LogicalRoleSpec,
    producer_dependency_inputs: &[ObjectId],
    consumer: &plurifold_core::LogicalRoleSpec,
    context: &FusionContext<'_>,
) -> Option<SeparatePaths> {
    let mut best: Option<SeparatePath> = None;
    let mut best_cross_resource: Option<SeparatePath> = None;
    for producer_impl in &producer.implementations {
        let producer_task = producer_impl
            .task
            .instantiate(producer_dependency_inputs.iter().copied());
        for producer_resource in context.resources {
            let Some(producer_cost) = placement_cost(
                context.scheduler,
                &producer_task,
                producer_resource,
                context.objects,
                context.topology,
            ) else {
                continue;
            };
            let intermediate = ObjectId::new();
            let mut predicted_objects = context.objects.clone();
            predicted_objects.insert(
                intermediate,
                ObjectMetadata {
                    id: intermediate,
                    size_bytes: producer_impl.task.cost.output_bytes,
                    digest: None,
                    encoding: None,
                    locations: vec![producer_resource.id],
                    producer: None,
                },
            );
            for consumer_impl in &consumer.implementations {
                let consumer_task = consumer_impl.task.instantiate([intermediate]);
                for consumer_resource in context.resources {
                    let Some(consumer_cost) = placement_cost(
                        context.scheduler,
                        &consumer_task,
                        consumer_resource,
                        &predicted_objects,
                        context.topology,
                    ) else {
                        continue;
                    };
                    let total_ms = producer_cost.total_ms + consumer_cost.total_ms;
                    let candidate = SeparatePath {
                        producer_resource: producer_resource.id,
                        consumer_resource: consumer_resource.id,
                        intermediate_bytes: producer_impl.task.cost.output_bytes,
                        total_ms,
                    };
                    let replace = best
                        .as_ref()
                        .map(|current| total_ms < current.total_ms)
                        .unwrap_or(true);
                    if replace {
                        best = Some(candidate);
                    }
                    if candidate.producer_resource != candidate.consumer_resource {
                        let replace_cross = best_cross_resource
                            .as_ref()
                            .map(|current| candidate.total_ms < current.total_ms)
                            .unwrap_or(true);
                        if replace_cross {
                            best_cross_resource = Some(candidate);
                        }
                    }
                }
            }
        }
    }
    best.map(|best| SeparatePaths {
        best,
        best_cross_resource,
    })
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
    producer: &TaskSpec,
    consumer: &TaskTemplate,
    requirements: ResourceRequirements,
) -> TaskSpec {
    let mut external_inputs = Vec::<ObjectId>::new();
    let mut external_indexes = HashMap::<ObjectId, usize>::new();
    let producer_inputs = pipeline_external_bindings(
        &producer.inputs,
        &mut external_inputs,
        &mut external_indexes,
    );
    let mut consumer_inputs = pipeline_external_bindings(
        &consumer.inputs,
        &mut external_inputs,
        &mut external_indexes,
    );
    consumer_inputs.push(PipelineInput::PreviousOutput);

    TaskSpec {
        id: TaskId::new(),
        artifact: "plurifold:pipeline".into(),
        entrypoint: "run".into(),
        arguments: vec![],
        inputs: external_inputs,
        requirements,
        effects: EffectSemantics::Pure,
        cost: plurifold_core::CostHint {
            compute_ms_on_reference: producer.cost.compute_ms_on_reference
                + consumer.cost.compute_ms_on_reference,
            output_bytes: consumer.cost.output_bytes,
        },
        pipeline: Some(TaskPipeline {
            stages: vec![
                TaskPipelineStage {
                    artifact: producer.artifact.clone(),
                    entrypoint: producer.entrypoint.clone(),
                    arguments: producer.arguments.clone(),
                    inputs: producer_inputs,
                },
                TaskPipelineStage {
                    artifact: consumer.artifact.clone(),
                    entrypoint: consumer.entrypoint.clone(),
                    arguments: consumer.arguments.clone(),
                    inputs: consumer_inputs,
                },
            ],
        }),
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

pub(crate) fn plan(
    spec: &LogicalJobSpec,
    scheduler: &TopologyAwareScheduler,
    resources: &[ResourceDescriptor],
    objects: &HashMap<ObjectId, ObjectMetadata>,
    topology: &TopologySnapshot,
) -> Result<CooperativePlan, PlanError> {
    validate_logical_job(spec)?;

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
            .map(|dependency| selected[dependency].output)
            .collect::<Vec<_>>();
        let dependency_ready_ms = role
            .depends_on
            .iter()
            .map(|dependency| selected[dependency].finish_ms)
            .fold(0.0_f64, f64::max);

        let mut best: Option<Candidate<'_>> = None;
        for implementation in &role.implementations {
            let task = implementation
                .task
                .instantiate(dependency_inputs.iter().copied());
            for resource in resources {
                let Some(cost) =
                    placement_cost(scheduler, &task, resource, &predicted_objects, topology)
                else {
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
                    best = Some(Candidate {
                        implementation_name: &implementation.name,
                        task: &implementation.task,
                        resource_id: resource.id,
                        start_ms,
                        cost,
                    });
                }
            }
        }

        let Some(best) = best else {
            return Err(PlanError::NoFeasibleImplementation(role.name.clone()));
        };
        let finish_ms = best.start_ms + best.cost.total_ms;
        resource_available_ms.insert(best.resource_id, finish_ms);

        let output = ObjectId::new();
        predicted_objects.insert(
            output,
            ObjectMetadata {
                id: output,
                size_bytes: best.task.cost.output_bytes,
                digest: None,
                encoding: None,
                locations: vec![best.resource_id],
                producer: None,
            },
        );
        selected.insert(
            role.name.clone(),
            SelectedRole {
                task: best.task.clone(),
                output,
                finish_ms,
            },
        );
        planned_roles.insert(
            role.name.clone(),
            PlannedRole {
                name: role.name.clone(),
                implementation: best.implementation_name.to_owned(),
                placement: PlannedPlacement {
                    resource_id: best.resource_id,
                    start_ms: best.start_ms,
                    finish_ms,
                    compute_ms: best.cost.compute_ms,
                    input_transfer_ms: best.cost.input_transfer_ms,
                    queue_ms: best.cost.queue_ms,
                    startup_ms: best.cost.startup_ms,
                    risk_penalty_ms: best.cost.risk_penalty_ms,
                    total_ms: best.cost.total_ms,
                },
            },
        );
    }

    let roles = spec
        .roles
        .iter()
        .map(|role| CooperativeRoleSpec {
            name: role.name.clone(),
            task: selected[&role.name].task.clone(),
            depends_on: role.depends_on.clone(),
        })
        .collect();
    let estimated_makespan_ms = spec
        .outputs
        .iter()
        .map(|role| selected[role].finish_ms)
        .fold(0.0_f64, f64::max);

    Ok(CooperativePlan {
        job: CooperativeJobSpec {
            id: spec.id,
            roles,
            outputs: spec.outputs.clone(),
        },
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
        .filter_map(|dependency| selected.get(dependency).map(|role| role.output))
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
        if role.implementations.is_empty() {
            return Err(PlanError::InvalidLogicalJob(format!(
                "role {} has no implementations",
                role.name
            )));
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
        Architecture, CostHint, EffectSemantics, LinkProfile, LogicalRoleSpec,
        ResourceRequirements, RoleImplementation,
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

        assert_eq!(plan.roles[0].implementation, "accelerator");
        assert_eq!(plan.roles[0].placement.resource_id, accelerator);
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
                },
                LogicalRoleSpec {
                    name: "join".into(),
                    implementations: vec![
                        implementation("join-left", "site:left", 100.0, 1),
                        implementation("join-right", "site:right", 100.0, 1),
                    ],
                    depends_on: vec!["large".into(), "small".into()],
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

        assert_eq!(join.implementation, "join-left");
        assert_eq!(join.placement.resource_id, left);
        assert!(join.placement.input_transfer_ms < 500.0);
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
            plan.roles[0].placement.resource_id,
            plan.roles[1].placement.resource_id
        );
        assert_eq!(plan.roles[0].placement.start_ms, 0.0);
        assert_eq!(plan.roles[1].placement.start_ms, 0.0);
        assert_eq!(plan.estimated_makespan_ms, 1_000.0);
    }
}
