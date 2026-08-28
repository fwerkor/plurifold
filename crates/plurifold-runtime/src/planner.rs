use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use plurifold_core::{
    CooperativeJobSpec, CooperativeRoleSpec, LogicalJobSpec, ObjectId, ObjectMetadata,
    ResourceDescriptor, ResourceId, TaskTemplate, TopologySnapshot,
};
use plurifold_scheduler::{PlacementBreakdown, ScheduleError, TopologyAwareScheduler};
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
