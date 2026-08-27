use std::collections::HashMap;

use mosaic_core::{
    LinkProfile, ObjectId, ObjectMetadata, ResourceDescriptor, ResourceId, TaskSpec,
    TopologySnapshot,
};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct PlacementBreakdown {
    pub compute_ms: f64,
    pub input_transfer_ms: f64,
    pub queue_ms: f64,
    pub startup_ms: f64,
    pub risk_penalty_ms: f64,
    pub total_ms: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlacementDecision {
    pub resource_id: ResourceId,
    pub cost: PlacementBreakdown,
}

#[derive(Debug, Error, PartialEq)]
pub enum ScheduleError {
    #[error("task references unknown object {0}")]
    MissingObject(ObjectId),
    #[error("no compatible resource has reachable inputs")]
    NoCandidate,
}

#[derive(Clone, Debug)]
pub struct TopologyAwareScheduler {
    /// Multiplier applied to expected wasted compute from resource failure.
    pub risk_multiplier: f64,
}

impl Default for TopologyAwareScheduler {
    fn default() -> Self {
        Self {
            risk_multiplier: 1.0,
        }
    }
}

impl TopologyAwareScheduler {
    pub fn choose(
        &self,
        task: &TaskSpec,
        resources: &[ResourceDescriptor],
        objects: &HashMap<ObjectId, ObjectMetadata>,
        topology: &TopologySnapshot,
    ) -> Result<PlacementDecision, ScheduleError> {
        for input in &task.inputs {
            if !objects.contains_key(input) {
                return Err(ScheduleError::MissingObject(*input));
            }
        }

        resources
            .iter()
            .filter(|resource| resource.supports(&task.requirements))
            .filter_map(|resource| {
                self.score_candidate(task, resource, objects, topology)
                    .map(|cost| PlacementDecision {
                        resource_id: resource.id,
                        cost,
                    })
            })
            .min_by(|a, b| a.cost.total_ms.total_cmp(&b.cost.total_ms))
            .ok_or(ScheduleError::NoCandidate)
    }

    fn score_candidate(
        &self,
        task: &TaskSpec,
        resource: &ResourceDescriptor,
        objects: &HashMap<ObjectId, ObjectMetadata>,
        topology: &TopologySnapshot,
    ) -> Option<PlacementBreakdown> {
        let compute_ms = task.cost.compute_ms_on_reference / resource.performance_score;
        let mut input_transfer_ms = 0.0;

        for input_id in &task.inputs {
            let object = &objects[input_id];
            if object.is_local_to(resource.id) {
                continue;
            }

            let best_transfer = object
                .locations
                .iter()
                .filter_map(|source| {
                    topology.transfer_time_ms(*source, resource.id, object.size_bytes)
                })
                .min_by(f64::total_cmp)?;
            input_transfer_ms += best_transfer;
        }

        let failure_probability = resource.failure_probability.clamp(0.0, 1.0);
        let risk_penalty_ms = compute_ms * failure_probability * self.risk_multiplier;
        let total_ms = compute_ms
            + input_transfer_ms
            + resource.queue_delay_ms.max(0.0)
            + resource.startup_delay_ms.max(0.0)
            + risk_penalty_ms;

        Some(PlacementBreakdown {
            compute_ms,
            input_transfer_ms,
            queue_ms: resource.queue_delay_ms.max(0.0),
            startup_ms: resource.startup_delay_ms.max(0.0),
            risk_penalty_ms,
            total_ms,
        })
    }
}

#[derive(Clone, Debug)]
pub struct FusionAdvisor {
    /// Minimum estimated network time saved before fusion is worth proposing.
    pub minimum_saved_transfer_ms: f64,
}

impl Default for FusionAdvisor {
    fn default() -> Self {
        Self {
            minimum_saved_transfer_ms: 20.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FusionRecommendation {
    pub should_fuse: bool,
    pub estimated_saved_transfer_ms: f64,
}

impl FusionAdvisor {
    pub fn evaluate(
        &self,
        intermediate_bytes: u64,
        cross_domain_link: &LinkProfile,
    ) -> FusionRecommendation {
        let estimated_saved_transfer_ms = cross_domain_link
            .transfer_time_ms(intermediate_bytes)
            .unwrap_or(f64::INFINITY);
        FusionRecommendation {
            should_fuse: estimated_saved_transfer_ms >= self.minimum_saved_transfer_ms,
            estimated_saved_transfer_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use mosaic_core::{
        Architecture, CostHint, EffectSemantics, LinkProfile, ObjectId, ObjectMetadata,
        ResourceDescriptor, ResourceId, ResourceRequirements, TaskId, TaskSpec, TopologySnapshot,
    };

    use super::*;

    fn resource(id: ResourceId, performance: f64) -> ResourceDescriptor {
        ResourceDescriptor {
            id,
            epoch: 1,
            architecture: Architecture::X86_64,
            cpu_cores: 16,
            memory_bytes: 64 << 30,
            accelerators: vec![],
            features: BTreeSet::new(),
            performance_score: performance,
            queue_delay_ms: 0.0,
            startup_delay_ms: 0.0,
            failure_probability: 0.0,
        }
    }

    fn task(input: ObjectId, compute_ms: f64) -> TaskSpec {
        TaskSpec {
            id: TaskId::new(),
            artifact: "demo".into(),
            entrypoint: "run".into(),
            inputs: vec![input],
            requirements: ResourceRequirements::default(),
            effects: EffectSemantics::Pure,
            cost: CostHint {
                compute_ms_on_reference: compute_ms,
                output_bytes: 0,
            },
        }
    }

    #[test]
    fn keeps_data_heavy_work_local_even_when_remote_compute_is_faster() {
        let local = ResourceId::new();
        let remote = ResourceId::new();
        let object_id = ObjectId::new();
        let objects = HashMap::from([(
            object_id,
            ObjectMetadata {
                id: object_id,
                size_bytes: 10 << 30,
                digest: None,
                encoding: None,
                locations: vec![local],
                producer: None,
            },
        )]);
        let topology = TopologySnapshot {
            links: vec![LinkProfile {
                from: local,
                to: remote,
                rtt_ms: 100.0,
                bandwidth_mbps: 100.0,
            }],
        };
        let decision = TopologyAwareScheduler::default()
            .choose(
                &task(object_id, 5_000.0),
                &[resource(local, 1.0), resource(remote, 10.0)],
                &objects,
                &topology,
            )
            .unwrap();
        assert_eq!(decision.resource_id, local);
    }

    #[test]
    fn sends_compute_heavy_small_input_work_to_faster_remote_resource() {
        let local = ResourceId::new();
        let remote = ResourceId::new();
        let object_id = ObjectId::new();
        let objects = HashMap::from([(
            object_id,
            ObjectMetadata {
                id: object_id,
                size_bytes: 1 << 20,
                digest: None,
                encoding: None,
                locations: vec![local],
                producer: None,
            },
        )]);
        let topology = TopologySnapshot {
            links: vec![LinkProfile {
                from: local,
                to: remote,
                rtt_ms: 100.0,
                bandwidth_mbps: 1_000.0,
            }],
        };
        let decision = TopologyAwareScheduler::default()
            .choose(
                &task(object_id, 60_000.0),
                &[resource(local, 1.0), resource(remote, 10.0)],
                &objects,
                &topology,
            )
            .unwrap();
        assert_eq!(decision.resource_id, remote);
    }
}
