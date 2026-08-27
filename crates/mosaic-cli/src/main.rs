use std::collections::BTreeSet;

use clap::{Parser, Subcommand};
use mosaic_core::{
    Architecture, CostHint, EffectSemantics, LinkProfile, ObjectId, ObjectMetadata,
    ResourceDescriptor, ResourceId, ResourceRequirements, TaskId, TaskSpec, TopologySnapshot,
};
use mosaic_runtime::Fabric;

#[derive(Parser)]
#[command(name = "mosaic", about = "Mosaic Fabric control-plane prototype")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Demonstrate that scheduling considers data movement, not only raw compute speed.
    Demo,
}

fn main() {
    let args = Args::parse();
    match args.command {
        Command::Demo => demo(),
    }
}

fn demo() {
    let mut fabric = Fabric::default();
    let local = ResourceId::new();
    let remote = ResourceId::new();
    fabric.register_resource(cpu_resource(local, 1.0));
    fabric.register_resource(cpu_resource(remote, 10.0));
    fabric.set_topology(TopologySnapshot {
        links: vec![LinkProfile {
            from: local,
            to: remote,
            rtt_ms: 100.0,
            bandwidth_mbps: 100.0,
        }],
    });

    let data = ObjectId::new();
    fabric.publish_object(ObjectMetadata {
        id: data,
        size_bytes: 10 << 30,
        digest: None,
        encoding: None,
        locations: vec![local],
        producer: None,
    });
    let task = TaskSpec {
        id: TaskId::new(),
        artifact: "demo://analysis".into(),
        entrypoint: "run".into(),
        inputs: vec![data],
        requirements: ResourceRequirements::default(),
        effects: EffectSemantics::Pure,
        cost: CostHint {
            compute_ms_on_reference: 5_000.0,
            output_bytes: 1 << 20,
        },
    };
    let task_id = fabric.submit(task).unwrap();
    let decision = fabric.schedule_task(task_id).unwrap();

    println!("local resource:  {local}");
    println!("remote resource: {remote} (10x compute, 100 ms / 100 Mbps link)");
    println!("selected:        {}", decision.resource_id);
    println!("estimated total: {:.1} ms", decision.cost.total_ms);
    println!("input transfer:  {:.1} ms", decision.cost.input_transfer_ms);
}

fn cpu_resource(id: ResourceId, performance_score: f64) -> ResourceDescriptor {
    ResourceDescriptor {
        id,
        epoch: 0,
        architecture: Architecture::X86_64,
        cpu_cores: 16,
        memory_bytes: 64 << 30,
        accelerators: vec![],
        features: BTreeSet::new(),
        performance_score,
        queue_delay_ms: 0.0,
        startup_delay_ms: 0.0,
        failure_probability: 0.0,
    }
}
