use std::collections::BTreeSet;

use clap::{Parser, Subcommand};
use mosaic_core::{Architecture, ResourceDescriptor, ResourceId};

#[derive(Parser)]
#[command(
    name = "mosaic-agent",
    about = "Mosaic Fabric resource agent prototype"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Emit the capability descriptor that a future agent will register with a coordinator.
    Describe {
        #[arg(long, default_value = "local")]
        id: String,
        #[arg(long, default_value_t = 8)]
        cpu_cores: u32,
        #[arg(long, default_value_t = 16)]
        memory_gib: u64,
        #[arg(long, default_value_t = 1.0)]
        performance: f64,
        #[arg(long = "feature")]
        features: Vec<String>,
    },
}

fn main() {
    let args = Args::parse();
    match args.command {
        Command::Describe {
            id,
            cpu_cores,
            memory_gib,
            performance,
            features,
        } => {
            let descriptor = ResourceDescriptor {
                // v0 prints a generated logical ID; the human-readable id is carried as a feature tag.
                id: ResourceId::new(),
                epoch: 0,
                architecture: current_architecture(),
                cpu_cores,
                memory_bytes: memory_gib << 30,
                accelerators: vec![],
                features: features
                    .into_iter()
                    .chain([format!("agent-name:{id}")])
                    .collect::<BTreeSet<_>>(),
                performance_score: performance,
                queue_delay_ms: 0.0,
                startup_delay_ms: 0.0,
                failure_probability: 0.01,
            };
            println!("{}", serde_json::to_string_pretty(&descriptor).unwrap());
        }
    }
}

fn current_architecture() -> Architecture {
    match std::env::consts::ARCH {
        "x86_64" => Architecture::X86_64,
        "aarch64" => Architecture::Aarch64,
        "riscv64" => Architecture::RiscV64,
        other => Architecture::Other(other.to_owned()),
    }
}
