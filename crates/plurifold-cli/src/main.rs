use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use plurifold_core::{
    Architecture, CooperativeJobSpec, CostHint, EffectSemantics, JobId, LinkProfile,
    LogicalJobSpec, ObjectId, ObjectMetadata, ResourceDescriptor, ResourceId, ResourceRequirements,
    TaskId, TaskSpec, TopologySnapshot,
};
use plurifold_protocol::{
    CooperativeJobView, ErrorResponse, LinkUpdateRequest, PlanLogicalJobRequest,
    PublishObjectRequest, PutObjectResponse, ResourceListResponse, SubmitCooperativeJobRequest,
    SubmitCooperativeJobResponse, SubmitLogicalJobResponse, SubmitTaskRequest, SubmitTaskResponse,
    TaskView,
};
use plurifold_runtime::{CooperativeJobStatus, CooperativePlan, Fabric, TaskStatus};
use reqwest::Client;

#[derive(Parser)]
#[command(name = "plurifold", about = "Plurifold control-plane CLI")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Demonstrate topology-aware local scheduling without starting network services.
    Demo,
    /// Put bytes into an agent CAS and publish the resulting object to the coordinator.
    Put {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        agent: String,
        #[arg(long)]
        file: PathBuf,
    },
    /// Submit a task. Phase 1 agents currently implement builtin:* artifacts.
    Submit {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        artifact: String,
        #[arg(long, default_value = "run")]
        entrypoint: String,
        #[arg(long = "argument")]
        arguments: Vec<String>,
        #[arg(long = "input")]
        inputs: Vec<ObjectId>,
        #[arg(long = "require-feature")]
        required_features: Vec<String>,
        #[arg(long, default_value_t = 1_000.0)]
        compute_ms: f64,
        #[arg(long, default_value_t = 0)]
        output_bytes: u64,
    },
    /// Query one task.
    Status {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        task: TaskId,
    },
    /// Wait until a task completes or becomes uncertain.
    Wait {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        task: TaskId,
        #[arg(long, default_value_t = 30)]
        timeout_s: u64,
    },
    /// Submit, inspect, and wait for multi-role cooperative jobs.
    Job {
        #[command(subcommand)]
        command: JobCommand,
    },
    /// List active resources and their advertised data endpoints.
    Resources {
        #[arg(long)]
        coordinator: String,
    },
    /// Set or update the measured link between two resources.
    Link {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        from: ResourceId,
        #[arg(long)]
        to: ResourceId,
        #[arg(long)]
        rtt_ms: f64,
        #[arg(long)]
        bandwidth_mbps: f64,
    },
}

#[derive(Subcommand)]
enum JobCommand {
    /// Submit a cooperative job definition from JSON.
    Submit {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        file: PathBuf,
    },
    /// Preview implementation choices and predicted placements for a logical job.
    Plan {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        file: PathBuf,
    },
    /// Submit a logical job whose ready roles are dynamically replanned against current state.
    AutoSubmit {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        file: PathBuf,
    },
    /// Query a cooperative job and all role states.
    Status {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        job: JobId,
    },
    /// Wait until a cooperative job completes or becomes uncertain.
    Wait {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        job: JobId,
        #[arg(long, default_value_t = 30)]
        timeout_s: u64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Args::parse().command {
        Command::Demo => demo(),
        Command::Put {
            coordinator,
            agent,
            file,
        } => put(&Client::new(), &coordinator, &agent, &file).await?,
        Command::Submit {
            coordinator,
            artifact,
            entrypoint,
            arguments,
            inputs,
            required_features,
            compute_ms,
            output_bytes,
        } => {
            submit(
                &Client::new(),
                &coordinator,
                TaskSpec {
                    id: TaskId::new(),
                    artifact,
                    entrypoint,
                    arguments,
                    inputs,
                    requirements: ResourceRequirements {
                        required_features: required_features.into_iter().collect(),
                        ..ResourceRequirements::default()
                    },
                    effects: EffectSemantics::Pure,
                    cost: CostHint {
                        compute_ms_on_reference: compute_ms,
                        output_bytes,
                    },
                },
            )
            .await?;
        }
        Command::Status { coordinator, task } => {
            let view = task_view(&Client::new(), &coordinator, task).await?;
            println!("{}", serde_json::to_string_pretty(&view)?);
        }
        Command::Wait {
            coordinator,
            task,
            timeout_s,
        } => wait(&Client::new(), &coordinator, task, timeout_s).await?,
        Command::Job { command } => match command {
            JobCommand::Submit { coordinator, file } => {
                let bytes = tokio::fs::read(file).await?;
                let job: CooperativeJobSpec = serde_json::from_slice(&bytes)?;
                submit_job(&Client::new(), &coordinator, job).await?;
            }
            JobCommand::Plan { coordinator, file } => {
                let bytes = tokio::fs::read(file).await?;
                let job: LogicalJobSpec = serde_json::from_slice(&bytes)?;
                let plan = plan_job(&Client::new(), &coordinator, job).await?;
                println!("{}", serde_json::to_string_pretty(&plan)?);
            }
            JobCommand::AutoSubmit { coordinator, file } => {
                let bytes = tokio::fs::read(file).await?;
                let job: LogicalJobSpec = serde_json::from_slice(&bytes)?;
                let submitted = auto_submit_job(&Client::new(), &coordinator, job).await?;
                println!("{}", submitted.job_id);
            }
            JobCommand::Status { coordinator, job } => {
                let view = cooperative_job_view(&Client::new(), &coordinator, job).await?;
                println!("{}", serde_json::to_string_pretty(&view)?);
            }
            JobCommand::Wait {
                coordinator,
                job,
                timeout_s,
            } => wait_job(&Client::new(), &coordinator, job, timeout_s).await?,
        },
        Command::Resources { coordinator } => {
            let response = checked(
                Client::new()
                    .get(format!("{}/v1/resources", base(&coordinator)))
                    .send()
                    .await?,
            )
            .await?;
            let resources: ResourceListResponse = response.json().await?;
            println!("{}", serde_json::to_string_pretty(&resources)?);
        }
        Command::Link {
            coordinator,
            from,
            to,
            rtt_ms,
            bandwidth_mbps,
        } => {
            checked(
                Client::new()
                    .post(format!("{}/v1/topology/link", base(&coordinator)))
                    .json(&LinkUpdateRequest {
                        link: LinkProfile {
                            from,
                            to,
                            rtt_ms,
                            bandwidth_mbps,
                        },
                    })
                    .send()
                    .await?,
            )
            .await?;
        }
    }
    Ok(())
}

async fn put(
    client: &Client,
    coordinator: &str,
    agent: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = tokio::fs::read(file).await?;
    let response = checked(
        client
            .post(format!("{}/v1/objects", base(agent)))
            .body(bytes)
            .send()
            .await?,
    )
    .await?;
    let put: PutObjectResponse = response.json().await?;
    checked(
        client
            .post(format!("{}/v1/objects/publish", base(coordinator)))
            .json(&PublishObjectRequest {
                object: put.object.clone(),
            })
            .send()
            .await?,
    )
    .await?;
    println!("{}", put.object.id);
    Ok(())
}

async fn submit(
    client: &Client,
    coordinator: &str,
    task: TaskSpec,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = checked(
        client
            .post(format!("{}/v1/tasks", base(coordinator)))
            .json(&SubmitTaskRequest { task })
            .send()
            .await?,
    )
    .await?;
    let submitted: SubmitTaskResponse = response.json().await?;
    println!("{}", submitted.task_id);
    Ok(())
}

async fn task_view(
    client: &Client,
    coordinator: &str,
    task: TaskId,
) -> Result<TaskView, Box<dyn std::error::Error>> {
    let response = checked(
        client
            .get(format!("{}/v1/tasks/{task}", base(coordinator)))
            .send()
            .await?,
    )
    .await?;
    Ok(response.json().await?)
}

async fn submit_job(
    client: &Client,
    coordinator: &str,
    job: CooperativeJobSpec,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = checked(
        client
            .post(format!("{}/v1/jobs", base(coordinator)))
            .json(&SubmitCooperativeJobRequest { job })
            .send()
            .await?,
    )
    .await?;
    let submitted: SubmitCooperativeJobResponse = response.json().await?;
    println!("{}", submitted.job_id);
    Ok(())
}

async fn plan_job(
    client: &Client,
    coordinator: &str,
    job: LogicalJobSpec,
) -> Result<CooperativePlan, Box<dyn std::error::Error>> {
    let response = checked(
        client
            .post(format!("{}/v1/jobs/plan", base(coordinator)))
            .json(&PlanLogicalJobRequest { job })
            .send()
            .await?,
    )
    .await?;
    Ok(response.json().await?)
}

async fn auto_submit_job(
    client: &Client,
    coordinator: &str,
    job: LogicalJobSpec,
) -> Result<SubmitLogicalJobResponse, Box<dyn std::error::Error>> {
    let response = checked(
        client
            .post(format!("{}/v1/jobs/auto", base(coordinator)))
            .json(&PlanLogicalJobRequest { job })
            .send()
            .await?,
    )
    .await?;
    Ok(response.json().await?)
}

async fn cooperative_job_view(
    client: &Client,
    coordinator: &str,
    job: JobId,
) -> Result<CooperativeJobView, Box<dyn std::error::Error>> {
    let response = checked(
        client
            .get(format!("{}/v1/jobs/{job}", base(coordinator)))
            .send()
            .await?,
    )
    .await?;
    Ok(response.json().await?)
}

async fn wait(
    client: &Client,
    coordinator: &str,
    task: TaskId,
    timeout_s: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(timeout_s);
    loop {
        let view = task_view(client, coordinator, task).await?;
        match view.status {
            TaskStatus::Completed(_) => {
                println!("{}", serde_json::to_string_pretty(&view)?);
                return Ok(());
            }
            TaskStatus::Uncertain => {
                return Err(format!("task {task} entered uncertain state").into());
            }
            TaskStatus::Pending | TaskStatus::Running(_) => {}
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for task {task}").into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_job(
    client: &Client,
    coordinator: &str,
    job: JobId,
    timeout_s: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(timeout_s);
    loop {
        let view = cooperative_job_view(client, coordinator, job).await?;
        match view.status {
            CooperativeJobStatus::Completed(_) => {
                println!("{}", serde_json::to_string_pretty(&view)?);
                return Ok(());
            }
            CooperativeJobStatus::Uncertain => {
                return Err(format!("cooperative job {job} entered uncertain state").into());
            }
            CooperativeJobStatus::Running => {}
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for cooperative job {job}").into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn checked(
    response: reqwest::Response,
) -> Result<reqwest::Response, Box<dyn std::error::Error>> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let message = response
        .json::<ErrorResponse>()
        .await
        .map(|error| error.error)
        .unwrap_or_else(|_| "request failed".to_owned());
    Err(format!("{status}: {message}").into())
}

fn base(url: &str) -> &str {
    url.trim_end_matches('/')
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
    fabric
        .publish_object(ObjectMetadata {
            id: data,
            size_bytes: 10 << 30,
            digest: None,
            encoding: None,
            locations: vec![local],
            producer: None,
        })
        .unwrap();
    let task = TaskSpec {
        id: TaskId::new(),
        artifact: "demo://analysis".into(),
        entrypoint: "run".into(),
        arguments: vec![],
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
