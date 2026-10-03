use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type NodeId = String;
pub type PodId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub addr: String,
    pub cap: Resources,
    pub availib: Resources,
    pub status: NodeStatus,
    pub labels: HashMap<String, String>,
    pub last_ping: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resources {
    pub cpu_t: u64, // The cpu millisecs the pod will need
    pub mem: u64, // Memory the pod will need
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeStatus {
    Unknown,
    Ready,
    NotReady,
}

impl NodeStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeStatus::Unknown => "Unknown",
            NodeStatus::Ready => "Ready",
            NodeStatus::NotReady => "NotReady",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match &s.to_lowercase()[..]{
            "ready" => NodeStatus::Ready,
            "notready" => NodeStatus::NotReady,
            _ => NodeStatus::Unknown,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pod {
    pub id: PodId,
    pub name: String,
    pub namespace: String,
    pub spec: PodSpec,
    pub status: PodStatus,
    pub phase: PodPhase,
    pub node_id: Option<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodSpec {
    pub containers: Vec<ContainerSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub resources: Resources,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodStatus {
    pub pod_id: PodId,
    pub mssg: String,
    pub container_statuses: Vec<ContainerStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerStatus {
    pub name: String,
    pub ready: bool,
    pub ecode: i32,
    pub mssg: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PodPhase {
    Pending,
    Scheduled,
    Starting,
    Running,
    Succeeded,
    Failed,
    Terminating,
    Terminated,
}

impl PodPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            PodPhase::Pending => "Pending",
            PodPhase::Scheduled => "Scheduled",
            PodPhase::Starting => "Starting",
            PodPhase::Running => "Running",
            PodPhase::Succeeded => "Succeeded",
            PodPhase::Failed => "Failed",
            PodPhase::Terminating => "Terminating",
            PodPhase::Terminated => "Terminated",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match &s.to_lowercase()[..]{
            "pending" => PodPhase::Pending,
            "scheduled" => PodPhase::Scheduled,
            "starting" => PodPhase::Starting,
            "running" => PodPhase::Running,
            "succeeded" => PodPhase::Succeeded,
            "failed" => PodPhase::Failed,
            "terminating" => PodPhase::Terminating,
            "terminated" => PodPhase::Terminated,
            _ => PodPhase::Pending,
        }
    }
}
