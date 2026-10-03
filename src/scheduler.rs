use crate::models::{Node, NodeId, Pod, NodeStatus};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("No available nodes")]
    NoAvailableNodes,
    #[error("Insufficient resources on node {0}")]
    InsufficientResources(NodeId),
}
pub type Result<T> = std::result::Result<T, SchedulerError>;

pub struct Scheduler;

impl Scheduler {
    pub fn new() -> Self {
        Scheduler
    }

    pub async fn schedule(&self, pod: &Pod, nodes: &[Node]) -> Result<Option<NodeId>> {
        let availib_nd: Vec<&Node> = nodes.iter().filter(|n| matches!(n.status, NodeStatus::Ready)).collect();

        if availib_nd.is_empty() {
            return Err(SchedulerError::NoAvailableNodes);
        }

        let (mut req_cpu,mut req_mem) = (0u64,0u64);
        for cont in &pod.spec.containers {
            req_cpu += cont.resources.cpu_millis;
            req_mem += cont.resources.memory_bytes;
        }

        for node in &availib_nd {
            if node.allocatable.cpu_millis >= req_cpu && node.allocatable.memory_bytes >= req_mem {
                return Ok(Some(node.id.clone()));
            }
        }
        Err(SchedulerError::InsufficientResources("any".to_string()))
    }
}
