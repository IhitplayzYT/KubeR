use crate::models::{ContainerSpec, ContainerStatus, PodStatus, PodId};
use std::collections::HashMap;
use std::process::Child;
use std::sync::Arc;
use tokio::sync::Mutex;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("Process spawn error: {0}")]
    SpawnError(#[from] std::io::Error),
    #[error("Pod {0} not found")]
    PodNotFound(PodId),
    #[error("Container {0} not found")]
    ContainerNotFound(String),
}

pub type Result<T> = std::result::Result<T, RuntimeError>;

pub struct ProcessRuntime {
    processes: Arc<Mutex<HashMap<String, HashMap<String, ProcessInfo>>>>,
}

#[derive(Clone)]
struct ProcessInfo {
    child: Arc<Mutex<Option<Child>>>, // The pod
    exit_code: Arc<Mutex<Option<i32>>>,
}

impl ProcessRuntime {
    pub fn new() -> Self {
        ProcessRuntime { processes: Arc::new(Mutex::new(HashMap::new()))}
    }

    pub async fn start_pod(&self, pod_id: &str, containers: &[ContainerSpec]) -> Result<()> {
        let mut cont_map = HashMap::new();
        let mut ret = self.processes.lock().await;

        for cont in containers {
            let mut cmd = std::process::Command::new(&cont.image);
            
            if !cont.command.is_empty() {
                cmd.args(&cont.command);
            }

            if !cont.args.is_empty() {
                cmd.args(&cont.args);
            }

            cont_map.insert(cont.name.clone(),  ProcessInfo {child: Arc::new(Mutex::new(Some(cmd.spawn()?))),exit_code: Arc::new(Mutex::new(None))});
        }
        ret.insert(pod_id.to_string(), cont_map);
        Ok(())
    }

    pub async fn stop_pod(&self, pod_id: &str) -> Result<()> {
        let mut processes = self.processes.lock().await;
        if let Some(container_map) = processes.remove(pod_id) {
            for (_, proc) in container_map {
                if let Some(mut child) = proc.child.lock().await.take() {
                    child.kill()?;
                    child.wait()?;
                }
            }
        }
        Ok(())
    }

    pub async fn get_pod_status(&self, pod_id: &str) -> Result<PodStatus> {
        let processes = self.processes.lock().await;        
        let container_map = processes.get(pod_id).ok_or_else(|| RuntimeError::PodNotFound(pod_id.to_string()))?;
        let mut status = Vec::new();
        
        for (name, info) in container_map {
            let ready =  info.child.lock().await.is_some();
            let exit_code = * info.exit_code.lock().await;
            status.push(ContainerStatus {name: name.clone(),ready,ecode: exit_code.unwrap_or(0),mssg: if ready {"Running".to_string()} else if let Some(code) = exit_code {format!("Exited {}", code)} else {"Stopped".to_string()}});
        }

        Ok(PodStatus {pod_id: pod_id.to_string(),mssg: "Pod status".to_string(),container_statuses:status})
    }

    pub async fn get_pod_logs(&self, pod_id: &str, container_name: &str) -> Result<Vec<String>> {
        let processes = self.processes.lock().await;
        let container_map = processes.get(pod_id).ok_or_else(|| RuntimeError::PodNotFound(pod_id.to_string()))?;

        let _info = container_map.get(container_name).ok_or_else(|| RuntimeError::ContainerNotFound(container_name.to_string()))?;
        let mut ret = vec![];
        // TODO: Read stderr and stdout and return 
       Ok(ret) 
    }
}
