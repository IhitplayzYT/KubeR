use crate::db::StateStore;
use crate::models::{ContainerSpec, ContainerStatus, Node, NodeStatus, Pod, PodPhase, PodSpec, PodStatus, Resources};
use crate::scheduler::Scheduler;
use super::cluster::cluster_api_server::{ClusterApi, ClusterApiServer};
use super::cluster::{Pod as ClusterPod, Node as ClusterNode, Resources as ClusterResources, ContainerSpec as ClusterContainerSpec, ContainerStatus as ClusterContainerStatus, PodStatus as ClusterPodStatus, PodSpec as ClusterPodSpec, CreatePodRequest, DeletePodResponse, ListPodsRequest, ListPodsResponse, RegisterNodeRequest, HeartbeatRequest, HeartbeatResponse, NodeId,PodId, ListNodesRequest, ListNodesResponse, Ack};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub struct ClusterService {
    db: Arc<StateStore>,
    scheduler: Arc<Scheduler>,
}

impl ClusterService {
    pub fn new(db: Arc<StateStore>) -> Self {
        ClusterService {db,scheduler: Arc::new(Scheduler::new())}
    }
    fn convert_pod_to_proto(pod: &Pod) -> ClusterPod {ClusterPod {id: pod.id.clone(),name: pod.name.clone(),namespace: pod.namespace.clone(),spec: Some(ClusterPodSpec {containers: pod.spec.containers.iter().map(|c| ClusterContainerSpec {name: c.name.clone(),image: c.image.clone(),command: c.command.clone(),args: c.args.clone(),cpu_t: c.resources.cpu_t,mem: c.resources.mem}).collect()}),status: Some(ClusterPodStatus {pod_id: pod.status.pod_id.clone(),phase: pod.phase.as_str().to_string(),mssg: pod.status.mssg.clone(),statuses: pod.status.container_statuses.iter().map(|c| ClusterContainerStatus {name: c.name.clone(),ready: c.ready,ecode: c.ecode,mssg: c.mssg.clone()}).collect()}),phase: pod.phase.as_str().to_string(),node_id: pod.node_id.clone().unwrap_or_default()}}
    fn convert_node_to_proto(node: &Node) -> ClusterNode {ClusterNode {id: node.id.clone(),addr: node.addr.clone(),cap: Some(ClusterResources {cpu_t: node.cap.cpu_t,mem: node.cap.mem}),availib: Some(ClusterResources {cpu_t: node.availib.cpu_t,mem: node.availib.mem}),status: node.status.as_str().to_string(),labels: node.labels.clone(),last_ping: node.last_ping}}
    fn convert_proto_to_pod_spec(req: &CreatePodRequest) -> PodSpec { PodSpec {containers: req.containers.iter().map(|c| ContainerSpec {name: c.name.clone(),image: c.image.clone(),command: c.command.clone(),args: c.args.clone(),resources: Resources {cpu_t: c.cpu_t,mem: c.mem}}).collect()}}
}

#[tonic::async_trait]
impl ClusterApi for ClusterService {
    async fn create_pod(&self, request: Request<CreatePodRequest>) -> Result<Response<PodId>, Status> {
        let req = request.into_inner();
        let pod_id = Uuid::new_v4().to_string();
        let spec = Self::convert_proto_to_pod_spec(&req);
        let pod = Pod {id: pod_id.clone(),name: req.name,namespace: req.namespace,spec,status: PodStatus {pod_id: pod_id.clone(),mssg: "Pod created".to_string(),container_statuses: vec![]},phase: PodPhase::Pending,node_id: None};
        self.db.put_pod(&pod).await.map_err(|e| Status::internal(format!("Database error: {}", e)))?;

        let nodes = self.db.list_nodes().await.map_err(|e| Status::internal(format!("Database error: {}", e)))?;

        if let Ok(Some(node_id)) = self.scheduler.schedule(&pod, &nodes).await {
            let mut updated_pod = pod.clone();
            updated_pod.node_id = Some(node_id.clone());
            updated_pod.phase = PodPhase::Scheduled;
            self.db.put_pod(&updated_pod).await.map_err(|e| Status::internal(format!("Database error: {}", e)))?;
        }

        Ok(Response::new(PodId { pod_id }))
    }

    async fn get_pod(&self, request: Request<PodId>) -> Result<Response<ClusterPod>, Status> {
        let req = request.into_inner();
        let pod = self.db.get_pod(&req.pod_id).await.map_err(|e| Status::internal(format!("Database error: {}", e)))?.ok_or_else(|| Status::not_found("Pod not found"))?;
        Ok(Response::new(Self::convert_pod_to_proto(&pod)))
    }

    async fn delete_pod(&self, request: Request<PodId>) -> Result<Response<DeletePodResponse>, Status> {
        let req = request.into_inner();
        self.db.delete_pod(&req.pod_id).await.map_err(|e| Status::internal(format!("Database error: {}", e)))?;
        Ok(Response::new(DeletePodResponse { success: true }))
    }

    async fn list_pods(&self, request: Request<ListPodsRequest>) -> Result<Response<ListPodsResponse>, Status> {
        let req = request.into_inner();
        let pods = if req.namespace.is_empty() {
            self.db.list_pods().await
        } else {
            self.db.list_pods_by_namespace(&req.namespace).await
        }.map_err(|e| Status::internal(format!("Database error: {}", e)))?;

        Ok(Response::new(ListPodsResponse { pods:pods.iter().map(Self::convert_pod_to_proto).collect() }))
    }

    async fn register_node(&self, request: Request<RegisterNodeRequest>) -> Result<Response<NodeId>, Status> {
        let req = request.into_inner();
        let node_id = Uuid::new_v4().to_string();
        let node = Node {id: node_id.clone(),addr: req.addr,cap: Resources {cpu_t: req.cpu_t,mem : req.mem},availib: Resources {cpu_t: req.cpu_t,mem: req.mem},status: NodeStatus::Ready,labels: req.labels,last_ping: chrono::Utc::now().timestamp()};
        self.db.put_node(&node).await.map_err(|e| Status::internal(format!("Database error: {}", e)))?;
        Ok(Response::new(NodeId { node_id }))
    }

    async fn heartbeat(&self, request: Request<HeartbeatRequest>) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        if let Some(mut node) = self.db.get_node(&req.node_id).await.map_err(|e| Status::internal(format!("Database error: {}", e)))? {
            node.availib = Resources {
                cpu_t: req.cpu_t,
                mem: req.mem,
            };
            node.last_ping = chrono::Utc::now().timestamp();
            node.status = NodeStatus::Ready;

            self.db.put_node(&node).await.map_err(|e| Status::internal(format!("Database error: {}", e)))?;
            Ok(Response::new(HeartbeatResponse { accepted: true }))
        } else {
            Ok(Response::new(HeartbeatResponse { accepted: false }))
        }
    }

    async fn get_node(&self, request: Request<NodeId>) -> Result<Response<ClusterNode>, Status> {
        Ok(Response::new(Self::convert_node_to_proto(& self.db.get_node(& request.into_inner().node_id).await.map_err(|e| Status::internal(format!("Database error: {}", e)))?.ok_or_else(|| Status::not_found("Node not found"))?)))
    }

    async fn list_nodes(&self, _request: Request<ListNodesRequest>) -> Result<Response<ListNodesResponse>, Status> {
        let nodes = self.db.list_nodes().await.map_err(|e| Status::internal(format!("Database error: {}", e)))?;
        let proto_nodes: Vec<ClusterNode> = nodes.iter().map(Self::convert_node_to_proto).collect();
        Ok(Response::new(ListNodesResponse { nodes: proto_nodes }))
    }

    async fn report_pod_status(&self, request: tonic::Request<ClusterPodStatus>) -> Result<Response<Ack>, Status> {
        let req = request.into_inner();
        if let Some(mut pod) = self.db.get_pod(&req.pod_id).await.map_err(|e| Status::internal(format!("Database error: {}", e)))?{
            pod.status = PodStatus {pod_id: req.pod_id.clone(),mssg: req.mssg.clone(),container_statuses: req.statuses.iter().map(|c| ContainerStatus {name: c.name.clone(),ready: c.ready,ecode: c.ecode,mssg: c.mssg.clone()}).collect(),};
            pod.phase = PodPhase::from_str(&req.phase);
            self.db.put_pod(&pod).await.map_err(|e| Status::internal(format!("Database error: {}", e)))?;
            Ok(Response::new(Ack { success: true }))
        } else {
            Ok(Response::new(Ack { success: false }))
        }
    }
}

pub fn create_service(db: Arc<StateStore>) -> ClusterApiServer<ClusterService> {
    ClusterApiServer::new(ClusterService::new(db))
}
