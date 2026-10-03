# KubeR Implementation Plan
### Architecture

```
                 ┌───────────────────────────┐
                 │       API Server          │
                 │                           │
                 │  Resource API             │
                 │  Auth                     │
                 │  Validation               │
                 │  Watch/Event system       │
                 └─────────────┬─────────────┘
                               │
                ┌──────────────┼──────────────┐
                │              │              │
                ▼              ▼              ▼
          ┌──────────┐   ┌───────────┐  ┌────────────┐
          │Scheduler │   │Controller │  │State Store │
          └────┬─────┘   │ Manager   │  │            │
               │         └─────┬─────┘  │ Mysql      │
               │               │        └────────────┘
               |               |             |
               └───────────────┼──────────────
                               │
                              gRPC
                 ┌─────────────┼─────────────┐
                 ▼             ▼             ▼
           ┌──────────┐  ┌──────────┐  ┌──────────┐
           │  Node A  │  │  Node B  │  │  Node C  │
           │  Agent   │  │  Agent   │  │  Agent   │
           └────┬─────┘  └────┬─────┘  └────┬─────┘
                │              │              │
             runtime        runtime        runtime
                │              │              │
              Pod A          Pod B          Pod C
```

### Scope (V1)

**Included:**
- Nodes
- Workloads (Pods)
- Scheduling
- Containers/processes
- Desired state reconciliation
- Health checking
- Node failure detection
- gRPC communication
- Logs
- Resource accounting
- Basic networking (host network)

## Phase 0: Architecture and Workspace Setup

### Objectives
- Define cluster architecture
- Define resource model
- Define Pod model
- Define Node model
- Define lifecycle states
- Define desired/actual state semantics
- Define gRPC boundaries
- Define protobuf messages
- Create Cargo workspace

### Tasks

#### 0.1 Define Core Data Models

**Node Model:**
```rust
struct Node {
    id: NodeId,
    address: SocketAddr,
    capacity: Resources,
    allocatable: Resources,
    status: NodeStatus,
    conditions: Vec<NodeCondition>,
    labels: HashMap<String, String>,
    last_heartbeat: Instant,
}

struct Resources {
    cpu_millis: u64,
    memory_bytes: u64,
}

enum NodeStatus {
    Unknown,
    Ready,
    NotReady,
}
```

**Pod Model:**
```rust
struct Pod {
    id: PodId,
    name: String,
    namespace: String,
    spec: PodSpec,
    status: PodStatus,
    phase: PodPhase,
    node_id: Option<NodeId>,
}

struct PodSpec {
    containers: Vec<ContainerSpec>,
}

struct ContainerSpec {
    name: String,
    image: String,
    command: Vec<String>,
    args: Vec<String>,
    resources: Resources,
}

enum PodPhase {
    Pending,
    Scheduled,
    Starting,
    Running,
    Succeeded,
    Failed,
    Terminating,
    Terminated,
}
```


#### 0.2 Define Trait Interfaces
**Scheduler:**
```rust
trait Scheduler {
    async fn schedule(&self, pod: &Pod, nodes: &[Node]) -> Result<Option<NodeId>>;
}
```

**Runtime:**
```rust
trait Runtime {
    async fn start(&self, workload: &Workload) -> Result<WorkloadId>;
    async fn stop(&self, id: WorkloadId) -> Result<()>;
    async fn status(&self, id: WorkloadId) -> Result<WorkloadStatus>;
    async fn logs(&self, id: WorkloadId) -> Result<Vec<String>>;
}
```

**StateStore:**
```rust
trait StateStore {
    async fn get_node(&self, id: &NodeId) -> Result<Option<Node>>;
    async fn put_node(&self, node: &Node) -> Result<()>;
    async fn delete_node(&self, id: &NodeId) -> Result<()>;
    async fn list_nodes(&self) -> Result<Vec<Node>>;

    async fn get_pod(&self, id: &PodId) -> Result<Option<Pod>>;
    async fn put_pod(&self, pod: &Pod) -> Result<()>;
    async fn delete_pod(&self, id: &PodId) -> Result<()>;
    async fn list_pods(&self) -> Result<Vec<Pod>>;
}
```

## Phase 1: gRPC Foundation

### Objectives
- Configure tonic/prost
- Create proto files
- Generate Rust bindings
- Implement API server skeleton
- Add request/error handling
- Add deadlines
- Add health service
- Add reflection

### Tasks

#### 1.1 Setup Protobuf Tooling
- Install protoc compiler
- Add tonic, prost, tonic-build dependencies
- Configure build.rs for proto compilation

#### 1.2 Define Protobuf Services

**cluster.proto:**
```protobuf
service ClusterApi {
    rpc CreatePod(CreatePodRequest) returns (CreatePodResponse);
    rpc GetPod(GetPodRequest) returns (Pod);
    rpc DeletePod(DeletePodRequest) returns (DeletePodResponse);
    rpc ListPods(ListPodsRequest) returns (ListPodsResponse);
    rpc WatchPods(WatchPodsRequest) returns (stream PodEvent);
    rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
    rpc GetNode(GetNodeRequest) returns (Node);
    rpc ListNodes(ListNodesRequest) returns (ListNodesResponse);
    rpc ReportPodStatus(PodStatus) returns (Ack);
}
```

**node.proto:**
```protobuf
service NodeAgent {
    rpc StartPod(StartPodRequest) returns (StartPodResponse);
    rpc StopPod(StopPodRequest) returns (StopPodResponse);
    rpc GetPodStatus(GetPodStatusRequest) returns (PodStatus);
    rpc StreamLogs(StreamLogsRequest) returns (stream LogLine);
    rpc StreamMetrics(StreamMetricsRequest) returns (stream Metric);
}
```

#### 1.3 Implement API Server
- Create tonic-based API server
- Implement unary RPC handlers
- Add error handling with thiserror
- Add request validation

#### 1.4 Implement Health Service
- Use tonic-health for health checks
- Implement serving status checks
- Add health check endpoint

## Phase 2: State Store

### Objectives
- Implement State trait
- Implement Node storage
- Implement Pod storage
- Implement Assignment storage
- Implement Event storage
- Add CRUD operations
- Add atomic state updates
- Add state recovery after restart

#### 2.2 Implement Node Storage
- Add indexing for status queries
- Add heartbeat timestamp tracking

#### 2.3 Implement Pod Storage
- Add indexing for phase queries
- Add node assignment indexing

#### 2.4 Implement Assignment Storage
- Track pod → node and node → pods mappings

#### 2.5 Implement Event Storage
- Add event retention policy


## Phase 3: Node Agent

### Objectives
- Implement node registration
- Implement node ID generation
- Implement node metadata
- Implement heartbeat mechanism
- Implement node status tracking
- Implement resource discovery
- Implement gRPC NodeAgent service
- Implement process spawning
- Implement process termination
- Implement process monitoring
- Implement exit code reporting
- Implement stdout/stderr capture

### Tasks

#### 3.1 Node Registration
- Collect node metadata (hostname, IP, resources)
- Implement RegisterNode RPC
- Store node in state store
- Initialize node as Ready

#### 3.2 Heartbeat Mechanism
- Implement periodic heartbeat (2s interval)
- Send Heartbeat RPC to API server
- Update last_heartbeat timestamp
- Handle heartbeat failures with retry

#### 3.3 Resource Discovery
- Detect CPU capacity (cores)
- Detect memory capacity
- Calculate allocatable resources
- Report resources in heartbeat

#### 3.4 Implement NodeAgent Service
- Implement StartPod RPC
- Implement StopPod RPC
- Implement GetPodStatus RPC
- Implement StreamLogs RPC (streaming)
- Implement StreamMetrics RPC (streaming)

#### 3.5 Process Runtime (V1)
- Use tokio::process::Command for process spawning
- Implement process lifecycle management
- Track running processes in memory
- Handle process termination

## Phase 4: Scheduler

### Objectives
- Watch for Pending Pods
- Obtain Ready Nodes
- Implement resource filtering
- Implement CPU constraints
- Implement memory constraints
- Implement node selection
- Persist assignment
- Report scheduling event

### Tasks

#### 4.1 Pod Watcher
- Watch for Pods with phase=Pending
- Subscribe to pod state changes
- Queue pods for scheduling

#### 4.2 Node Filtering
- Filter nodes by status (Ready only)
- Filter by resource availability (CPU, memory)
- Implement taint/toleration support (skeleton)

#### 4.3 Resource Filtering
- Check CPU capacity vs pod request
- Check memory capacity vs pod request
- Calculate remaining resources
- Filter out nodes that can't fit pod

#### 4.4 Node Selection (V1: First-Fit)

#### 4.5 Assignment Persistence

#### 4.6 Scheduling Event
- Emit event on successful scheduling
- Include pod ID, node ID, timestamp
- Store in event store

## Phase 5: Reconciliation Controllers

### Objectives
- Implement controller loop
- Implement Pod controller
- Implement desired state tracking
- Implement actual state tracking
- Implement state diff
- Implement retry mechanism
- Implement idempotent operations
- Implement backoff

### Tasks

#### 5.1 Controller Framework
- Create generic controller trait
- Implement controller loop with interval
- Add shutdown signal handling
- Add error handling and retry

#### 5.2 Pod Controller
- Watch pod state changes
- Compare desired vs actual state
- Trigger actions on state mismatch

#### 5.3 Desired vs Actual State
- Desired: Pod spec from user
- Actual: Pod status from node agent
- Detect: Scheduled but not Running
- Detect: Failed pods

#### 5.4 State Diff
- Calculate difference between desired and actual
- Identify missing pods
- Identify failed pods
- Identify orphaned pods

#### 5.5 Retry Mechanism
- Implement exponential backoff
- Add max retry limit
- Add retry counter

#### 5.6 Idempotency
- Ensure operations are idempotent
- Use version numbers/optimistic locking
- Handle duplicate operations gracefully

## Phase 6: Replica Controller

### Objectives
- Implement desired replica count
- Implement create missing replicas
- Implement remove excess replicas
- Implement failed pod replacement
- Implement rolling replacement

### Tasks

#### 6.1 Define Workload/Deployment Model
```rust
struct Deployment {
    id: DeploymentId,
    name: String,
    replicas: u32,
    pod_spec: PodSpec,
    status: DeploymentStatus,
}
```

#### 6.2 Replica Counting
- Count running pods for deployment
- Count desired replicas
- Calculate difference

#### 6.3 Create Missing Replicas
- When actual < desired
- Create new pods from pod_spec
- Assign unique names/IDs
- Submit to scheduler

#### 6.4 Remove Excess Replicas
- When actual > desired
- Select pods for deletion (youngest first)
- Delete pods gracefully
- Wait for termination

#### 6.5 Failed Pod Replacement
- Detect failed pods
- Create replacement pod
- Update deployment status

#### 6.6 Rolling Update (Skeleton)
- Define update strategy
- Implement gradual rollout
- Implement rollback capability

## Phase 7: Failure Detection

### Objectives
- Implement heartbeat timeout
- Implement Node Ready/NotReady transitions
- Implement node failure event
- Implement pod orphan detection
- Implement pod rescheduling
- Implement controller retry
- Handle network failures
- Handle API server restart recovery
- Handle node-agent restart recovery

## Phase 8: Watch API and Event Streaming

### Objectives
- Implement WatchPods streaming RPC
- Implement event bus
- Implement gRPC streaming

### Tasks

#### 8.1 Event Bus
- Create in-memory event bus
- Implement pub/sub pattern
- Support multiple subscribers
- Use channels for communication

#### 8.2 Event Streaming
- Emit events on state changes
- Buffer events for subscribers
- Handle slow consumers

#### 8.3 WatchPods Implementation
- Implement streaming RPC
- Send pod events to client
- Filter events by type
- Handle client disconnection

## Phase 9: Services and Networking

### Objectives
- Implement Pod IP assignment
- Implement pod network (host network initially)
- Implement Service object
- Implement service discovery
- Implement endpoint tracking
- Implement load balancing
- Implement round robin
- Implement health-aware routing

### Tasks

#### 9.1 Pod Networking (V1: Host Network)
#### 9.2 Service Model
```rust
struct Service {
    id: ServiceId,
    name: String,
    selector: HashMap<String, String>,
    ports: Vec<ServicePort>,
    endpoints: Vec<Endpoint>,
}
```

#### 9.3 Endpoint Tracking

#### 9.4 Service Discovery
- Implement service lookup
- Resolve service name to endpoints
- Cache endpoint mappings

#### 9.5 Load Balancing
- Implement round-robin selection
- Implement health-aware routing
- Skip unhealthy endpoints

#### 9.6 Service Proxy
- Implement simple TCP proxy
- Forward traffic to endpoints
- Handle connection failures

## Phase 10: Logs and Exec

### Objectives
- Implement stdout streaming
- Implement stderr streaming
- Implement log retrieval
- Implement log following
- Implement process stdin
- Implement process stdout/stderr
- Implement interactive exec

## Phase 11: Container Runtime

### Objectives
- Implement OCI image pull
- Implement image cache
- Implement container runtime abstraction
- Implement namespaces
- Implement cgroups
- Implement filesystem isolation
- Implement network namespace
- Implement process isolation

### Tasks

#### 11.1 Runtime Abstraction
- Define Runtime trait (already defined in Phase 0)
- Implement ProcessRuntime (existing)
- Implement ContainerRuntime (new)

#### 11.2 OCI Image Handling
- Pull images from registry
- Cache images locally
- Verify image integrity
- Manage image layers

#### 11.3 Namespace Isolation
- Use Linux namespaces (clone with CLONE_NEW*)
- Implement PID namespace
- Implement network namespace
- Implement mount namespace
- Implement UTS namespace

#### 11.4 Cgroups
- Create cgroup for container
- Set resource limits (CPU, memory)
- Enforce limits
- Cleanup on exit

#### 11.5 Filesystem Isolation
- Use pivot_root
- Mount procfs
- Mount tmpfs
- Setup root filesystem

#### 11.6 Process Isolation
- Drop capabilities
- Set seccomp filters
- Handle signals

**Acceptance Criteria:**
- Containers isolated with namespaces
- Resource limits enforced with cgroups
- Filesystem properly isolated
- Process isolation works

## Phase 14: Production Reliability

### Objectives
- Implement exponential backoff
- Implement request deadlines
- Implement retry policies
- Implement idempotency
- Implement graceful shutdown
- Implement connection recovery
- Implement leader election
- Implement controller restart recovery
- Implement state corruption handling
- Implement crash recovery

### Tasks

#### 14.1 Retry Policies
- Define retry strategies
- Implement exponential backoff
- Add jitter
- Set max retry limits

#### 14.2 Request Deadlines
- Add timeouts to all RPCs
- Implement deadline propagation
- Handle deadline exceeded errors

#### 14.3 Graceful Shutdown
- Handle SIGTERM/SIGINT
- Drain in-flight requests
- Persist state before exit
- Close connections cleanly

#### 14.4 Connection Recovery
- Detect connection failures
- Implement automatic reconnection
- Queue operations during outage
- Replay queued operations on reconnect

#### 14.5 Leader Election
- Implement leader election for controllers
- Use lease mechanism
- Only active leader runs controllers
- Handle leader transition

#### 14.6 State Corruption Handling
- Detect corrupted state
- Implement state validation
- Repair or restore from backup
- Alert on corruption

#### 14.7 Crash Recovery
- Log crash information
- Analyze crash dumps
- Implement automatic restart
- Restore state after crash