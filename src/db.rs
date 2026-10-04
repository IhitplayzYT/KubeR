use crate::models::{Node, NodeId, NodeStatus, Pod, PodId, PodPhase, PodSpec, PodStatus, Resources};
use sqlx::{MySql, Pool, mysql::MySqlPoolOptions};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),
    #[error("Pod not found: {0}")]
    PodNotFound(PodId),
}

pub type Result<T> = std::result::Result<T, DbError>;

pub struct StateStore {
    pool: Pool<MySql>,
}

impl StateStore {
    pub async fn new(db_url: &str) -> Result<Self> {
        let pool = MySqlPoolOptions::new().connect(db_url).await?;
        Self::init_schema(&pool).await?;
        Ok(StateStore { pool })
    }

    async fn init_schema(pool: &Pool<MySql>) -> Result<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS nodes (
                id VARCHAR(255) PRIMARY KEY,
                addr VARCHAR(255) NOT NULL,
                cpu_cap BIGINT NOT NULL,
                mem_cap BIGINT NOT NULL,
                cpu_availib BIGINT NOT NULL,
                mem_availib BIGINT NOT NULL,
                status VARCHAR(50) NOT NULL,
                labels JSON,
                last_ping BIGINT NOT NULL
            )"#).execute(pool).await?;

        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS pods (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                namespace VARCHAR(255) NOT NULL,
                spec JSON NOT NULL,
                status JSON NOT NULL,
                phase VARCHAR(50) NOT NULL,
                node_id VARCHAR(255),
                FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE SET NULL
            )"#).execute(pool).await?;
        Ok(())
    }

    pub async fn get_node(&self, id: &NodeId) -> Result<Option<Node>> {
        let row = sqlx::query_as::<_, (String, String, i64, i64, i64, i64, String, Option<String>, i64)>("SELECT id, addr, cpu_cap, mem_cap, cpu_availib, mem_availib, status, labels, last_ping FROM nodes WHERE id = ?").bind(id).fetch_optional(&self.pool).await?;

        if let Some((id, addr, cpu_cap, mem_cap, cpu_alloc, mem_alloc, status, labels, last_ping)) = row {
            let labels: serde_json::Value = labels.and_then(|l| serde_json::from_str(&l).ok()).unwrap_or(serde_json::json!({}));
            let labels_map = labels.as_object().map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string())).collect()).unwrap_or_default();
            Ok(Some(Node {id,addr,cap: Resources {cpu_t: cpu_cap as u64,mem: mem_cap as u64},availib: Resources {cpu_t: cpu_alloc as u64,mem: mem_alloc as u64},status: NodeStatus::from_str(&status),labels: labels_map,last_ping}))
        } else {
            Ok(None)
        }
    }

    pub async fn put_node(&self, node: &Node) -> Result<()> {
        let labels_json = serde_json::to_string(&node.labels).unwrap_or_default();
        sqlx::query(r#"
            INSERT INTO nodes (id, addr, cpu_cap, mem_cap, cpu_availib, mem_availib, status, labels, last_ping) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE
                addr = VALUES(addr),
                cpu_cap = VALUES(cpu_cap),
                mem_cap = VALUES(mem_cap),
                cpu_availib = VALUES(cpu_availib),
                mem_availib = VALUES(mem_availib),
                status = VALUES(status),
                labels = VALUES(labels),
                last_ping = VALUES(last_ping)
            "#).bind(&node.id).bind(&node.addr).bind(node.cap.cpu_t as i64).bind(node.cap.mem as i64).bind(node.availib.cpu_t as i64).bind(node.availib.mem as i64).bind(node.status.as_str()).bind(&labels_json).bind(node.last_ping).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn delete_node(&self, id: &NodeId) -> Result<()> {
        sqlx::query("DELETE FROM nodes WHERE id = ?").bind(id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn list_nodes(&self) -> Result<Vec<Node>> {
        let rows = sqlx::query_as::<_, (String, String, i64, i64, i64, i64, String, Option<String>, i64)>("SELECT id, addr, cpu_cap, mem_cap, cpu_availib, mem_availib, status, labels, last_ping FROM nodes").fetch_all(&self.pool).await?;
        let nodes = rows.into_iter().map(|(id, addr, cpu_cap, mem_cap, cpu_alloc, mem_alloc, status, labels, last_ping)| {
            let labels: serde_json::Value = labels.and_then(|l| serde_json::from_str(&l).ok()).unwrap_or(serde_json::json!({}));
            let labels_map = labels.as_object().map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string())).collect()).unwrap_or_default();
            Node {id,addr,cap: Resources {cpu_t: cpu_cap as u64,mem: mem_cap as u64},availib:Resources {cpu_t: cpu_alloc as u64,mem: mem_alloc as u64},status: NodeStatus::from_str(&status),labels: labels_map,last_ping}}).collect();
        Ok(nodes)
    }

    pub async fn get_pod(&self, id: &PodId) -> Result<Option<Pod>> {
        let row = sqlx::query_as::<_, (String, String, String, String, String, String, Option<String>)>("SELECT id, name, namespace, spec, status, phase, node_id FROM pods WHERE id = ?").bind(id).fetch_optional(&self.pool).await?;
        if let Some((id, name, namespace, spec_json, status_json, phase, node_id)) = row {
            let spec:PodSpec = serde_json::from_str(&spec_json).map_err(|e| DbError::Sqlx(sqlx::Error::Decode(Box::new(e))))?;
            let status: PodStatus = serde_json::from_str(&status_json).map_err(|e| DbError::Sqlx(sqlx::Error::Decode(Box::new(e))))?;
            Ok(Some(Pod {id,name,namespace,spec,status,phase: PodPhase::from_str(&phase),node_id}))
        } else {
            Ok(None)
        }
    }

    pub async fn put_pod(&self, pod: &Pod) -> Result<()> {
        let spec_json = serde_json::to_string(&pod.spec).map_err(|e| DbError::Sqlx(sqlx::Error::Decode(Box::new(e))))?;
        let status_json = serde_json::to_string(&pod.status).map_err(|e| DbError::Sqlx(sqlx::Error::Decode(Box::new(e))))?;
        sqlx::query(r#"
            INSERT INTO pods (id, name, namespace, spec, status, phase, node_id) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                namespace = VALUES(namespace),
                spec = VALUES(spec),
                status = VALUES(status),
                phase = VALUES(phase),
                node_id = VALUES(node_id)
            "#).bind(&pod.id).bind(&pod.name).bind(&pod.namespace).bind(&spec_json).bind(&status_json).bind(pod.phase.as_str()).bind(&pod.node_id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn delete_pod(&self, id: &PodId) -> Result<()> {
        sqlx::query("DELETE FROM pods WHERE id = ?").bind(id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn list_pods(&self) -> Result<Vec<Pod>> {
        let rows = sqlx::query_as::<_, (String, String, String, String, String, String, Option<String>)>("SELECT id, name, namespace, spec, status, phase, node_id FROM pods").fetch_all(&self.pool).await?;
        let pods = rows.into_iter().map(|(id, name, namespace, spec_json, status_json, phase, node_id)| {
                let spec: PodSpec = serde_json::from_str(&spec_json).unwrap();
                let status: PodStatus = serde_json::from_str(&status_json).unwrap();
                Pod {id,name,namespace,spec,status,phase: PodPhase::from_str(&phase),node_id}
            }).collect();
        Ok(pods)
    }
    pub async fn list_pods_by_namespace(&self, namespace: &str) -> Result<Vec<Pod>> {
        let rows = sqlx::query_as::<_, (String, String, String, String, String, String, Option<String>)>("SELECT id, name, namespace, spec, status, phase, node_id FROM pods WHERE namespace = ?").bind(namespace).fetch_all(&self.pool).await?;
        let pods = rows.into_iter().map(|(id, name, namespace, spec_json, status_json, phase, node_id)| {
                let spec: PodSpec = serde_json::from_str(&spec_json).unwrap();
                let status: PodStatus = serde_json::from_str(&status_json).unwrap();
                Pod {id,name,namespace,spec,status,phase: PodPhase::from_str(&phase),node_id}
            }).collect();
        Ok(pods)
    }
}
