mod api;
mod db;
mod models;
mod runtime;
mod scheduler;
mod helper;

pub mod cluster {
    tonic::include_proto!("cluster");
}
use api::create_service;
use db::StateStore;
use std::sync::Arc;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "mysql://root:password@localhost:3306/kuber".to_string());
    let db = Arc::new(StateStore::new(&db_url).await?);
    let addr = "[::1]:50051".parse()?; // Ipv6 loopback at port 50051 for gRPC
    println!("KubeR gRPC server listening on {}", addr);
    let cluster_service = create_service(db.clone());
    Server::builder().add_service(cluster_service).serve(addr).await?;
    Ok(())
}
