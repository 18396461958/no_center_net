use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeInfo {
    pub id: String,
    pub addr: SocketAddr,
    pub last_seen: u64,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NodeStatus {
    Alive,
    Suspicious,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub nodes: Vec<NodeInfo>,
    pub cluster_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub service_id: String,
    pub service_name: String,
    pub service_type: String,
    pub endpoints: Vec<ServiceEndpoint>,
    pub metadata: HashMap<String, String>,
    pub health_status: HealthStatus,
    pub last_health_check: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEndpoint {
    pub protocol: String,
    pub address: String,
    pub port: u16,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Unhealthy,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceListResponse {
    pub timestamp: u64,
    pub cluster_id: String,
    pub services: Vec<ServiceInfo>,
    pub total_services: usize,
    pub healthy_services: usize,
}