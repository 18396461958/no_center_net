use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use crate::types::{NodeInfo, NodeStatus, ClusterState, ServiceInfo, ServiceEndpoint, HealthStatus};
use uuid::Uuid;
use std::collections::HashMap;

#[derive(Clone)]
pub struct ClusterManager {
    node_id: String,
    self_addr: SocketAddr,
    pub members: Arc<DashMap<String, NodeInfo>>,
    pub services: Arc<DashMap<String, ServiceInfo>>,
    seed_nodes: Vec<SocketAddr>,
    is_running: Arc<RwLock<bool>>,
    http_client: reqwest::Client,
}

impl ClusterManager {
    pub fn new(addr: SocketAddr, seed_nodes: Vec<SocketAddr>) -> Self {
        let node_id = Uuid::new_v4().to_string();
        
        let manager = Self {
            node_id: node_id.clone(),
            self_addr: addr,
            members: Arc::new(DashMap::new()),
            services: Arc::new(DashMap::new()),
            seed_nodes,
            is_running: Arc::new(RwLock::new(true)),
            http_client: reqwest::Client::new(),
        };

        manager.add_self_to_members();
        manager.register_default_services();
        manager
    }

    fn add_self_to_members(&self) {
        let self_info = NodeInfo {
            id: self.node_id.clone(),
            addr: self.self_addr,
            last_seen: current_timestamp(),
            status: NodeStatus::Alive,
        };
        self.members.insert(self.node_id.clone(), self_info);
    }

    
    pub async fn start_health_check(&self) {
        let manager = self.clone();
        tokio::spawn(async move {
            manager.health_check_loop().await;
        });
    }

    async fn health_check_loop(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        
        while *self.is_running.read().await {
            interval.tick().await;
            self.perform_health_checks().await;
        }
    }

    async fn perform_health_checks(&self) {
        let services = self.get_all_services();
        
        for service in services {
            // 跳过自己的服务
            if service.metadata.get("node_id") == Some(&self.node_id) {
                continue;
            }
            
            // 对每个端点进行健康检查
            for endpoint in &service.endpoints {
                if self.check_endpoint_health(&endpoint).await {
                    // 更新服务健康状态
                    if let Some(mut service_entry) = self.services.get_mut(&service.service_id) {
                        service_entry.health_status = HealthStatus::Healthy;
                        service_entry.last_health_check = current_timestamp();
                    }
                } else {
                    if let Some(mut service_entry) = self.services.get_mut(&service.service_id) {
                        service_entry.health_status = HealthStatus::Unhealthy;
                    }
                }
            }
        }
    }

    async fn check_endpoint_health(&self, endpoint: &ServiceEndpoint) -> bool {
        let url = format!("{}://{}:{}{}",
            endpoint.protocol,
            endpoint.address,
            endpoint.port,
            endpoint.path.as_deref().unwrap_or("/health")
        );
        
        match self.http_client.get(&url).send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    fn register_default_services(&self) {
        // 注册当前节点提供的服务
        let api_service = ServiceInfo {
            service_id: Uuid::new_v4().to_string(),
            service_name: "cluster-api".to_string(),
            service_type: "http-api".to_string(),
            endpoints: vec![ServiceEndpoint {
                protocol: "http".to_string(),
                address: self.self_addr.ip().to_string(),
                port: self.self_addr.port(),
                path: Some("/api".to_string()),
            }],
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("version".to_string(), "1.0.0".to_string());
                meta.insert("node_id".to_string(), self.node_id.clone());
                meta
            },
            health_status: HealthStatus::Healthy,
            last_health_check: current_timestamp(),
        };

        self.services.insert(api_service.service_id.clone(), api_service);

        // 注册gossip服务
        let gossip_service = ServiceInfo {
            service_id: Uuid::new_v4().to_string(),
            service_name: "cluster-gossip".to_string(),
            service_type: "gossip".to_string(),
            endpoints: vec![ServiceEndpoint {
                protocol: "http".to_string(),
                address: self.self_addr.ip().to_string(),
                port: self.self_addr.port(),
                path: Some("/gossip".to_string()),
            }],
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("version".to_string(), "1.0.0".to_string());
                meta.insert("node_id".to_string(), self.node_id.clone());
                meta
            },
            health_status: HealthStatus::Healthy,
            last_health_check: current_timestamp(),
        };

        self.services.insert(gossip_service.service_id.clone(), gossip_service);
    }

    pub async fn join_cluster(&self) -> anyhow::Result<()> {
        for seed in &self.seed_nodes {
            if let Ok((members, services)) = self.query_seed_node(seed).await {
                for member in members {
                    self.members.insert(member.id.clone(), member);
                }
                for service in services {
                    self.services.insert(service.service_id.clone(), service);
                }
                break;
            }
        }
        Ok(())
    }

    async fn query_seed_node(&self, addr: &SocketAddr) -> anyhow::Result<(Vec<NodeInfo>, Vec<ServiceInfo>)> {
        if addr == &self.self_addr {
            return Ok((self.get_all_members(), self.get_all_services()));
        }
        
        let members_url = format!("http://{}/cluster/members", addr);
        let services_url = format!("http://{}/cluster/services", addr);
        
        let members_result = self.http_client.get(&members_url).send().await;
        let services_result = self.http_client.get(&services_url).send().await;
        
        let members = match members_result {
            Ok(response) if response.status().is_success() => {
                response.json::<Vec<NodeInfo>>().await.unwrap_or_default()
            }
            _ => vec![],
        };
        
        let services = match services_result {
            Ok(response) if response.status().is_success() => {
                response.json::<Vec<ServiceInfo>>().await.unwrap_or_default()
            }
            _ => vec![],
        };
        
        Ok((members, services))
    }

    pub fn get_all_members(&self) -> Vec<NodeInfo> {
        self.members.iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn get_alive_members(&self) -> Vec<NodeInfo> {
        self.members.iter()
            .filter(|entry| matches!(entry.value().status, NodeStatus::Alive))
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn get_all_services(&self) -> Vec<ServiceInfo> {
        self.services.iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn get_healthy_services(&self) -> Vec<ServiceInfo> {
        self.services.iter()
            .filter(|entry| matches!(entry.value().health_status, HealthStatus::Healthy))
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn get_services_by_type(&self, service_type: &str) -> Vec<ServiceInfo> {
        self.services.iter()
            .filter(|entry| entry.value().service_type == service_type)
            .filter(|entry| matches!(entry.value().health_status, HealthStatus::Healthy))
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn get_services_by_name(&self, service_name: &str) -> Vec<ServiceInfo> {
        self.services.iter()
            .filter(|entry| entry.value().service_name == service_name)
            .filter(|entry| matches!(entry.value().health_status, HealthStatus::Healthy))
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn get_random_member(&self) -> Option<NodeInfo> {
        let members: Vec<NodeInfo> = self.get_alive_members()
            .into_iter()
            .filter(|m| m.id != self.node_id)
            .collect();
        
        if members.is_empty() {
            return None;
        }
        
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let index = {
            let mut hasher = DefaultHasher::new();
            current_timestamp().hash(&mut hasher);
            (hasher.finish() as usize) % members.len()
        };
        
        Some(members[index].clone())
    }

    pub fn get_random_service_endpoint(&self, service_name: &str) -> Option<ServiceEndpoint> {
        let services = self.get_services_by_name(service_name);
        if services.is_empty() {
            return None;
        }
        
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let index = {
            let mut hasher = DefaultHasher::new();
            current_timestamp().hash(&mut hasher);
            (hasher.finish() as usize) % services.len()
        };
        
        services.get(index).and_then(|service| {
            service.endpoints.first().map(|endpoint| endpoint.clone())
        })
    }

    pub async fn start_gossip(&self) {
        let manager = self.clone();
        tokio::spawn(async move {
            manager.gossip_loop().await;
        });
    }

    async fn gossip_loop(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        
        while *self.is_running.read().await {
            interval.tick().await;
            
            if let Some(target) = self.get_random_member() {
                if let Err(e) = self.send_gossip(&target).await {
                    tracing::warn!("Failed to send gossip to {}: {}", target.addr, e);
                }
            }
            
            self.cleanup_dead_nodes();
            self.cleanup_dead_services();
        }
    }

    async fn send_gossip(&self, target: &NodeInfo) -> anyhow::Result<()> {
        let gossip_message = serde_json::json!({
            "sender_id": self.node_id,
            "sender_addr": self.self_addr.to_string(),
            "members": self.get_all_members(),
            "services": self.get_all_services(),
            "timestamp": current_timestamp(),
        });
        
        let url = format!("http://{}/gossip/members", target.addr);
        
        let request_builder = self.http_client.post(&url);
        let request_builder = request_builder.json(&gossip_message);
        
        match request_builder.send().await {
            Ok(response) if response.status().is_success() => {
                if let Ok(remote_services) = response.json::<Vec<ServiceInfo>>().await {
                    for service in remote_services {
                        self.services.insert(service.service_id.clone(), service);
                    }
                }
                tracing::info!("Gossip exchanged successfully with {}", target.addr);
                Ok(())
            }
            Ok(response) => {
                tracing::warn!("Gossip to {} failed with status: {}", target.addr, response.status());
                Ok(())
            }
            Err(e) => {
                tracing::warn!("Failed to send gossip to {}: {}", target.addr, e);
                Ok(())
            }
        }
    }

    fn cleanup_dead_nodes(&self) {
        let now = current_timestamp();
        let timeout_duration = 60_000;
        
        self.members.retain(|_, member| {
            if member.id == self.node_id {
                return true;
            }
            
            if now.saturating_sub(member.last_seen) > timeout_duration {
                tracing::warn!("Node {} marked as dead", member.id);
                return false;
            }
            
            true
        });
    }

    fn cleanup_dead_services(&self) {
        let now = current_timestamp();
        let timeout_duration = 120_000;
        
        self.services.retain(|_, service| {
            if service.metadata.get("node_id") == Some(&self.node_id) {
                return true;
            }
            
            if now.saturating_sub(service.last_health_check) > timeout_duration {
                tracing::warn!("Service {} marked as dead", service.service_id);
                return false;
            }
            
            true
        });
    }

    pub fn get_cluster_state(&self) -> ClusterState {
        let nodes = self.get_all_members();
        ClusterState {
            cluster_size: nodes.len(),
            nodes,
        }
    }

    pub fn register_service(&self, service: ServiceInfo) {
        self.services.insert(service.service_id.clone(), service);
    }

    pub fn unregister_service(&self, service_id: &str) {
        self.services.remove(service_id);
    }
}

pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}