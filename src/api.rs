use axum::{
    extract::{State, Query, Path},
    http::StatusCode,
    response::Json,
    routing::{get, post, delete},
    Router,
};
use std::sync::Arc;
use crate::cluster::ClusterManager;
use crate::types::{ClusterState, ServiceListResponse, ServiceInfo, ServiceEndpoint};

pub type SharedClusterManager = Arc<ClusterManager>;

#[derive(Debug, serde::Deserialize)]
pub struct ServiceQuery {
    pub service_type: Option<String>,
    pub service_name: Option<String>,
    pub healthy_only: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
pub struct ServiceRegistration {
    pub service_name: String,
    pub service_type: String,
    pub endpoints: Vec<ServiceEndpoint>,
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

pub fn create_api_router(cluster_manager: SharedClusterManager) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/cluster/state", get(get_cluster_state))
        .route("/cluster/members", get(get_members))
        .route("/cluster/services", get(get_services))
        .route("/cluster/services", post(register_service))  // 新增：注册服务
        .route("/cluster/services/{service_id}", delete(unregister_service))  // 新增：注销服务
        .route("/cluster/services/healthy", get(get_healthy_services))
        .route("/cluster/services/available", get(get_available_services))
        .route("/cluster/services/{service_name}/endpoint", get(get_service_endpoint))  // 新增：获取服务端点
        .route("/api/{service}", get(proxy_request))
        .route("/gossip/members", post(receive_gossip))
        .with_state(cluster_manager)
}

async fn health_check() -> &'static str {
    "OK"
}

async fn get_cluster_state(
    State(cluster): State<SharedClusterManager>,
) -> Json<ClusterState> {
    Json(cluster.get_cluster_state())
}

async fn get_members(
    State(cluster): State<SharedClusterManager>,
) -> Json<Vec<crate::types::NodeInfo>> {
    Json(cluster.get_all_members())
}

async fn get_services(
    State(cluster): State<SharedClusterManager>,
) -> Json<Vec<crate::types::ServiceInfo>> {
    Json(cluster.get_all_services())
}

// 新增：注册服务端点
async fn register_service(
    State(cluster): State<SharedClusterManager>,
    Json(registration): Json<ServiceRegistration>,
) -> Result<Json<ServiceInfo>, StatusCode> {
    use uuid::Uuid;
    
    let service_info = ServiceInfo {
        service_id: Uuid::new_v4().to_string(),
        service_name: registration.service_name,
        service_type: registration.service_type,
        endpoints: registration.endpoints,
        metadata: registration.metadata.unwrap_or_default(),
        health_status: crate::types::HealthStatus::Healthy,
        last_health_check: crate::cluster::current_timestamp(),
    };
    
    cluster.register_service(service_info.clone());
    tracing::info!("Registered new service: {}", service_info.service_name);
    
    Ok(Json(service_info))
}

// 新增：注销服务端点
async fn unregister_service(
    State(cluster): State<SharedClusterManager>,
    Path(service_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    cluster.unregister_service(&service_id);
    tracing::info!("Unregistered service: {}", service_id);
    
    Ok(Json(serde_json::json!({
        "message": "Service unregistered successfully",
        "service_id": service_id
    })))
}

// 新增：获取服务随机端点
async fn get_service_endpoint(
    State(cluster): State<SharedClusterManager>,
    Path(service_name): Path<String>,
) -> Result<Json<ServiceEndpoint>, StatusCode> {
    if let Some(endpoint) = cluster.get_random_service_endpoint(&service_name) {
        Ok(Json(endpoint))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn get_healthy_services(
    State(cluster): State<SharedClusterManager>,
) -> Json<Vec<crate::types::ServiceInfo>> {
    Json(cluster.get_healthy_services())
}

async fn get_available_services(
    State(cluster): State<SharedClusterManager>,
    Query(params): Query<ServiceQuery>,
) -> Json<ServiceListResponse> {
    let timestamp = crate::cluster::current_timestamp();

    let services = if let Some(service_type) = &params.service_type {
        cluster.get_services_by_type(service_type)
    } else if let Some(service_name) = &params.service_name {
        cluster.get_services_by_name(service_name)
    } else if params.healthy_only.unwrap_or(true) {
        cluster.get_healthy_services()
    } else {
        cluster.get_all_services()
    };

    let healthy_count = services.iter()
        .filter(|s| matches!(s.health_status, crate::types::HealthStatus::Healthy))
        .count();

    let response = ServiceListResponse {
        timestamp,
        cluster_id: "default-cluster".to_string(),
        services,
        total_services: cluster.get_all_services().len(),
        healthy_services: healthy_count,
    };

    Json(response)
}

// 改进的代理请求，使用服务发现
async fn proxy_request(
    Path(service_name): Path<String>,
    State(cluster): State<SharedClusterManager>,
) -> Result<String, StatusCode> {
    // 使用服务发现获取目标端点
    if let Some(endpoint) = cluster.get_random_service_endpoint(&service_name) {
        Ok(format!(
            "Proxying to service '{}' at {}://{}:{}{}",
            service_name,
            endpoint.protocol,
            endpoint.address,
            endpoint.port,
            endpoint.path.unwrap_or_default()
        ))
    } else {
        // 回退到随机节点
        if let Some(target) = cluster.get_random_member() {
            Ok(format!("Service '{}' not found, falling back to node: {}", service_name, target.addr))
        } else {
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

async fn receive_gossip(
    State(cluster): State<SharedClusterManager>,
    Json(payload): Json<serde_json::Value>,
) -> Json<Vec<crate::types::ServiceInfo>> {
    if let (Some(sender_id), Some(members), Some(services)) = (
        payload.get("sender_id").and_then(|v| v.as_str()),
        payload.get("members").and_then(|v| v.as_array()),
        payload.get("services").and_then(|v| v.as_array()),
    ) {
        tracing::info!("Received gossip from {} with {} members and {} services", 
                      sender_id, members.len(), services.len());
        
        let now = crate::cluster::current_timestamp();
        
        if let Ok(members_list) = serde_json::from_value::<Vec<crate::types::NodeInfo>>(
            serde_json::Value::Array(members.clone())
        ) {
            for mut member in members_list {
                // 添加判断：跳过发送者自身的成员信息
                if member.id == sender_id {
                    // 使用 insert 方法更新 DashMap
                    if let Some(mut existing_member) = cluster.members.get_mut(&member.id) {
                        existing_member.last_seen = now;
                        continue
                    }else{
                        member.last_seen = now;
                        cluster.members.insert(member.id.clone(), member);
                    }
                }
            }
        }
        
        if let Ok(services_list) = serde_json::from_value::<Vec<crate::types::ServiceInfo>>(
            serde_json::Value::Array(services.clone())
        ) {
            for mut service in services_list {
                if service.metadata.get("node_id").and_then(|v| Some(v.as_str())) == Some(sender_id) {
                    if let Some(mut existing_service) = cluster.services.get_mut(&service.service_id) {
                        existing_service.last_health_check = now;
                        continue;
                    }else{
                        service.last_health_check = now;
                        cluster.register_service(service);  
                    }
                }
            }
        }
    }
    
    Json(cluster.get_all_services())
}