mod types;
mod cluster;
mod api;

use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{CorsLayer, Any};
use crate::api::create_api_router;
use crate::cluster::ClusterManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    
    let base_port = if args.len() > 1 {
        args[1].parse::<u16>().unwrap_or(3000)
    } else {
        3000
    };
    
    let addr: SocketAddr = format!("127.0.0.1:{}", base_port).parse()?;
    
    let seed_nodes: Vec<SocketAddr> = if base_port == 3000 {
        Vec::new()
    } else {
        vec!["127.0.0.1:3000".parse().unwrap()]
    };

    tracing::info!("Starting node on {} with seeds: {:?}", addr, seed_nodes);

    let cluster_manager = Arc::new(ClusterManager::new(addr, seed_nodes));
    
    if let Err(e) = cluster_manager.join_cluster().await {
        tracing::warn!("Failed to join cluster: {}, starting as standalone", e);
    } else {
        tracing::info!("Successfully joined cluster. Current members: {}", 
        cluster_manager.get_all_members().len());
    }
    
    cluster_manager.start_gossip().await;
    cluster_manager.start_health_check().await;  // 启动健康检查

    let app = create_api_router(cluster_manager.clone()).layer( // 添加CORS中间件层
            CorsLayer::new()
                .allow_origin(Any)    // 允许所有来源
                .allow_headers(Any)   // 允许所有头部
                .allow_methods(Any), // 允许所有方法
        );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("服务器运行在 {}", addr);
    tracing::info!("可用端点:");
    tracing::info!("  GET  /health - 健康检查");
    tracing::info!("  GET  /cluster/state - 集群状态");
    tracing::info!("  GET  /cluster/members - 集群成员");
    tracing::info!("  GET  /cluster/services - 所有服务");
    tracing::info!("  POST /cluster/services - 注册服务");
    tracing::info!("  DELETE /cluster/services/{{service_id}} - 注销服务");
    tracing::info!("  GET  /cluster/services/healthy - 健康服务");
    tracing::info!("  GET  /cluster/services/available - 可用服务");
    tracing::info!("  GET  /cluster/services/{{service_name}}/endpoint - 获取服务端点");
    tracing::info!("  GET  /api/{{service_name}} - 代理请求");
    tracing::info!("  POST /gossip/members - Gossip通信端点");
    
    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}