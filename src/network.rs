static NETWORK_INFO: OnceLock<NetworkInfo> = OnceLock::new();

#[derive(Clone)]
pub struct NetworkInfo {
    pub ip: std::net::IpAddr
}

pub async fn get_engine_network_info() -> Result<NetworkInfo, WorkerError> {
    match NETWORK_INFO.get() {
        Some(info) => Ok(info.clone()),
        None => {
            let info = NetworkInfo{
                ip: toolbox::ip::get_public_ip(None)
                    .map_err(|e| WorkerError::CustomError(e.to_string()))?
            };
            
            NETWORK_INFO
                .set(info.clone())
                .map_err(|_e| WorkerError::CustomError("Failed to set network information".to_string()))?;

            Ok(info)
        }
    }
}


#[tokio::test]
async fn test_get_engine_network_info(){
    let net_info = get_engine_network_info().await;
    assert!(net_info.is_ok());
}


use crate::WorkerError;
use std::sync::OnceLock;
