use crate::health_check::health::health_check_response::ServingStatus;
use crate::health_check::health::*;
use futures::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::RwLock;
use tonic::{Code, Request, Response, Status};

pub mod health {
    tonic::include_proto!("grpc.health.v1");
}

type HealthResult<T> = Result<Response<T>, Status>;
type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<HealthCheckResponse, Status>> + Send + Sync>>;

pub struct HealthCheckService {
    services: RwLock<HashMap<String, ServingStatus>>,
}

impl HealthCheckService {
    pub fn new() -> Self {
        let mut services = HashMap::new();
        services.insert(String::new(), ServingStatus::Serving);
        HealthCheckService {
            services: RwLock::new(services),
        }
    }

    pub fn set_status(&mut self, service_name: &str, status: ServingStatus) -> Result<(), ()> {
        if let Ok(mut writer) = self.services.write() {
            writer.insert(service_name.to_string(), status);
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn shutdown(&mut self) -> Result<(), ()> {
        if let Ok(mut writer) = self.services.write() {
            for status in writer.values_mut() {
                *status = ServingStatus::NotServing;
            }
            Ok(())
        } else {
            Err(())
        }
    }
}

#[tonic::async_trait]
impl server::Health for HealthCheckService {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> HealthResult<HealthCheckResponse> {
        if let Ok(services) = self.services.read() {
            match services.get(&request.get_ref().service) {
                Some(status) => {
                    let response = Response::new(HealthCheckResponse {
                        status: (*status) as i32,
                    });
                    Ok(response)
                }
                None => Err(Status::new(Code::NotFound, "")),
            }
        } else {
            Err(Status::new(
                Code::Internal,
                "Unable to check status of services",
            ))
        }
    }

    type WatchStream = ResponseStream;

    async fn watch(&self, request: Request<HealthCheckRequest>) -> HealthResult<Self::WatchStream> {
        unimplemented!();
    }
}
