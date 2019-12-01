use crate::health_check::health::health_check_response::ServingStatus;
use crate::health_check::health::*;
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use tokio::sync::{mpsc, watch};
use tonic::{Code, Request, Response, Status};

pub mod health {
    tonic::include_proto!("grpc.health.v1");
}

type HealthResult<T> = Result<Response<T>, Status>;
type ResponseStream<T> = mpsc::Receiver<Result<T, Status>>;

pub struct HealthCheckService {
    services: RwLock<HashMap<String, ServingStatus>>,
    signaller: Mutex<watch::Sender<Option<(String, ServingStatus)>>>,
    subscriber: watch::Receiver<Option<(String, ServingStatus)>>,
}

impl HealthCheckService {
    pub fn new() -> Self {
        let mut services = HashMap::new();
        services.insert(String::new(), ServingStatus::Serving);
        let (tx, rx) = watch::channel(None);
        HealthCheckService {
            services: RwLock::new(services),
            signaller: Mutex::new(tx),
            subscriber: rx,
        }
    }

    pub fn set_status(&mut self, service_name: &str, status: ServingStatus) -> Result<(), ()> {
        if let Ok(mut writer) = self.services.write() {
            let last = writer.insert(service_name.to_string(), status);
            if last.is_none() || last.unwrap() != status {
                if let Ok(sig) = self.signaller.lock() {
                    sig.broadcast(Some((service_name.to_string(), status)))
                        .map_err(|_| ())?;
                }
            }
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn shutdown(&mut self) -> Result<(), ()> {
        if let Ok(mut writer) = self.services.write() {
            for (name, status) in writer.iter_mut() {
                if *status != ServingStatus::NotServing {
                    *status = ServingStatus::NotServing;
                    
                    if let Ok(sig) = self.signaller.lock() {
                        sig.broadcast(Some((name.to_string(), *status))).map_err(|_| ())?;
                    }
                }
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

    type WatchStream = ResponseStream<HealthCheckResponse>;

    async fn watch(&self, request: Request<HealthCheckRequest>) -> HealthResult<Self::WatchStream> {
        let name = &request.get_ref().service;
        let (mut tx, res_rx) = mpsc::channel(10);
        
        let mut rx = self.subscriber.clone();
        while let Some(value) = rx.recv().await {
            match value {
                Some((changed_name, status)) => {
                    if *name == changed_name {
                        let _ = tx.send(Ok(HealthCheckResponse {
                            status: status as i32,
                        }))
                        .await;
                    }
                },
                _ => {},
            }
        }
        Ok(Response::new(res_rx))
    }
}
