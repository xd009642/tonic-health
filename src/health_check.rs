use crate::health_check::health::health_check_response::ServingStatus;
use crate::health_check::health::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::{mpsc, watch};
use tonic::{Code, Request, Response, Status};

pub mod health {
    tonic::include_proto!("grpc.health.v1");
}

#[derive(Debug, Clone)]
enum StatusChange {
    All,
    Specific(String),
}

type HealthResult<T> = Result<Response<T>, Status>;
type ResponseStream<T> = mpsc::Receiver<Result<T, Status>>;

pub struct HealthCheckService {
    services: Arc<RwLock<HashMap<String, ServingStatus>>>,
    signaller: Mutex<watch::Sender<Option<StatusChange>>>,
    subscriber: watch::Receiver<Option<StatusChange>>,
}

impl HealthCheckService {
    pub fn new() -> Self {
        let mut services = HashMap::new();
        services.insert(String::new(), ServingStatus::Serving);
        let (tx, rx) = watch::channel(None);
        HealthCheckService {
            services: Arc::new(RwLock::new(services)),
            signaller: Mutex::new(tx),
            subscriber: rx,
        }
    }

    pub fn set_status(&mut self, service_name: &str, status: ServingStatus) -> Result<(), ()> {
        if let Ok(mut writer) = self.services.clone().write() {
            writer.insert(service_name.to_string(), status);
            if let Ok(sig) = self.signaller.lock() {
                sig.broadcast(Some(StatusChange::Specific(service_name.to_string())))
                    .map_err(|_| ())?;
                Ok(())
            } else {
                Err(())
            }
        } else {
            Err(())
        }
    }

    pub fn shutdown(&mut self) -> Result<(), ()> {
        if let Ok(mut writer) = self.services.clone().write() {
            for status in writer.values_mut() {
                *status = ServingStatus::NotServing;
            }
            if let Ok(sig) = self.signaller.lock() {
                sig.broadcast(Some(StatusChange::All)).map_err(|_| ())?;
                Ok(())
            } else {
                Err(())
            }
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
        if let Ok(services) = self.services.clone().read() {
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
        {
            let service_lock = self.services.clone();
            let services = service_lock
                .read()
                .map_err(|_| Status::new(Code::Internal, "Unable to check status of service"))?;
            // Do an initial check to make sure it exists
            if services.get(name).is_none() {
                return Err(Status::new(Code::NotFound, ""));
            }
        }
        let (mut tx, res_rx) = mpsc::channel(10);
        let mut rx = self.subscriber.clone();
        while let Some(value) = rx.recv().await {
            let service_lock = self.services.clone();
            match value {
                Some(StatusChange::Specific(changed_name)) => {
                    if *name == changed_name {
                        if let Ok(services) = service_lock.read() {
                            let status = services.get(name).unwrap();
                            tx.send(Ok(HealthCheckResponse {
                                status: (*status) as i32,
                            }))
                            .await;
                        }
                    }
                }
                Some(StatusChange::All) => {
                    // send it
                    if let Ok(services) = service_lock.read() {
                        let status = services.get(name).unwrap();
                        tx.send(Ok(HealthCheckResponse {
                            status: (*status) as i32,
                        }))
                        .await;
                    }
                }
                _ => {}
            }
        }
        Ok(Response::new(res_rx))
    }
}
