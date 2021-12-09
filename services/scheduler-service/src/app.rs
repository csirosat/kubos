/*
 * Copyright (C) 2019 Kubos Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//!
//! Definitions and functions for dealing with scheduled app execution
//!

use flat_db::DataPoint;
use juniper::GraphQLObject;
use kubos_service::Config;
use log::{debug, error, info, warn};
// use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::process::Command;
use tokio::time::delay_for;

// Configuration used for execution of an app
#[derive(Clone, Debug, GraphQLObject, Serialize, Deserialize)]
pub struct App {
    pub name: String,
    pub args: Option<Vec<String>>,
    pub config: Option<String>,
}

impl App {
    pub async fn execute(&self, id: Option<i32>) {
        info!("Start app {:?} {}", &id, self.name);

        let mut retry = 3;

        loop {
            if retry <= 0 {
                warn!("Retry loop exiting for {:?}", id);
                break;
            }

            let mut cmd = Command::new(self.name.clone());

            if let Some(args) = &self.args {
                // let cmd_args: Vec<String> = args.iter().map(|x| format!("{}", x)).collect();
                cmd.args(args);
            };

            match cmd.status().await {
                Ok(status) => {
                    let code = match status.code() {
                        Some(a) => a,
                        None => {
                            // assume no status means there was an error starting the app...
                            warn!("No status code for {:?}. Assume app failed to start", id);

                            retry -= 1;

                            delay_for(Duration::from_secs(1)).await;
                            continue;
                        }
                    };
                    info!("App {:?} returned code {} {:?}", id, code, status.code());
                    if let Some(id) = id {
                        log_status_code_to_telemetry(id, code).await;
                    }

                    break;
                }
                Err(err) => {
                    error!(
                        "Started app {:?}, but failed to fetch status information: {:?}",
                        id, err
                    );

                    retry -= 1;

                    delay_for(Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }
}

async fn log_status_code_to_telemetry(id: i32, code: i32) {
    let config = match Config::new("telemetry-service") {
        Ok(c) => c,
        Err(_) => {
            debug!("Telemetry service config not found");
            return;
        }
    };

    let port = match config.get("direct_port").map(|p| p.as_integer()).flatten() {
        Some(port) => port as u16,
        None => {
            debug!("Telemetry direct_port not found");
            return;
        }
    };

    if let Ok(mut socket) = UdpSocket::bind("0.0.0.0:0").await {
        let dp = DataPoint::now("app-exit", &format!("{}", id), code.into());
        if let Ok(buf) = serde_cbor::to_vec(&dp) {
            if let Err(e) = socket.send_to(&buf, ("0.0.0.0", port)).await {
                debug!("Couldn't send DataPoint to Telemetry service:{:?}", e);
            }
        } else {
            debug!("Couldn't serialize datapoint");
        }
    } else {
        debug!("Coudln't create new UDP socket");
    }
}
