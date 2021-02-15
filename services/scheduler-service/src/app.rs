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

use crate::schema::GenericResponse;
use juniper::GraphQLObject;
use log::{error, info};
// use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

#[derive(Debug, Deserialize)]
pub struct StartAppResponse {
    #[serde(rename = "startApp")]
    pub start_app: GenericResponse,
}

#[derive(Debug, Deserialize)]
pub struct StartAppGraphQL {
    pub data: StartAppResponse,
}

// // Helper function for sending query to app service
// pub fn service_query(query: &str, hosturl: &str) -> Result<StartAppGraphQL, SchedulerError> {
//     // The app service will wait 300ms to see if an app completes before returning its response to us
//     let client = Client::builder()
//         .timeout(Duration::from_millis(350))
//         .build()
//         .map_err(|e| SchedulerError::QueryError { err: e.to_string() })?;
//     let mut map = HashMap::new();
//     map.insert("query", query);
//     let url = format!("http://{}", hosturl);

//     let res = client
//         .post(&url)
//         .json(&map)
//         .send()
//         .map_err(|e| SchedulerError::QueryError { err: e.to_string() })?;

//     Ok(from_str(
//         &res.text()
//             .map_err(|e| SchedulerError::QueryError { err: e.to_string() })?,
//     )
//     .map_err(|e| SchedulerError::QueryError { err: e.to_string() })?)
// }

// Configuration used for execution of an app
#[derive(Clone, Debug, GraphQLObject, Serialize, Deserialize)]
pub struct App {
    pub name: String,
    pub args: Option<Vec<String>>,
    pub config: Option<String>,
}

impl App {
    pub async fn execute(&self, _service_url: &str) {
        info!("Start app {}", self.name);

        let mut cmd = Command::new(self.name.clone());

        if let Some(args) = &self.args {
            // let cmd_args: Vec<String> = args.iter().map(|x| format!("{}", x)).collect();
            cmd.args(args);
        };

        match cmd.status().await {
            Ok(status) => {
                if !status.success() {
                    let a = match status.code() {
                        Some(a) => a,
                        None => -1,
                    };
                    info!("error: App returned {}", a);
                } else {
                    info!("Exited healthy");
                }
            }
            Err(err) => error!(
                "Started app, but failed to fetch status information: {:?}",
                err
            ),
        }
    }
}
