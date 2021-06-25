//
// Copyright (C) 2018 Kubos Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

pub use flat_db::DataPoint;
use flat_db::Database;
use log::{debug, error, info};
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;

pub struct DirectUdp {
    db: Arc<Database>,
}

impl DirectUdp {
    pub fn new(db: Arc<Database>) -> Self {
        DirectUdp { db }
    }

    pub fn start(&self, url: String) {
        let socket = UdpSocket::bind(url.parse::<SocketAddr>().unwrap_or_else(|err| {
            error!(
                "Couldn't start direct UDP connection. Failed to parse {}: {:?}",
                url, err
            );
            panic!()
        }))
        .unwrap_or_else(|err| {
            error!(
                "Couldn't start direct UDP connection. Failed to bind {}: {:?}",
                url, err
            );
            panic!()
        });

        info!("Direct UDP listening on: {}", socket.local_addr().unwrap());

        loop {
            // Wait for an incoming message
            let mut buf = [0; 4096];
            let (size, _peer) = socket
                .recv_from(&mut buf)
                .map_err(|err| format!("Failed to receive a message: {}", err))
                .unwrap();

            debug!("Received Telemetry");

            if let Ok(val) = serde_cbor::from_slice::<DataPoint>(&buf[0..(size)]) {
                self.db.insert(&[val]).unwrap();
            } else if let Ok(vec) = serde_cbor::from_slice::<Vec<DataPoint>>(&buf[0..(size)]) {
                self.db.insert(vec).unwrap();
            } else {
                error!(
                    "Couldn't deserialize JSON object or object array from {:?}",
                    String::from_utf8_lossy(&buf[0..(size)].to_vec())
                );
            }
        }
    }
}
