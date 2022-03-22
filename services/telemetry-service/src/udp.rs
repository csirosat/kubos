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

use chrono::{DateTime, Utc};
pub use flat_db::DataPoint;
use flat_db::{Database, DbError};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::convert::TryInto;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;

use deku::DekuContainerRead;
use live_telemetry_protocol::{Point, PointType, Points, TelemetryMessage};

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

        'main_loop: loop {
            // Wait for an incoming message
            let mut buf = vec![0; 4096];
            let (size, _peer) = socket
                .recv_from(&mut buf)
                .map_err(|err| format!("Failed to receive a message: {}", err))
                .unwrap();

            debug!("Received Telemetry");

            let mut inp = (&buf[0..size], 0);
            'tm: loop {
                if inp.0.len() == 0 {
                    continue 'main_loop;
                }

                let msg = match TelemetryMessage::from_bytes(inp) {
                    Ok((next, d)) => {
                        inp = next;
                        d
                    }
                    Err(e) => {
                        debug!("Telemetry not in Telemetry Message Format: {:?}", e);
                        break 'tm;
                    }
                };

                match msg {
                    TelemetryMessage::Points(points) => match self.db.insert(points) {
                        Ok(_) => {}
                        Err(DbError::IOError { error }) => {
                            error!("DB IO Error: {:?}", error);
                            break 'main_loop;
                        }
                        Err(e) => {
                            warn!("DB Insert Error: {:?}", e);
                        }
                    },
                    m => {
                        warn!("Unknown TelemetryMessage: {:?}", m);
                    }
                }
            }

            let dps = if let Ok(val) = serde_cbor::from_slice::<DataPoint>(&buf[0..size]) {
                vec![val]
            } else if let Ok(vec) = serde_cbor::from_slice::<Vec<DataPoint>>(&buf[0..size]) {
                vec
            } else {
                error!(
                    "Couldn't deserialize JSON object or object array from {:?}",
                    String::from_utf8_lossy(&buf[0..size].to_vec())
                );
                continue;
            };

            let dps: Vec<(DateTime<Utc>, u16, PointType)> = dps
                .into_iter()
                .filter_map(|dp| {
                    let DataPoint(timestamp, subsystem, metric, value) = dp;
                    telemetry_map::get_id((&subsystem, &metric)).map(|id| (timestamp, id, value))
                })
                .filter_map(|(ts, id, value)| value.try_into().ok().map(|value| (ts, id, value)))
                .collect();

            let mut time_bins: HashMap<DateTime<Utc>, HashMap<u16, PointType>> = HashMap::new();

            for (ts, id, value) in dps {
                let bin = time_bins.entry(ts).or_default();
                bin.entry(id).or_insert(value);
            }

            let points_bin: Vec<Points> = time_bins
                .drain()
                .map(|(ts, mut bin)| {
                    let mut points = Points::new(ts);

                    points.points = bin
                        .drain()
                        .map(|(id, value)| Point::new_with_value(id, value))
                        .collect();

                    points
                })
                .collect();

            for p in points_bin {
                match self.db.insert(p) {
                    Ok(_) => {}
                    Err(DbError::IOError { error }) => {
                        error!("DB IO Error: {:?}", error);
                        break 'main_loop;
                    }
                    Err(e) => {
                        warn!("DB Insert Error: {:?}", e);
                    }
                }
            }
        }
    }
}
