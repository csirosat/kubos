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

use crate::udp::*;
use diesel;
use diesel::prelude::*;
use flate2::write::GzEncoder;
use flate2::Compression;
use juniper::{FieldError, FieldResult, Value};
use kubos_service;
use kubos_telemetry_db;
use serde_derive::Serialize;
use serde_json;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use tar;

pub type Context = kubos_service::Context<Subsystem>;

#[derive(Clone)]
pub struct Subsystem {
    pub database: Arc<Mutex<kubos_telemetry_db::Database>>,
}

impl Subsystem {
    pub fn new(database: kubos_telemetry_db::Database, direct_udp: Option<String>) -> Self {
        let db = Arc::new(Mutex::new(database));

        if let Some(udp_url) = direct_udp {
            let udp = DirectUdp::new(db.clone());
            thread::Builder::new()
                .stack_size(16 * 1024)
                .spawn(move || udp.start(udp_url.to_owned()))
                .unwrap();
        }

        Subsystem { database: db }
    }
}

#[derive(Serialize)]
pub struct Entry(kubos_telemetry_db::Entry);

#[juniper::object]
/// A telemetry entry
impl Entry {
    /// Timestamp
    fn timestamp(&self) -> f64 {
        self.0.timestamp as f64
    }
    /// Subsystem name
    fn subsystem(&self) -> &str {
        &self.0.subsystem
    }
    /// Telemetry parameter
    fn parameter(&self) -> &str {
        &self.0.parameter
    }
    /// Telemetry value
    fn value(&self) -> String {
        serde_cbor::from_slice::<serde_cbor::Value>(&self.0.value)
            .ok()
            .and_then(|value| serde_json::to_string(&value).ok())
            .unwrap_or(String::from(""))
    }
}

fn query_db(
    database: &Arc<Mutex<kubos_telemetry_db::Database>>,
    timestamp_ge: Option<f64>,
    timestamp_le: Option<f64>,
    subsystem: Option<String>,
    parameters: Option<Vec<String>>,
    limit: Option<i32>,
) -> FieldResult<Vec<Entry>> {
    use kubos_telemetry_db::telemetry;
    use kubos_telemetry_db::telemetry::dsl;

    let mut query = telemetry::table.into_boxed::<<SqliteConnection as Connection>::Backend>();

    if let Some(sub) = subsystem {
        query = query.filter(dsl::subsystem.eq(sub));
    }

    if let Some(params) = parameters {
        query = query.filter(dsl::parameter.eq_any(params));
    }

    if let Some(time_ge) = timestamp_ge {
        query = query.filter(dsl::timestamp.ge(time_ge));
    }

    if let Some(time_le) = timestamp_le {
        query = query.filter(dsl::timestamp.le(time_le));
    }

    if let Some(l) = limit {
        query = query.limit(l.into());
    }

    query = query.order(dsl::timestamp.desc());

    let entries = query
        .load::<kubos_telemetry_db::Entry>(
            &database
                .lock()
                .or_else(|err| {
                    log::error!("Failed to get lock on database: {:?}", err);
                    Err(err)
                })?
                .connection,
        )
        .or_else(|err| {
            log::error!("Failed to load database entries: {:?}", err);
            Err(err)
        })?;

    let mut g_entries: Vec<Entry> = Vec::new();
    for entry in entries {
        g_entries.push(Entry(entry));
    }

    Ok(g_entries)
}

pub struct QueryRoot;

#[juniper::object(Context = Context)]
impl QueryRoot {
    // Test query to verify service is running without
    // attempting to execute any actual logic
    //
    // {
    //    ping: "pong"
    // }
    /// Test service query
    fn ping() -> FieldResult<String> {
        Ok(String::from("pong"))
    }

    /// Telemetry entries in database
    fn telemetry(
        context: &Context,
        timestamp_ge: Option<f64>,
        timestamp_le: Option<f64>,
        subsystem: Option<String>,
        parameter: Option<String>,
        parameters: Option<Vec<String>>,
        limit: Option<i32>,
    ) -> FieldResult<Vec<Entry>> {
        if parameter.is_some() && parameters.is_some() {
            return Err(FieldError::new(
                "The `parameter` and `parameters` input fields are mutually exclusive",
                Value::null(),
            ));
        }

        if let Some(param) = parameter {
            query_db(
                &context.subsystem().database,
                timestamp_ge,
                timestamp_le,
                subsystem,
                Some(vec![param]),
                limit,
            )
        } else {
            query_db(
                &context.subsystem().database,
                timestamp_ge,
                timestamp_le,
                subsystem,
                parameters,
                limit,
            )
        }
    }

    /// Telemetry entries in database
    fn routed_telemetry(
        context: &Context,
        timestamp_ge: Option<f64>,
        timestamp_le: Option<f64>,
        subsystem: Option<String>,
        parameter: Option<String>,
        parameters: Option<Vec<String>>,
        limit: Option<i32>,
        output: String,
        compress: bool,
    ) -> FieldResult<String> {
        if parameter.is_some() && parameters.is_some() {
            return Err(FieldError::new(
                "The `parameter` and `parameters` input fields are mutually exclusive",
                Value::null(),
            ));
        }

        let entries = if let Some(param) = parameter {
            query_db(
                &context.subsystem().database,
                timestamp_ge,
                timestamp_le,
                subsystem,
                Some(vec![param]),
                limit,
            )?
        } else {
            query_db(
                &context.subsystem().database,
                timestamp_ge,
                timestamp_le,
                subsystem,
                parameters,
                limit,
            )?
        };

        let entries = serde_cbor::to_vec(&entries)?;

        let output_str = output.clone();
        let output_path = Path::new(&output_str);

        let file_name_raw = output_path
            .file_name()
            .ok_or_else(|| FieldError::new("Unable to parse output file name", Value::null()))?;
        let file_name = file_name_raw.to_str().ok_or_else(|| {
            FieldError::new("Unable to parse output file name to string", Value::null())
        })?;

        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }

        {
            let mut output_file = File::create(output_path)?;
            output_file.write_all(&entries)?;
        }

        if compress {
            let tar_path = format!("{}.tar.gz", output_str);
            let tar_file = File::create(&tar_path)?;
            let encoder = GzEncoder::new(tar_file, Compression::default());
            let mut tar = tar::Builder::new(encoder);
            tar.append_file(file_name, &mut File::open(output_path)?)?;
            tar.finish()?;

            fs::remove_file(output_path)?;

            Ok(tar_path)
        } else {
            Ok(output)
        }
    }
}

pub struct MutationRoot;

#[derive(GraphQLObject)]
struct InsertResponse {
    success: bool,
    errors: String,
}

#[derive(GraphQLObject)]
struct DeleteResponse {
    success: bool,
    errors: String,
    entries_deleted: Option<i32>,
}

#[derive(GraphQLInputObject)]
struct InsertEntry {
    timestamp: Option<f64>,
    subsystem: String,
    parameter: String,
    value: String,
}

#[juniper::object(Context = Context)]
impl MutationRoot {
    fn insert(
        context: &Context,
        timestamp: Option<f64>,
        subsystem: String,
        parameter: String,
        value: String,
    ) -> FieldResult<InsertResponse> {
        let value = match serde_json::from_str::<serde_json::Value>(value.as_str())
            .ok()
            .and_then(|value| serde_cbor::to_vec(&value).ok())
        {
            Some(value) => value,
            _ => {
                return Ok(InsertResponse {
                    success: false,
                    errors: String::from("Could not convert betweek json and cbor"),
                });
            }
        };

        let result = match timestamp {
            Some(time) => context
                .subsystem()
                .database
                .lock()
                .or_else(|err| {
                    log::error!("insert - Failed to get lock on database: {:?}", err);
                    Err(err)
                })?
                .insert(time, &subsystem, &parameter, &value),
            None => context
                .subsystem()
                .database
                .lock()
                .or_else(|err| {
                    log::error!("insert - Failed to get lock on database: {:?}", err);
                    Err(err)
                })?
                .insert_systime(&subsystem, &parameter, &value),
        };

        Ok(InsertResponse {
            success: result.is_ok(),
            errors: match result {
                Ok(_) => "".to_owned(),
                Err(err) => format!("{}", err),
            },
        })
    }

    fn insert_bulk(
        context: &Context,
        timestamp: Option<f64>,
        entries: Vec<InsertEntry>,
    ) -> FieldResult<InsertResponse> {
        let time = time::now_utc().to_timespec();
        let systime = time.sec as f64 + (f64::from(time.nsec) / 1_000_000_000.0);

        let mut new_entries: Vec<kubos_telemetry_db::Entry> = Vec::new();
        for entry in entries {
            let ts = entry.timestamp.or(timestamp).unwrap_or(systime);

            let value = match serde_json::from_str::<serde_json::Value>(entry.value.as_str())
                .ok()
                .and_then(|value| serde_cbor::to_vec(&value).ok())
            {
                Some(value) => value,
                _ => {
                    return Ok(InsertResponse {
                        success: false,
                        errors: String::from("Could not convert betweek json and cbor"),
                    });
                }
            };

            new_entries.push(kubos_telemetry_db::Entry {
                timestamp: ts,
                subsystem: entry.subsystem,
                parameter: entry.parameter,
                value,
            });
        }

        let result = context
            .subsystem()
            .database
            .lock()
            .or_else(|err| {
                log::error!("insert_bulk - Failed to get lock on database: {:?}", err);
                Err(err)
            })?
            .insert_bulk(new_entries);

        Ok(InsertResponse {
            success: result.is_ok(),
            errors: match result {
                Ok(_) => "".to_owned(),
                Err(err) => format!("{}", err),
            },
        })
    }

    fn delete(
        context: &Context,
        timestamp_ge: Option<f64>,
        timestamp_le: Option<f64>,
        subsystem: Option<String>,
        parameter: Option<String>,
    ) -> FieldResult<DeleteResponse> {
        use diesel::sqlite::SqliteConnection;
        use kubos_telemetry_db::telemetry;
        use kubos_telemetry_db::telemetry::dsl;

        let mut selection = diesel::delete(telemetry::table)
            .into_boxed::<<SqliteConnection as Connection>::Backend>();

        if let Some(sub) = subsystem {
            selection = selection.filter(dsl::subsystem.eq(sub));
        }

        if let Some(param) = parameter {
            selection = selection.filter(dsl::parameter.eq(param));
        }

        if let Some(time_ge) = timestamp_ge {
            selection = selection.filter(dsl::timestamp.ge(time_ge));
        }

        if let Some(time_le) = timestamp_le {
            selection = selection.filter(dsl::timestamp.le(time_le));
        }

        let result = selection.execute(
            &context
                .subsystem()
                .database
                .lock()
                .or_else(|err| {
                    log::error!("delete - Failed to get lock on database: {:?}", err);
                    Err(err)
                })?
                .connection,
        );

        match result {
            Ok(num) => Ok(DeleteResponse {
                success: true,
                errors: "".to_owned(),
                entries_deleted: Some(num as i32),
            }),
            Err(err) => Ok(DeleteResponse {
                success: false,
                errors: format!("{}", err),
                entries_deleted: None,
            }),
        }
    }
}
