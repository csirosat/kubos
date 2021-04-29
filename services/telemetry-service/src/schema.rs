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

use std::{
    fs::read_dir,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use crate::udp::*;
use flat_db::Database;
use git_version::git_version;
use juniper::{FieldError, FieldResult, GraphQLObject, Value};
use kubos_service;

pub type Context = kubos_service::Context<Subsystem>;

#[derive(Clone)]
pub struct Subsystem {
    pub database: Arc<Database>,
    pub db_path: PathBuf,
}

impl Subsystem {
    pub fn new(database: Database, db_path: &Path, direct_udp: Option<String>) -> Self {
        let db = Arc::new(database);
        let db_path = db_path.to_owned();

        if let Some(udp_url) = direct_udp {
            let udp = DirectUdp::new(db.clone());
            thread::Builder::new()
                .stack_size(16 * 1024)
                .spawn(move || udp.start(udp_url.to_owned()))
                .unwrap();
        }

        Subsystem {
            database: db,
            db_path,
        }
    }
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

    fn files(context: &Context) -> FieldResult<Vec<String>> {
        let db_path = context.subsystem().db_path.to_owned();
        let dir = db_path.parent().ok_or(FieldError::new(
            "Path does not have a parent",
            Value::null(),
        ))?;

        Ok(read_dir(&dir)
            .map_err(|e| {
                FieldError::new(format!("Could not read DB directory:{}", e), Value::null())
            })?
            .filter_map(|dirent| dirent.ok())
            .filter_map(|dirent| match dirent.file_type() {
                Ok(ftype) if ftype.is_file() => Some(dirent),
                _ => None,
            })
            .map(|file| file.file_name())
            .filter_map(|file_name| file_name.to_str().as_ref().map(|s| s.to_string()))
            .map(|s| {
                let mut dir = dir.to_path_buf();
                dir.push(s);
                dir
            })
            .filter(|f| f != &db_path)
            .filter_map(|file_name| file_name.to_str().as_ref().map(|s| s.to_string()))
            .collect())
    }

    fn git() -> ServiceGitHash {
        ServiceGitHash {
            name: "telemetry-service",
            hash: git_version!(),
        }
    }
}

#[derive(GraphQLObject)]
pub struct ServiceGitHash {
    name: &'static str,
    hash: &'static str,
}

pub struct MutationRoot;

#[juniper::object(Context = Context)]
impl MutationRoot {}
