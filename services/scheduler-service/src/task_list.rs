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
//! Definitions and functions concerning the manipulation of task lists
//!

use crate::error::SchedulerError;
use crate::scheduler::SchedulerHandle;
use crate::task::Task;
use chrono::{DateTime, Utc};
use clock_timer::RealTimer;
use juniper::GraphQLObject;
use log::info;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::broadcast;

// Task list's contents
#[derive(Debug, GraphQLObject, Serialize, Deserialize)]
pub struct ListContents {
    pub tasks: Vec<Task>,
}

// Task list's metadata
#[derive(Debug, GraphQLObject, Serialize)]
pub struct TaskList {
    pub tasks: Vec<Task>,
    pub path: String,
    pub filename: String,
    pub time_imported: String,
}

impl TaskList {
    pub fn from_path(path_obj: &Path) -> Result<TaskList, SchedulerError> {
        let path = path_obj
            .to_str()
            .map(|path| path.to_owned())
            .ok_or_else(|| SchedulerError::TaskListParseError {
                err: "Failed to convert path".to_owned(),
                name: "".to_owned(),
            })?;

        let filename = path_obj
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| SchedulerError::TaskListParseError {
                err: "Failed to read task list name".to_owned(),
                name: path.to_owned(),
            })?
            .to_owned();

        let data = path_obj
            .metadata()
            .map_err(|e| SchedulerError::TaskListParseError {
                err: format!("Failed to read file metadata: {}", e),
                name: filename.to_owned(),
            })?;

        let time_imported: DateTime<Utc> = data
            .modified()
            .map_err(|e| SchedulerError::TaskListParseError {
                err: format!("Failed to get modified time: {}", e),
                name: filename.to_owned(),
            })?
            .into();
        let time_imported = time_imported.format("%Y-%m-%d %H:%M:%S").to_string();

        let list_contents =
            fs::read_to_string(&path_obj).map_err(|e| SchedulerError::TaskListParseError {
                err: format!("Failed to read task list: {}", e),
                name: filename.to_owned(),
            })?;

        let list_contents: ListContents = serde_json::from_str(&list_contents).map_err(|e| {
            SchedulerError::TaskListParseError {
                err: format!("Failed to parse json: {}", e),
                name: filename.to_owned(),
            }
        })?;

        let tasks = list_contents.tasks;

        Ok(TaskList {
            path,
            filename,
            tasks,
            time_imported,
        })
    }

    // Schedules the tasks contained in this task list
    pub fn schedule_tasks(
        &self,
        real_timer: RealTimer,
        tokio_handle: Handle,
    ) -> Result<SchedulerHandle, SchedulerError> {
        let (stopper, _) = broadcast::channel::<()>(1);
        let tasks: Vec<Arc<Task>> = self.tasks.iter().map(|t| Arc::new(t.to_owned())).collect();

        for task in tasks {
            info!("Scheduling task '{}'", &task.app.name);
            tokio_handle.spawn(task.schedule(real_timer.clone(), stopper.subscribe()));
        }

        Ok(SchedulerHandle { stopper })
    }
}

// Copy a task list into a mode directory
pub fn import_task_list(
    scheduler_dir: &str,
    raw_name: &str,
    path: &str,
    raw_mode: &str,
) -> Result<(), SchedulerError> {
    let name = raw_name.to_lowercase();
    let mode = raw_mode.to_lowercase();
    info!(
        "Importing task list '{}': {} into mode '{}'",
        name, path, mode
    );
    let schedule_dest = format!("{}/{}/{}.json", scheduler_dir, mode, name);

    if !Path::new(&format!("{}/{}", scheduler_dir, mode)).is_dir() {
        return Err(SchedulerError::ImportError {
            err: "Mode not found".to_owned(),
            name: name.to_owned(),
        });
    }

    fs::copy(path, &schedule_dest).map_err(|e| SchedulerError::ImportError {
        err: e.to_string(),
        name: name.to_owned(),
    })?;

    if let Err(e) = validate_task_list(&schedule_dest) {
        let _ = fs::remove_file(&schedule_dest);
        return Err(e);
    }

    Ok(())
}

// Import raw json into a task list into a mode directory
pub fn import_raw_task_list(
    scheduler_dir: &str,
    name: &str,
    mode: &str,
    json: &str,
) -> Result<(), SchedulerError> {
    let name = name.to_lowercase();
    let mode = mode.to_lowercase();
    info!("Importing raw task list '{}' into mode '{}'", name, mode);
    let schedule_dest = format!("{}/{}/{}.json", scheduler_dir, mode, name);

    if !Path::new(&format!("{}/{}", scheduler_dir, mode)).is_dir() {
        return Err(SchedulerError::ImportError {
            err: "Mode not found".to_owned(),
            name: name.to_owned(),
        });
    }

    let mut task_list =
        fs::File::create(&schedule_dest).map_err(|e| SchedulerError::ImportError {
            err: e.to_string(),
            name: name.to_owned(),
        })?;
    task_list
        .write_all(json.as_bytes())
        .map_err(|e| SchedulerError::ImportError {
            err: e.to_string(),
            name: name.to_owned(),
        })?;
    task_list
        .sync_all()
        .map_err(|e| SchedulerError::ImportError {
            err: e.to_string(),
            name: name.to_owned(),
        })?;

    if let Err(e) = validate_task_list(&schedule_dest) {
        let _ = fs::remove_file(&schedule_dest);
        return Err(e);
    }

    Ok(())
}

// Remove an existing task list from the mode's directory
pub fn remove_task_list(scheduler_dir: &str, name: &str, mode: &str) -> Result<(), SchedulerError> {
    let name = name.to_lowercase();
    let mode = mode.to_lowercase();
    info!("Removing task list '{}'", name);
    let sched_path = format!("{}/{}/{}.json", scheduler_dir, mode, name);

    if !Path::new(&format!("{}/{}", scheduler_dir, mode)).is_dir() {
        return Err(SchedulerError::RemoveError {
            err: "Mode not found".to_owned(),
            name: name.to_owned(),
        });
    }

    if !Path::new(&sched_path).is_file() {
        return Err(SchedulerError::RemoveError {
            err: "File not found".to_owned(),
            name: name.to_owned(),
        });
    }

    fs::remove_file(&sched_path).map_err(|e| SchedulerError::RemoveError {
        err: e.to_string(),
        name: name.to_owned(),
    })?;

    info!("Removed task list '{}'", name);
    Ok(())
}

// Retrieve list of the task lists in a mode's directory
pub fn get_mode_task_lists(mode_path: &str) -> Result<Vec<TaskList>, SchedulerError> {
    let mut schedules = vec![];

    let mut files_list: Vec<PathBuf> = fs::read_dir(mode_path)
        .map_err(|e| SchedulerError::GenericError {
            err: format!("Failed to read mode dir: {}", e),
        })?
        // Filter out invalid entries
        .filter_map(|x| x.ok())
        // Convert DirEntry -> PathBuf
        .map(|entry| entry.path())
        // Filter out non-directories
        .filter(|entry| entry.is_file())
        .collect();
    // Sort into predictable order
    files_list.sort();

    for path in files_list {
        schedules.push(TaskList::from_path(&path)?);
    }

    Ok(schedules)
}

// Validate the format and content of a task list
pub fn validate_task_list(path: &str) -> Result<(), SchedulerError> {
    let task_path = Path::new(path);
    let task_list = TaskList::from_path(task_path)?;
    for task in task_list.tasks {
        let _ = match task.get_absolute() {
            Ok(_) => Ok(()),
            Err(SchedulerError::TaskTimeError { .. }) => Ok(()),
            Err(e) => Err(e),
        }?;
        let _ = task.get_period()?;
    }
    Ok(())
}
