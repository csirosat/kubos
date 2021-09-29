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
//! Definitions and functions for dealing with tasks & scheduling
//!

use crate::app::App;
use crate::error::SchedulerError;
use chrono::offset::TimeZone;
use chrono::Duration;
use chrono::NaiveDateTime;
use chrono::Utc;
use clock_timer::RealTimer;
use juniper::GraphQLObject;
use log::error;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::select;
use tokio::sync::broadcast::Receiver;

// Configuration used to schedule app execution
#[derive(Clone, Debug, GraphQLObject, Serialize, Deserialize)]
pub struct Task {
    pub id: Option<i32>,
    // Start delay specified in Xh Ym Zs format
    // Used by init and recurring tasks
    pub delay: Option<String>,
    // Start time specified in yyyy-mm-dd hh:mm:ss format
    // Used by onetime tasks
    pub time: Option<String>,
    // Period of recurrence specified in Xh Ym Zs format
    // Used by recurring tasks
    pub period: Option<String>,
    // Details of the app to be executed
    pub app: App,
}

impl Task {
    fn description(&self) -> String {
        if let Some(id) = self.id {
            format!("{}: {}", id, self.app.name)
        } else {
            format!("{}", self.app.name)
        }
    }

    // Parse timer delay duration from either delay or time fields
    pub fn get_absolute(&self) -> Result<NaiveDateTime, SchedulerError> {
        if self.delay.is_some() && self.time.is_some() {
            return Err(SchedulerError::TaskParseError {
                err: "Both delay and time defined".to_owned(),
                description: self.description(),
            });
        }
        if let Some(delay) = &self.delay {
            Ok(parse_hms_field(delay.to_owned()).map(|d| Utc::now().naive_utc() + d)?)
        } else if let Some(time) = &self.time {
            let run_time = Utc
                .datetime_from_str(&time, "%Y-%m-%d %H:%M:%S")
                .map_err(|e| SchedulerError::TaskParseError {
                    err: format!("Failed to parse time field '{}': {}", time, e),
                    description: self.description(),
                })?;
            let now = chrono::Utc::now();

            if run_time < now {
                Err(SchedulerError::TaskTimeError {
                    err: format!("Task scheduled for past time: {}", time),
                    description: self.app.name.to_owned(),
                })
            } else if (run_time - now) > chrono::Duration::days(90) {
                Err(SchedulerError::TaskTimeError {
                    err: format!("Task scheduled beyond 90 days in the future: {}", time),
                    description: self.description(),
                })
            } else {
                Ok(run_time.naive_utc())
            }
        } else {
            Err(SchedulerError::TaskParseError {
                err: "No delay or time defined".to_owned(),
                description: self.description(),
            })
        }
    }

    pub fn get_period(&self) -> Result<Option<Duration>, SchedulerError> {
        if let Some(period) = &self.period {
            Ok(Some(parse_hms_field(period.to_owned())?))
        } else {
            Ok(None)
        }
    }

    pub async fn schedule(self: Arc<Self>, real_timer: RealTimer, mut stop: Receiver<()>) {
        let name = self.app.name.to_owned();
        let when = match self.get_absolute() {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "Failed to parse time specification for task {:?} '{}': {}",
                    self.id, name, e
                );
                return;
            }
        };

        let period = self.get_period();
        let app = self.app.clone();

        match period {
            Ok(Some(period)) => {
                let mut interval = real_timer.interval_at(when, period);
                loop {
                    let task = async {
                        interval.tick().await;
                        app.execute(self.id).await;
                    };

                    select! {
                        _ = task => {}
                        _ = stop.recv() => {
                            return;
                        }
                    };
                }
            }
            _ => {
                let task = async {
                    real_timer.at(when).await;
                    app.execute(self.id).await;
                };

                select! {
                    _ = task => {}
                    _ = stop.recv() => {
                        return;
                    }
                };
            }
        }
    }
}

fn parse_hms_field(field: String) -> Result<Duration, SchedulerError> {
    let field_parts: Vec<String> = field.split(' ').map(|s| s.to_owned()).collect();
    let mut duration: i64 = 0;
    if field_parts.is_empty() {
        return Err(SchedulerError::HmsParseError {
            err: "No parts found".to_owned(),
            field: field.to_owned(),
        });
    }
    for mut part in field_parts {
        let unit: Option<char> = part.pop();
        let num: Result<u64, _> = part.parse();
        if let Ok(num) = num {
            match unit {
                Some('s') => {
                    duration += num as i64;
                }
                Some('m') => {
                    duration += num as i64 * 60;
                }
                Some('h') => {
                    duration += num as i64 * 60 * 60;
                }
                _ => {
                    return Err(SchedulerError::HmsParseError {
                        err: "Found invalid unit".to_owned(),
                        field: field.to_owned(),
                    });
                }
            }
        } else {
            return Err(SchedulerError::HmsParseError {
                err: "Failed to parse number".to_owned(),
                field: field.to_owned(),
            });
        }
    }
    Ok(Duration::seconds(duration))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_seconds() {
        assert_eq!(
            parse_hms_field("21s".to_owned()),
            Ok(Duration::from_secs(21))
        );
    }

    #[test]
    fn test_parse_minutes() {
        assert_eq!(
            parse_hms_field("3m".to_owned()),
            Ok(Duration::from_secs(180))
        );
    }

    #[test]
    fn test_parse_hours() {
        assert_eq!(
            parse_hms_field("2h".to_owned()),
            Ok(Duration::from_secs(7200))
        );
    }

    #[test]
    fn test_parse_minutes_seconds() {
        assert_eq!(
            parse_hms_field("1m 1s".to_owned()),
            Ok(Duration::from_secs(61))
        );
    }

    #[test]
    fn test_parse_hours_minutes() {
        assert_eq!(
            parse_hms_field("3h 10m".to_owned()),
            Ok(Duration::from_secs(11400))
        );
    }

    #[test]
    fn test_parse_hours_seconds() {
        assert_eq!(
            parse_hms_field("5h 44s".to_owned()),
            Ok(Duration::from_secs(18044))
        );
    }

    #[test]
    fn test_parse_hours_minutes_seconds() {
        assert_eq!(
            parse_hms_field("2h 2m 2s".to_owned()),
            Ok(Duration::from_secs(7322))
        );
    }
}
