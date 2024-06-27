use chrono::{DateTime, Utc};
use std::str::FromStr;

#[track_caller]
pub fn dt(s: &str) -> DateTime<Utc> {
    DateTime::from_str(&format!("{}+00:00", s)).unwrap()
}
