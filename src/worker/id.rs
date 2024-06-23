use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct WorkerId(Uuid);

impl WorkerId {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn get(self) -> Uuid {
        self.0
    }
}

#[cfg(test)]
impl From<u128> for WorkerId {
    fn from(id: u128) -> Self {
        Self::new(Uuid::from_u128(id))
    }
}
