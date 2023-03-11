use crate::canisters::{install, CanisterInstallMode};
use candid::Principal;
use ic_cdk::api::call::call_raw;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{config::CONFIG, Logger};

#[derive(Default, Serialize, Deserialize)]
pub struct Storage {
    pub buckets: BTreeMap<Principal, u64>,
}

const BUCKET_WASM_GZ: &[u8] =
    include_bytes!("../../../target/wasm32-unknown-unknown/release/bucket.wasm.gz");

impl Storage {
    async fn allocate_space(
        &mut self,
        max_bucket_size: u64,
        logger: &mut Logger,
    ) -> Result<Principal, String> {
        if let Some((id, _)) = self
            .buckets
            .iter()
            .find(|(_, size)| **size < max_bucket_size)
        {
            return Ok(*id);
        }
        let id = crate::canisters::new().await?;
        logger.info(format!("New bucket {} created.", id));
        self.buckets.insert(id, 0);
        install(id, BUCKET_WASM_GZ, CanisterInstallMode::Install).await?;
        logger.info(format!("WASM installed to bucket {}.", id));
        Ok(id)
    }

    #[allow(dead_code)]
    async fn upgrade_buckets(&self) -> Result<(), String> {
        for id in self.buckets.keys() {
            install(*id, BUCKET_WASM_GZ, CanisterInstallMode::Upgrade).await?;
        }
        Ok(())
    }

    pub async fn write_to_bucket(
        &mut self,
        logger: &mut Logger,
        blob: &[u8],
    ) -> Result<(Principal, u64), String> {
        let id = self.allocate_space(CONFIG.max_bucket_size, logger).await?;
        let response = call_raw(id, "write", blob, 0)
            .await
            .map_err(|err| format!("couldn't call write on a bucket: {:?}", err))?;
        let mut offset_bytes: [u8; 8] = Default::default();
        offset_bytes.copy_from_slice(&response);
        let offset = u64::from_be_bytes(offset_bytes);
        self.buckets.insert(id, offset + blob.len() as u64);
        Ok((id, offset))
    }
}
