use crate::canisters::{install, CanisterInstallMode};
use candid::Principal;
use ic_cdk::api::{
    call::call_raw,
    stable::{stable64_grow, stable64_read, stable64_size, stable64_write},
};
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

pub fn heap_to_stable(state: &super::State) {
    let buffer: Vec<u8> = serde_cbor::to_vec(state).expect("couldn't serialize the state");
    let len = 16 + buffer.len() as u64;
    if len > (stable64_size() << 16) && stable64_grow((len >> 16) + 1).is_err() {
        panic!("Couldn't grow memory");
    }
    stable64_write(16, &buffer);
    stable64_write(0, &16_u64.to_be_bytes());
    stable64_write(8, &(buffer.len() as u64).to_be_bytes());
}

pub fn heap_address() -> (u64, u64) {
    let mut offset_bytes: [u8; 8] = Default::default();
    stable64_read(0, &mut offset_bytes);
    let offset = u64::from_be_bytes(offset_bytes);
    let mut len_bytes: [u8; 8] = Default::default();
    stable64_read(8, &mut len_bytes);
    let len = u64::from_be_bytes(len_bytes);
    (offset, len)
}

pub fn stable_to_heap() -> super::State {
    let (offset, len) = heap_address();
    ic_cdk::println!(
        "Reading heap from coordinates: {:?}, stable memory size: {}",
        (offset, len),
        (stable64_size() << 16)
    );

    let mut bytes = Vec::with_capacity(len as usize);
    bytes.spare_capacity_mut();
    unsafe {
        bytes.set_len(len as usize);
    }

    // Restore heap
    stable64_read(offset, &mut bytes);
    serde_cbor::from_slice(&bytes).expect("couldn't deserialize")
}
