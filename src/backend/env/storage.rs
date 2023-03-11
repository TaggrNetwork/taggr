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

struct Allocator {
    segments: BTreeMap<u64, u64>,
    mem_grow: Box<dyn FnMut(u64)>,
    mem_size: Box<dyn Fn() -> u64>,
}

impl Allocator {
    fn init(&mut self, offset: u64) {}

    fn alloc(&mut self, n: u64) -> u64 {
        0
    }

    fn free(&mut self, offset: u64) {}

    fn segs(&self) -> usize {
        self.segments.len()
    }

    fn seg(&self, start: u64) -> u64 {
        self.segments.get(&start).copied().expect("no segment")
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn test_allocator() {
        static mut MEM_END: u64 = 0;
        let mem_grow = |n| unsafe {
            MEM_END += n;
        };
        fn mem_end() -> u64 {
            unsafe { MEM_END }
        }
        let mut a = Allocator {
            segments: Default::default(),
            mem_grow: Box::new(mem_grow),
            mem_size: Box::new(mem_end),
        };

        assert_eq!(mem_end(), 0);

        a.init(16);
        // |oooooooooooooooo|...
        assert_eq!(mem_end(), 16);
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(16), 16);

        assert_eq!(a.alloc(8), 16);
        // |oooooooooooooooo|xxxxxxxx|...
        assert_eq!(mem_end(), 16 + 8);

        assert_eq!(a.alloc(4), 16 + 8);
        // |oooooooooooooooo|xxxxxxxx|xxxx|...
        assert_eq!(mem_end(), 16 + 8 + 4);

        assert_eq!(a.alloc(4), 16 + 8 + 4);
        // |oooooooooooooooo|xxxxxxxx|xxxx|xxxx|...
        assert_eq!(mem_end(), 16 + 8 + 4 + 4);
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(32), 32);

        a.free(16 + 8);
        // |oooooooooooooooo|xxxxxxxx|....|xxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16 + 8), 16 + 8 + 4);
        assert_eq!(a.seg(32), 32);

        assert_eq!(a.alloc(4), 16 + 8);
        // |oooooooooooooooo|xxxxxxxx|xxxx|xxxx|...
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(32), 32);

        a.free(16);
        // |oooooooooooooooo|........|xxxx|xxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16), 16 + 8);
        a.free(16 + 8);
        // |oooooooooooooooo|............|xxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16), 16 + 8 + 4);
        assert_eq!(a.seg(32), 32);

        assert_eq!(a.alloc(10), 16);
        // |oooooooooooooooo|xxxxxxxxxx|..|xxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16 + 10), 16 + 10 + 2);
        assert_eq!(a.seg(32), 32);

        assert_eq!(a.alloc(32), 32);
        // |oooooooooooooooo|xxxxxxxxxx|..|xxxx|xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx|
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(16 + 10), 16 + 10 + 2);

        a.free(32);
        // |oooooooooooooooo|xxxxxxxxxx|..|xxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16 + 10), 16 + 10 + 2);
        assert_eq!(a.seg(32), 32);

        assert_eq!(a.alloc(16), 32);
        // |oooooooooooooooo|xxxxxxxxxx|..|xxxx|xxxxxxxxxxxxxxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16 + 10), 16 + 10 + 2);
        assert_eq!(a.seg(32 + 16), 64);

        a.free(16 + 10 + 2);
        // |oooooooooooooooo|xxxxxxxxxx|......|xxxxxxxxxxxxxxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16 + 10), 16 + 10 + 2 + 4);
        assert_eq!(a.seg(32 + 16), 64);

        a.free(16);
        // |oooooooooooooooo|................|xxxxxxxxxxxxxxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16), 32);
        assert_eq!(a.seg(32 + 16), 64);

        a.free(32);
        // |oooooooooooooooo|...
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(16), 64);

        assert_eq!(a.alloc(8), 16);
        // |oooooooooooooooo|xxxxxxxx|...

        assert_eq!(a.alloc(4), 16 + 8);
        // |oooooooooooooooo|xxxxxxxx|xxxx|...

        assert_eq!(a.alloc(4), 16 + 8 + 4);
        // |oooooooooooooooo|xxxxxxxx|xxxx|xxxx|...
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(32), 64);

        assert_eq!(a.alloc(4), 16 + 8 + 4 + 4);
        assert_eq!(a.alloc(4), 16 + 8 + 4 + 4 + 4);
        // |oooooooooooooooo|xxxxxxxx|xxxx|xxxx|xxxx|xxxx|...
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(40), 64);
        assert_eq!(mem_end(), 64);

        a.free(16);
        // |oooooooooooooooo|........|xxxx|xxxx|xxxx|xxxx|...
        a.free(16 + 8 + 4);
        // |oooooooooooooooo|........|xxxx|....|xxxx|xxxx|...
        assert_eq!(a.segs(), 3);
        assert_eq!(a.seg(16), 16 + 8);
        assert_eq!(a.seg(16 + 8 + 4), 16 + 8 + 4 + 4);
        assert_eq!(a.seg(40), 64);

        assert_eq!(a.alloc(4), 32);
        // |oooooooooooooooo|........|xxxx|xxxx|xxxx|xxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16), 16 + 8);
        assert_eq!(a.seg(40), 64);

        assert_eq!(a.alloc(20), 40);
        // |oooooooooooooooo|........|xxxx|xxxx|xxxx|xxxx|xxxxxxxxxxxxxxxxxxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16), 16 + 8);
        assert_eq!(a.seg(60), 64);

        assert_eq!(a.alloc(4), 60);
        assert_eq!(a.alloc(4), 16);
        // |oooooooooooooooo|xxxx|....|xxxx|xxxx|xxxx|xxxx|xxxxxxxxxxxxxxxxxxxx|xxxx|
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(20), 24);
        assert_eq!(a.seg(64), 64);
    }
}
