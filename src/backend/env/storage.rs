use crate::canisters::{install, CanisterInstallMode};
use candid::Principal;
use ic_cdk::api::{
    call::call_raw,
    stable::{stable64_grow, stable64_read, stable64_size, stable64_write},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{config::CONFIG, Logger};

pub trait Storable {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Self;
}

#[derive(Default, Serialize, Deserialize)]
pub struct Storage {
    pub buckets: BTreeMap<Principal, u64>,
    #[serde(default)]
    allocator: Allocator,
}

const INITIAL_OFFSET: u64 = 16;

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

    pub fn write<T: Serialize>(&mut self, value: &T) -> (u64, u64) {
        let buffer: Vec<u8> = serde_cbor::to_vec(value).expect("couldn't serialize the state");
        let offset = self.allocator.alloc(buffer.len() as u64);
        stable64_write(offset, &buffer);
        (offset, buffer.len() as u64)
    }

    pub fn size(&self) -> u64 {
        self.allocator.boundary
    }

    pub fn read<T: Storable>(offset: u64, len: u64) -> T {
        let mut bytes = Vec::with_capacity(len as usize);
        bytes.spare_capacity_mut();
        unsafe {
            bytes.set_len(len as usize);
        }
        stable64_read(offset, &mut bytes);
        T::from_bytes(bytes)
    }
}

pub fn heap_to_stable(state: &super::State) {
    let mut storage: Storage = Default::default();
    storage.allocator.segments = state.storage.allocator.segments.clone();
    storage.allocator.boundary = state.storage.allocator.boundary;
    let (offset, len) = storage.write(state);
    stable64_write(0, &offset.to_be_bytes());
    stable64_write(8, &len.to_be_bytes());
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
    Storage::read(offset, len)
}

#[derive(Serialize, Deserialize)]
struct Allocator {
    segments: BTreeMap<u64, u64>,
    boundary: u64,
    #[serde(skip)]
    mem_grow: Option<Box<dyn FnMut(u64)>>,
    #[serde(skip)]
    mem_size: Option<Box<dyn Fn() -> u64>>,
}

impl Default for Allocator {
    fn default() -> Self {
        Self {
            segments: Default::default(),
            boundary: INITIAL_OFFSET,
            mem_size: Some(Box::new(|| stable64_size() << 16)),
            mem_grow: Some(Box::new(|n| {
                stable64_grow(n >> 16).expect("couldn't grow memory");
            })),
        }
    }
}

impl Allocator {
    fn alloc(&mut self, n: u64) -> u64 {
        // find all segments that are big enough
        let mut candidates = BTreeMap::new();
        for (start, size) in self.segments.iter() {
            if size >= &n {
                candidates.insert(size, start);
            }
            if size == &n {
                break;
            }
        }
        let (start, new_segment) = match candidates.first_key_value() {
            // get the smallest segment from the candidates
            Some((size, start)) => (
                **start,
                // if the segment is larger, create a new rest segment
                (n < **size).then_some((**start + n, **size - n)),
            ),
            // if no large enough segments exist, grow the memory
            _ => {
                let boundary = self.boundary;
                self.boundary += n;
                if self.boundary >= (self.mem_size.as_ref().unwrap())() {
                    (self.mem_grow.as_mut().unwrap())(n);
                }
                (boundary, None)
            }
        };
        self.segments.remove(&start);
        if let Some((start, size)) = new_segment {
            self.segments.insert(start, size);
        }
        ic_cdk::println!(
            "Allocated {} bytes, segments={:?}, boundary={}, mem_size={}",
            n,
            &self.segments,
            self.boundary,
            (self.mem_size.as_ref().unwrap())()
        );
        start
    }

    fn free(&mut self, offset: u64, size: u64) {
        let left_segment = self.segments.range(..offset).last().map(|(a, b)| (*a, *b));
        let right_segment = self
            .segments
            .range(offset + size..)
            .next()
            .map(|(a, b)| (*a, *b));
        match (left_segment, right_segment) {
            (Some((l_start, l_size)), Some((r_start, r_size)))
                if l_start + l_size == offset && offset + size == r_start =>
            {
                assert!(offset + size <= r_start);
                self.segments
                    .remove(&l_start)
                    .expect("no left segment found");
                self.segments
                    .remove(&r_start)
                    .expect("no right segment found");
                self.segments.insert(l_start, l_size + size + r_size);
            }
            (_, Some((r_start, r_size))) if offset + size == r_start => {
                assert!(offset + size <= r_start);
                self.segments
                    .remove(&r_start)
                    .expect("no right segment found");
                self.segments.insert(offset, size + r_size);
            }
            (Some((l_start, l_size)), _) if l_start + l_size == offset => {
                self.segments
                    .insert(l_start, l_size + size)
                    .expect("no left segment found");
            }
            _ => {
                self.segments.insert(offset, size);
            }
        }
        ic_cdk::println!(
            "Deallocated segment={:?}, segments={:?}, boundary={}, mem_size={}",
            (offset, size),
            &self.segments,
            self.boundary,
            (self.mem_size.as_ref().unwrap())()
        );
    }

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
        static mut MEM_END: u64 = 16;
        let mem_grow = |n| unsafe {
            MEM_END += n;
        };
        fn mem_end() -> u64 {
            unsafe { MEM_END }
        }
        let mut a = Allocator {
            segments: Default::default(),
            mem_grow: Some(Box::new(mem_grow)),
            mem_size: Some(Box::new(mem_end)),
            boundary: 16,
        };

        // |oooooooooooooooo|...
        assert_eq!(mem_end(), 16);
        assert_eq!(a.segs(), 0);

        assert_eq!(a.alloc(8), 16);
        // |oooooooooooooooo|xxxxxxxx|...
        assert_eq!(mem_end(), 16 + 8);

        assert_eq!(a.alloc(4), 16 + 8);
        // |oooooooooooooooo|xxxxxxxx|xxxx|...
        assert_eq!(mem_end(), 16 + 8 + 4);

        assert_eq!(a.alloc(4), 16 + 8 + 4);
        // |oooooooooooooooo|xxxxxxxx|xxxx|xxxx| 32
        assert_eq!(mem_end(), 16 + 8 + 4 + 4);
        assert_eq!(a.segs(), 0);

        a.free(16 + 8, 4);

        // |oooooooooooooooo|xxxxxxxx|....|xxxx| 32
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(16 + 8), 4);

        assert_eq!(a.alloc(4), 16 + 8);
        // |oooooooooooooooo|xxxxxxxx|xxxx|xxxx| 32
        assert_eq!(a.segs(), 0);

        a.free(16, 8);
        // |oooooooooooooooo|........|xxxx|xxxx| 32
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(16), 8);

        a.free(16 + 8, 4);
        // |oooooooooooooooo|............|xxxx|...
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(16), 8 + 4);

        assert_eq!(a.alloc(10), 16);
        // |oooooooooooooooo|xxxxxxxxxx|..|xxxx|...
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(16 + 10), 2);

        assert_eq!(a.alloc(32), 32);
        // |oooooooooooooooo|xxxxxxxxxx|..|xxxx|xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx|
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(16 + 10), 2);

        a.free(32, 32);
        // |oooooooooooooooo|xxxxxxxxxx|..|xxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16 + 10), 2);
        assert_eq!(a.seg(32), 32);

        assert_eq!(a.alloc(16), 32);
        // |oooooooooooooooo|xxxxxxxxxx|..|xxxx|xxxxxxxxxxxxxxxx|... 64
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16 + 10), 2);
        assert_eq!(a.seg(32 + 16), 16);

        a.free(16 + 10 + 2, 4);
        // |oooooooooooooooo|xxxxxxxxxx|......|xxxxxxxxxxxxxxxx|... 64
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16 + 10), 6);
        assert_eq!(a.seg(32 + 16), 16);

        a.free(16, 10);
        // |oooooooooooooooo|................|xxxxxxxxxxxxxxxx|... 64
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16), 16);
        assert_eq!(a.seg(32 + 16), 16);

        a.free(32, 16);
        // |oooooooooooooooo|... 64
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(16), 48);

        assert_eq!(a.alloc(8), 16);
        // |oooooooooooooooo|xxxxxxxx|... 64

        assert_eq!(a.alloc(4), 16 + 8);
        // |oooooooooooooooo|xxxxxxxx|xxxx|... 64

        assert_eq!(a.alloc(4), 16 + 8 + 4);
        // |oooooooooooooooo|xxxxxxxx|xxxx|xxxx|... 64
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(32), 32);

        assert_eq!(a.alloc(4), 16 + 8 + 4 + 4);
        assert_eq!(a.alloc(4), 16 + 8 + 4 + 4 + 4);
        // |oooooooooooooooo|xxxxxxxx|xxxx|xxxx|xxxx|xxxx|... 64
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(40), 24);
        assert_eq!(mem_end(), 64);

        a.free(16, 8);
        // |oooooooooooooooo|........|xxxx|xxxx|xxxx|xxxx|... 64
        a.free(16 + 8 + 4, 4);
        // |oooooooooooooooo|........|xxxx|....|xxxx|xxxx|... 64
        assert_eq!(a.segs(), 3);
        assert_eq!(a.seg(16), 8);
        assert_eq!(a.seg(16 + 8 + 4), 4);
        assert_eq!(a.seg(40), 24);

        assert_eq!(a.alloc(4), 28);
        // |oooooooooooooooo|........|xxxx|xxxx|xxxx|xxxx|... 64
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16), 8);
        assert_eq!(a.seg(40), 24);

        assert_eq!(a.alloc(20), 40);
        // |oooooooooooooooo|........|xxxx|xxxx|xxxx|xxxx|xxxxxxxxxxxxxxxxxxxx|...
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16), 8);
        assert_eq!(a.seg(60), 4);

        assert_eq!(a.alloc(4), 60);
        assert_eq!(a.alloc(4), 16);
        // |oooooooooooooooo|xxxx|....|xxxx|xxxx|xxxx|xxxx|xxxxxxxxxxxxxxxxxxxx|xxxx|
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(20), 4);

        assert_eq!(a.alloc(4), 20);
        // |oooooooooooooooo|xxxx|xxxx|xxxx|xxxx|xxxx|xxxx|xxxxxxxxxxxxxxxxxxxx|xxxx|
        assert_eq!(a.segs(), 0);

        assert_eq!(a.alloc(4), 64);
        // |oooooooooooooooo|xxxx|xxxx|xxxx|xxxx|xxxx|xxxx|xxxxxxxxxxxxxxxxxxxx|xxxx|xxxx
        assert_eq!(a.segs(), 0);

        a.free(64, 4);
        // |oooooooooooooooo|xxxx|xxxx|xxxx|xxxx|xxxx|xxxx|xxxxxxxxxxxxxxxxxxxx|xxxx|....
        assert_eq!(a.segs(), 1);
        assert_eq!(a.seg(64), 4);

        a.free(16, 4);
        // |oooooooooooooooo|....|xxxx|xxxx|xxxx|xxxx|xxxx|xxxxxxxxxxxxxxxxxxxx|xxxx|....
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16), 4);
        assert_eq!(a.seg(64), 4);

        a.free(20, 4);
        // |oooooooooooooooo|........|xxxx|xxxx|xxxx|xxxx|xxxxxxxxxxxxxxxxxxxx|xxxx|....
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16), 8);
        assert_eq!(a.seg(64), 4);

        a.free(16 + 8 + 4, 4);
        // |oooooooooooooooo|........|xxxx|....|xxxx|xxxx|xxxxxxxxxxxxxxxxxxxx|xxxx|....
        assert_eq!(a.segs(), 3);
        assert_eq!(a.seg(16), 8);
        assert_eq!(a.seg(16 + 8 + 4), 4);
        assert_eq!(a.seg(64), 4);

        a.free(16 + 8, 4);
        // |oooooooooooooooo|................|xxxx|xxxx|xxxxxxxxxxxxxxxxxxxx|xxxx|....
        assert_eq!(a.segs(), 2);
        assert_eq!(a.seg(16), 16);
        assert_eq!(a.seg(64), 4);

        a.free(32 + 8, 4);
        // |oooooooooooooooo|................|xxxx|xxxx|....|xxxxxxxxxxxxxxx|xxxx|....
        assert_eq!(a.segs(), 3);
        assert_eq!(a.seg(16), 16);
        assert_eq!(a.seg(32 + 8), 4);
        assert_eq!(a.seg(64), 4);

        a.free(32 + 4, 4);
        // |oooooooooooooooo|................|xxxx|........|xxxxxxxxxxxxxxx|xxxx|....
        assert_eq!(a.segs(), 3);
        assert_eq!(a.seg(16), 16);
        assert_eq!(a.seg(32 + 4), 8);
        assert_eq!(a.seg(64), 4);

        assert!(a.boundary <= mem_end());
    }
}
