use std::future::Future;

use candid::utils::{ArgumentDecoder, ArgumentEncoder};
use ic_cdk::api::call::CallResult;
use ic_cdk::export::candid::{CandidType, Principal};
use ic_cdk::id;
use ic_cdk::{
    api::{
        call::call_with_payment,
        call::{call, call_raw},
    },
    notify,
};
use serde::Deserialize;

const CYCLES_FOR_NEW_CANISTER: u64 = 1_000_000_000_000;

#[derive(CandidType, Deserialize)]
struct CanisterId {
    canister_id: Principal,
}

#[derive(CandidType)]
struct CanisterSettings {
    pub controllers: Option<Vec<Principal>>,
}

pub async fn new() -> Result<Principal, String> {
    let (response,): (CanisterId,) = call_with_payment(
        Principal::management_canister(),
        "create_canister",
        (CanisterSettings { controllers: None },),
        CYCLES_FOR_NEW_CANISTER,
    )
    .await
    .map_err(|err| format!("couldn't create a new canister: {:?}", err))?;

    Ok(response.canister_id)
}

#[derive(CandidType, Deserialize)]
pub enum CanisterInstallMode {
    #[serde(rename = "install")]
    Install,
    #[serde(rename = "reinstall")]
    Reinstall,
    #[serde(rename = "upgrade")]
    Upgrade,
}

#[derive(Deserialize, CandidType)]
pub struct Settings {
    pub controllers: Vec<Principal>,
}

#[derive(Deserialize, CandidType)]
pub struct StatusCallResult {
    pub settings: Settings,
    pub module_hash: Option<Vec<u8>>,
}

pub async fn settings(canister_id: Principal) -> Result<StatusCallResult, String> {
    #[derive(CandidType)]
    struct In {
        canister_id: Principal,
    }

    let (result,): (StatusCallResult,) = call(
        Principal::management_canister(),
        "canister_status",
        (In { canister_id },),
    )
    .await
    .map_err(|err| format!("couldn't get canister status: {:?}", err))?;
    Ok(result)
}

#[derive(CandidType)]
struct InstallCodeArgs<'a> {
    pub mode: CanisterInstallMode,
    pub canister_id: Principal,
    pub wasm_module: &'a [u8],
    pub arg: Vec<u8>,
}

pub async fn install(
    canister_id: Principal,
    wasm_module: &[u8],
    mode: CanisterInstallMode,
) -> Result<(), String> {
    #[derive(CandidType)]
    struct InstallCodeArgs<'a> {
        pub mode: CanisterInstallMode,
        pub canister_id: Principal,
        pub wasm_module: &'a [u8],
        pub arg: Vec<u8>,
    }

    let args = (InstallCodeArgs {
        mode,
        canister_id,
        wasm_module,
        arg: ic_cdk::api::id().as_slice().to_vec(),
    },);
    let mgmnt_can_id = Principal::management_canister();
    let (_,): ((),) = call(mgmnt_can_id, "install_code", args)
        .await
        .map_err(|err| format!("couldn't install the WASM module: {:?}", err))?;
    Ok(())
}

pub fn upgrade_main_canister(wasm_module: &[u8]) {
    let args = (InstallCodeArgs {
        mode: CanisterInstallMode::Upgrade,
        canister_id: id(),
        wasm_module,
        arg: ic_cdk::api::id().as_slice().to_vec(),
    },);
    let mgmnt_can_id = Principal::management_canister();
    notify(mgmnt_can_id, "install_code", args).expect("self-upgrade failed");
}

pub async fn set_controllers(
    canister_id: Principal,
    controllers: Vec<Principal>,
) -> Result<(), String> {
    #[derive(CandidType)]
    struct In {
        canister_id: Principal,
        settings: CanisterSettings,
    }
    call(
        Principal::management_canister(),
        "update_settings",
        (In {
            canister_id,
            settings: CanisterSettings {
                controllers: Some(controllers),
            },
        },),
    )
    .await
    .map_err(|err| format!("couldn't set controllers: {:?}", err))?;
    Ok(())
}

pub async fn topup_with_cycles(canister_id: Principal, cycles: u64) -> Result<(), String> {
    #[derive(CandidType)]
    struct Args {
        pub canister_id: Principal,
    }
    let (_,): ((),) = call_with_payment(
        Principal::management_canister(),
        "deposit_cycles",
        (Args { canister_id },),
        cycles,
    )
    .await
    .map_err(|err| format!("couldn't deposit cycles: {:?}", err))?;
    Ok(())
}

pub async fn top_up(canister_id: Principal, min_cycle_balance: u64) -> Result<bool, String> {
    let bytes = call_raw(canister_id, "balance", Default::default(), 0)
        .await
        .map_err(|err| {
            format!(
                "couldn't get balance from canister {}: {:?}",
                canister_id, err
            )
        })?;
    let mut arr: [u8; 8] = Default::default();
    arr.copy_from_slice(&bytes);
    if u64::from_be_bytes(arr) < min_cycle_balance {
        topup_with_cycles(canister_id, min_cycle_balance)
            .await
            .map_err(|err| format!("failed to top up canister {}: {:?}", canister_id, err))?;
        return Ok(true);
    }
    Ok(false)
}

pub fn call_canister<T: ArgumentEncoder, R: for<'a> ArgumentDecoder<'a>>(
    id: Principal,
    method: &str,
    args: T,
) -> impl Future<Output = CallResult<R>> {
    ic_cdk::call(id, method, args)
}
