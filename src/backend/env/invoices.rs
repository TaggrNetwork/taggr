use ic_cdk::api::{id, time};
use ic_cdk::export::candid::{CandidType, Principal};
use ic_ledger_types::{
    AccountBalanceArgs, AccountIdentifier, BlockIndex, Memo, Subaccount, Tokens, TransferArgs,
    TransferResult, DEFAULT_FEE, DEFAULT_SUBACCOUNT, MAINNET_CYCLES_MINTING_CANISTER_ID,
    MAINNET_LEDGER_CANISTER_ID,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;

const INVOICE_MAX_AGE_HOURS: u64 = 24 * 3600000000000_u64;

#[derive(CandidType, Deserialize)]
struct IcpXdrConversionRate {
    xdr_permyriad_per_icp: u64,
}

#[derive(CandidType, Deserialize)]
struct IcpXdrConversionRateCertifiedResponse {
    data: IcpXdrConversionRate,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Invoice {
    pub e8s: u64,
    pub paid_e8s: u64,
    pub paid: bool,
    time: u64,
    sub_account: Subaccount,
    pub account: AccountIdentifier,
}

#[derive(Deserialize, Default, Serialize)]
pub struct Invoices {
    pub invoices: HashMap<Principal, Invoice>,
}

impl Invoices {
    pub fn clean_up(&mut self) {
        self.invoices
            .retain(|_, invoice| time() - invoice.time < INVOICE_MAX_AGE_HOURS)
    }

    async fn create_invoice(&mut self, invoice_id: Principal) -> Result<Invoice, String> {
        let time = time();
        let sub_account = principal_to_subaccount(&invoice_id);
        let account = AccountIdentifier::new(&id(), &sub_account);
        let invoice = Invoice {
            paid: false,
            e8s: get_xdr_in_e8s().await?,
            paid_e8s: 0,
            time,
            account,
            sub_account,
        };
        self.invoices.insert(invoice_id, invoice.clone());
        Ok(invoice)
    }

    pub fn close(&mut self, invoice_id: &Principal) {
        self.invoices.remove(invoice_id);
    }

    pub async fn outstanding(
        &mut self,
        invoice_id: &Principal,
        kilo_cycles: u64,
    ) -> Result<Invoice, String> {
        let invoice = match self.invoices.get_mut(invoice_id) {
            Some(invoice) => invoice,
            None => {
                let invoice = self.create_invoice(*invoice_id).await?;
                self.invoices.insert(*invoice_id, invoice);
                let invoice = self.invoices.get_mut(invoice_id).expect("no invoice found");
                if kilo_cycles == 0 {
                    return Ok(invoice.clone());
                }
                invoice
            }
        };
        if invoice.paid {
            return Ok(invoice.clone());
        }
        let balance = account_balance(invoice.account).await;
        let costs = if kilo_cycles == 0 {
            balance
        } else {
            Tokens::from_e8s(kilo_cycles * invoice.e8s)
        };
        if balance >= costs {
            transfer_raw(
                MAINNET_LEDGER_CANISTER_ID,
                costs,
                main_account(),
                Memo(0),
                Some(invoice.sub_account),
            )
            .await?;
            invoice.paid = true;
            invoice.paid_e8s = costs.e8s();
        } else if kilo_cycles > 0 {
            return Err("ICP balance too low".into());
        }
        Ok(invoice.clone())
    }
}

/// Transfer e8s from Treasury to `acc`.
pub async fn transfer(acc: &str, e8s: u64) -> Result<BlockIndex, String> {
    transfer_raw(
        MAINNET_LEDGER_CANISTER_ID,
        Tokens::from_e8s(e8s),
        parse_account(acc)?,
        Memo(0),
        None,
    )
    .await
}

pub fn parse_account(acc: &str) -> Result<AccountIdentifier, String> {
    let decoded_acc =
        &hex::decode(acc).map_err(|err| format!("couldn't decode account address: {:?}", err))?;
    if decoded_acc.len() != 32 {
        return Err(format!("malformed account address {:?}", acc));
    }
    let mut id: [u8; 32] = Default::default();
    id.copy_from_slice(decoded_acc);
    AccountIdentifier::try_from(id).map_err(|err| format!("couldn't parse account: {:?}", err))
}

pub fn fee() -> u64 {
    // TODO: fetch from ledger
    DEFAULT_FEE.e8s()
}

pub fn main_account() -> AccountIdentifier {
    AccountIdentifier::new(&id(), &DEFAULT_SUBACCOUNT)
}

pub async fn main_account_balance() -> Tokens {
    account_balance(main_account()).await
}

pub fn e8s_to_icp(e8s: u64) -> String {
    format!("{}.{:08}", e8s / 100000000, e8s % 100000000)
}

pub async fn transfer_raw(
    ledger_canister_id: Principal,
    amount: Tokens,
    to: AccountIdentifier,
    memo: Memo,
    sub_account: Option<Subaccount>,
) -> Result<BlockIndex, String> {
    let (result,): (TransferResult,) = ic_cdk::call(
        ledger_canister_id,
        "transfer",
        (TransferArgs {
            created_at_time: None,
            memo,
            amount: amount - DEFAULT_FEE,
            fee: DEFAULT_FEE,
            to,
            from_subaccount: sub_account,
        },),
    )
    .await
    .map_err(|err| format!("call to ledger failed: {:?}", err))?;
    result.map_err(|err| {
        format!(
            "transfer of {} e8s from subaccount {:?} failed: {:?}",
            amount, sub_account, err
        )
    })
}

async fn account_balance(account: AccountIdentifier) -> Tokens {
    let (balance,): (Tokens,) = ic_cdk::call(
        MAINNET_LEDGER_CANISTER_ID,
        "account_balance",
        (AccountBalanceArgs { account },),
    )
    .await
    .expect("couldn't check balance");
    balance
}

pub async fn get_xdr_in_e8s() -> Result<u64, String> {
    let (IcpXdrConversionRateCertifiedResponse {
        data: IcpXdrConversionRate {
            xdr_permyriad_per_icp,
        },
    },) = ic_cdk::call(
        MAINNET_CYCLES_MINTING_CANISTER_ID,
        "get_icp_xdr_conversion_rate",
        (),
    )
    .await
    .map_err(|err| format!("couldn't get ICP/XDR ratio: {:?}", err))?;
    Ok((100000000.0 / xdr_permyriad_per_icp as f64) as u64 * 10000)
}

pub async fn topup_with_icp(canister_id: &Principal, xdrs: u64) -> Result<u128, String> {
    let e8s = xdrs * get_xdr_in_e8s().await?;
    let block_index = super::invoices::transfer_raw(
        MAINNET_LEDGER_CANISTER_ID,
        Tokens::from_e8s(e8s),
        AccountIdentifier::new(
            &MAINNET_CYCLES_MINTING_CANISTER_ID,
            &principal_to_subaccount(canister_id),
        ),
        Memo(0x50555054),
        None,
    )
    .await?;
    notify(*canister_id, block_index).await
}

async fn notify(canister_id: Principal, block_index: u64) -> Result<u128, String> {
    #[derive(CandidType)]
    struct NotifyTopUpArg {
        block_index: u64,
        canister_id: Principal,
    }

    #[derive(CandidType, Debug, Deserialize)]
    pub enum NotifyError {
        Refunded {
            reason: String,
            block_index: Option<u64>,
        },
        InvalidTransaction(String),
        TransactionTooOld(u64),
        Processing,
        Other {
            error_code: u64,
            error_message: String,
        },
    }

    let (result,): (Result<u128, NotifyError>,) = ic_cdk::call(
        MAINNET_CYCLES_MINTING_CANISTER_ID,
        "notify_top_up",
        (NotifyTopUpArg {
            canister_id,
            block_index,
        },),
    )
    .await
    .map_err(|err| format!("couldn't notify the CMC canister: {:?}", err))?;
    result.map_err(|err| format!("CMC notification failed: {:?}", err))
}

pub fn principal_to_subaccount(principal_id: &Principal) -> Subaccount {
    let mut subaccount = [0; std::mem::size_of::<Subaccount>()];
    let principal_id = principal_id.as_slice();
    subaccount[0] = principal_id.len() as u8;
    subaccount[1..1 + principal_id.len()].copy_from_slice(principal_id);
    Subaccount(subaccount)
}
