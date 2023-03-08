use crate::*;
use candid::{CandidType, Deserialize};
use ic_cdk::export::candid::Principal;
use serde::Serialize;

type Timestamp = u64;

pub type Subaccount = Vec<u8>;

type Memo = [u8; 32];

pub type Token = u64;

#[derive(CandidType, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct Account {
    pub owner: Principal,
    pub subaccount: Option<Subaccount>,
}

#[derive(CandidType, Deserialize)]
pub struct TransferArgs {
    from_subaccount: Option<Subaccount>,
    to: Account,
    amount: u128,
    fee: Option<u128>,
    memo: Option<Memo>,
    created_at_time: Option<Timestamp>,
}

#[derive(Serialize, Deserialize)]
pub struct Transaction {
    pub timestamp: u64,
    pub from: Account,
    pub to: Account,
    pub amount: Token,
    pub fee: Token,
    pub memo: Option<Memo>,
}

// pub struct BadFee {
//     expected_fee: u64,
// }

// pub struct BadBurn {
//     min_burn_amount: u64,
// }

// pub struct Duplicate {
//     duplicate_of: u64,
// }

#[derive(CandidType, Debug, PartialEq, Serialize)]
pub struct InsufficientFunds {
    balance: u128,
}

#[derive(CandidType, Debug, PartialEq, Serialize)]
pub struct CreatedInFuture {
    ledger_time: Timestamp,
}

#[derive(CandidType, Debug, PartialEq, Serialize)]
pub struct GenericError {
    error_code: u128,
    message: String,
}

#[derive(CandidType, Debug, PartialEq, Serialize)]
pub enum TransferError {
    // BadFee(BadFee),
    // BadBurn(BadBurn),
    // Duplicate(Duplicate),
    // TemporarilyUnavailable,
    InsufficientFunds(InsufficientFunds),
    TooOld,
    CreatedInFuture(CreatedInFuture),
    GenericError(GenericError),
}

#[derive(CandidType)]
pub enum Value {
    Nat(u128),
    Text(String),
    // Int(i64),
    // Blob(Vec<u8>),
}

#[derive(CandidType)]
pub struct Standard {
    name: String,
    url: String,
}

#[query]
fn icrc1_metadata() -> Vec<(String, Value)> {
    vec![
        (
            "icrc1:symbol".into(),
            Value::Text(CONFIG.token_symbol.into()),
        ),
        ("icrc1:name".into(), Value::Text(CONFIG.name.into())),
        ("icrc1:decimals".into(), Value::Nat(2)),
        ("icrc1:fee".into(), Value::Nat(1)),
    ]
}

#[query]
fn icrc1_name() -> String {
    CONFIG.name.into()
}

#[query]
fn icrc1_symbol() -> String {
    CONFIG.token_symbol.into()
}

#[query]
fn icrc1_decimals() -> u8 {
    CONFIG.token_decimals
}

#[query]
fn icrc1_fee() -> u128 {
    CONFIG.transaction_fee as u128
}

#[query]
fn icrc1_total_supply() -> u128 {
    CONFIG.total_supply as u128
}

#[query]
fn icrc1_minting_account() -> Option<Account> {
    Some(account(Principal::anonymous()))
}

#[query]
fn icrc1_balance_of(mut account: Account) -> u128 {
    if account
        .subaccount
        .as_ref()
        .map(|val| val.iter().all(|b| b == &0))
        .unwrap_or(true)
    {
        account.subaccount = None
    };
    state().balances.get(&account).copied().unwrap_or_default() as u128
}

#[query]
fn icrc1_supported_standards() -> Vec<Standard> {
    vec![Standard {
        name: "ICRC-1".into(),
        url: "https://github.com/dfinity/ICRC-1".into(),
    }]
}

const MINUTE: u64 = 60000000000_u64;

#[update]
fn icrc1_transfer(args: TransferArgs) -> Result<u128, TransferError> {
    transfer(time(), state_mut(), caller(), args)
}

fn transfer(
    now: u64,
    state: &mut State,
    owner: Principal,
    args: TransferArgs,
) -> Result<u128, TransferError> {
    if owner == Principal::anonymous() {
        return Err(TransferError::GenericError(GenericError {
            error_code: 0,
            message: "No transfers from the minting account possible.".into(),
        }));
    }
    let TransferArgs {
        from_subaccount,
        to,
        amount,
        fee,
        created_at_time,
        memo,
        ..
    } = args;

    // check the time
    let effective_time = created_at_time.unwrap_or(now);
    if effective_time + 5 * MINUTE < now {
        return Err(TransferError::TooOld);
    }
    if effective_time.saturating_sub(5 * MINUTE) > now {
        return Err(TransferError::CreatedInFuture(CreatedInFuture {
            ledger_time: now,
        }));
    }

    let subaccount = if from_subaccount
        .as_ref()
        .map(|val| val.iter().all(|b| b == &0))
        .unwrap_or(true)
    {
        None
    } else {
        from_subaccount
    };
    let from = Account { owner, subaccount };

    match state.balances.get(&from) {
        None => {
            return Err(TransferError::InsufficientFunds(InsufficientFunds {
                balance: 0,
            }))
        }
        Some(balance) => {
            let effective_fee = fee.unwrap_or_else(icrc1_fee) as u64;
            let effective_amount = amount as u64 + effective_fee;
            if *balance < effective_amount {
                return Err(TransferError::InsufficientFunds(InsufficientFunds {
                    balance: *balance as u128,
                }));
            }
            let resulting_balance = balance.saturating_sub(effective_amount);
            if resulting_balance == 0 {
                state.balances.remove(&from);
            } else {
                state.balances.insert(from.clone(), resulting_balance);
            }
            if to.owner != Principal::anonymous() {
                let recipient_balance = state.balances.remove(&to).unwrap_or_default();
                state
                    .balances
                    .insert(to.clone(), recipient_balance + amount as u64);
            }
            state.ledger.push(Transaction {
                timestamp: now,
                from,
                to,
                amount: amount as u64,
                fee: effective_fee,
                memo,
            });
        }
    }
    Ok(0)
}

pub fn account(owner: Principal) -> Account {
    Account {
        owner,
        subaccount: None,
    }
}

pub fn mint(state: &mut State, account: Account, tokens: Token) {
    state
        .balances
        .entry(account.clone())
        .and_modify(|b| *b += tokens)
        .or_insert(tokens);
    state.ledger.push(Transaction {
        timestamp: time(),
        from: Account {
            owner: Principal::anonymous(),
            subaccount: None,
        },
        to: account,
        amount: tokens,
        fee: 0,
        memo: None,
    });
}

pub fn move_funds(state: &mut State, from: &Account, to: Account) -> Result<u128, TransferError> {
    let balance = state.balances.get(from).copied().unwrap_or_default();
    let mut n = 0;
    if balance > 0 {
        let fee = icrc1_fee();
        n = transfer(
            time(),
            state,
            from.owner,
            TransferArgs {
                from_subaccount: from.subaccount.clone(),
                to,
                amount: (balance - fee as u64) as u128,
                fee: None,
                memo: Default::default(),
                created_at_time: None,
            },
        )?;
    }
    state.balances.remove(from);
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pr(n: u8) -> Principal {
        let v = vec![n];
        Principal::from_slice(&v)
    }

    #[test]
    fn test_transfers() {
        let mut state = State::default();
        assert_eq!(
            transfer(
                1000 * MINUTE,
                &mut state,
                pr(0),
                TransferArgs {
                    from_subaccount: None,
                    to: account(pr(1)),
                    amount: 1,
                    fee: Some(1),
                    memo: None,
                    created_at_time: None
                }
            ),
            Err(TransferError::InsufficientFunds(InsufficientFunds {
                balance: 0
            }))
        );

        assert_eq!(
            transfer(
                100 * MINUTE,
                &mut state,
                pr(0),
                TransferArgs {
                    from_subaccount: None,
                    to: account(pr(1)),
                    amount: 1,
                    fee: Some(1),
                    memo: None,
                    created_at_time: Some(94 * MINUTE)
                }
            ),
            Err(TransferError::TooOld)
        );

        assert_eq!(
            transfer(
                100 * MINUTE,
                &mut state,
                pr(0),
                TransferArgs {
                    from_subaccount: None,
                    to: account(pr(1)),
                    amount: 1,
                    fee: Some(1),
                    memo: None,
                    created_at_time: Some(106 * MINUTE)
                }
            ),
            Err(TransferError::CreatedInFuture(CreatedInFuture {
                ledger_time: 6000000000000
            }))
        );

        assert_eq!(
            transfer(
                time(),
                &mut state,
                Principal::anonymous(),
                TransferArgs {
                    from_subaccount: None,
                    to: account(pr(1)),
                    amount: 1,
                    fee: Some(1),
                    memo: None,
                    created_at_time: None
                }
            ),
            Err(TransferError::GenericError(GenericError {
                error_code: 0,
                message: "No transfers from the minting account possible.".into()
            }))
        );

        state.balances.insert(account(pr(0)), 1000);

        assert_eq!(
            transfer(
                time(),
                &mut state,
                pr(0),
                TransferArgs {
                    from_subaccount: None,
                    to: account(pr(1)),
                    amount: 500,
                    fee: Some(1),
                    memo: None,
                    created_at_time: None
                }
            ),
            Ok(0),
        );
        assert_eq!(state.balances.get(&account(pr(0))), Some(&(1000 - 500 - 1)));
        assert_eq!(state.balances.get(&account(pr(1))), Some(&500));

        assert_eq!(
            transfer(
                time(),
                &mut state,
                pr(0),
                TransferArgs {
                    from_subaccount: None,
                    to: icrc1_minting_account().unwrap(),
                    amount: 490,
                    fee: Some(1),
                    memo: None,
                    created_at_time: None
                }
            ),
            Ok(0),
        );
        assert_eq!(
            state.balances.get(&account(pr(0))),
            Some(&(1000 - 500 - 1 - 490 - 1))
        );
        assert_eq!(state.balances.get(&icrc1_minting_account().unwrap()), None,);

        assert_eq!(
            transfer(
                time(),
                &mut state,
                pr(0),
                TransferArgs {
                    from_subaccount: None,
                    to: account(pr(0)),
                    amount: 490,
                    fee: Some(1),
                    memo: None,
                    created_at_time: None
                }
            ),
            Err(TransferError::InsufficientFunds(InsufficientFunds {
                balance: 8
            }))
        );
    }
}
