use self::invoices::Invoice;
use self::proposals::Status;
use self::token::account;
use self::user::{Notification, Predicate};
use crate::env::invoices::principal_to_subaccount;
use crate::proposals::Proposal;
use crate::token::{Account, Token, Transaction};
use config::{CONFIG, ICP_CYCLES_PER_XDR};
use ic_cdk::api::stable::stable64_size;
use ic_cdk::api::{self, canister_balance};
use ic_cdk::export::candid::Principal;
use ic_ledger_types::Tokens;
use invoices::e8s_to_icp;
use invoices::Invoices;
use memory::Storable;
use post::{Post, PostId};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use user::{User, UserId};

pub mod canisters;
pub mod config;
pub mod invoices;
pub mod memory;
pub mod post;
pub mod proposals;
pub mod reports;
pub mod storage;
pub mod token;
pub mod user;

pub type Cycles = i64;
pub type Karma = i64;
pub type Blob = ByteBuf;

const HOUR: u64 = 3600000000000_u64;
const WEEK: u64 = 7 * 24 * HOUR;

#[derive(Deserialize, Serialize)]
pub struct SearchResult {
    pub id: PostId,
    pub user_id: UserId,
    pub result: String,
    pub relevant: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Event {
    pub timestamp: u64,
    pub level: String,
    pub message: String,
}

#[derive(Serialize, Deserialize)]
pub struct Stats {
    weekly_karma_leaders: Vec<(UserId, Karma)>,
    users: usize,
    bootcamp_users: usize,
    cycles: Cycles,
    canister_cycle_balance: u64,
    burned_cycles: Cycles,
    burned_cycles_total: Cycles,
    total_revenue_shared: u64,
    total_rewards_shared: u64,
    posts: usize,
    comments: usize,
    account: String,
    last_distribution: u64,
    last_chores: u64,
    stalwarts: Vec<UserId>,
    bots: Vec<UserId>,
    state_size: u64,
    active_users: usize,
    invited_users: usize,
    buckets: Vec<(String, u64)>,
    users_online: usize,
    last_upgrade: u64,
    module_hash: String,
    canister_id: Principal,
    circulating_supply: u64,
    meta: String,
}

#[derive(Serialize, Deserialize)]
pub struct Realm {
    logo: String,
    pub description: String,
    pub posts: Vec<PostId>,
    controllers: Vec<UserId>,
    pub members: BTreeSet<UserId>,
    pub label_color: String,
}

#[derive(Default, Serialize, Deserialize)]
pub struct State {
    pub burned_cycles: Cycles,
    pub burned_cycles_total: Cycles,
    pub posts: HashMap<PostId, Post>,
    pub users: HashMap<UserId, User>,
    pub principals: HashMap<Principal, UserId>,
    pub next_post_id: PostId,
    pub next_user_id: UserId,
    pub accounting: Invoices,
    pub last_distribution: u64,
    pub storage: storage::Storage,
    pub last_chores: u64,
    pub logger: Logger,
    pub hot: VecDeque<PostId>,
    pub invites: BTreeMap<String, (UserId, Cycles)>,
    pub realms: BTreeMap<String, Realm>,
    pub balances: HashMap<Account, Token>,

    total_revenue_shared: u64,
    total_rewards_shared: u64,

    pub proposals: Vec<Proposal>,
    pub ledger: Vec<Transaction>,

    pub team_tokens: HashMap<UserId, Token>,

    #[serde(default)]
    pub memory: memory::Memory,

    #[serde(skip)]
    pub module_hash: String,
    #[serde(skip)]
    pub last_upgrade: u64,
}

impl Storable for State {
    fn to_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(&self).expect("couldn't serialize the state")
    }
    fn from_bytes(bytes: Vec<u8>) -> Self {
        serde_cbor::from_slice(&bytes).expect("couldn't deserialize")
    }
}

#[derive(Default, Deserialize, Serialize)]
pub struct Logger {
    pub events: Vec<Event>,
}

impl Logger {
    pub fn error<T: ToString>(&mut self, message: T) {
        self.log(message, "ERROR".to_string());
    }

    pub fn info<T: ToString>(&mut self, message: T) {
        self.log(message, "INFO".to_string());
    }

    fn log<T: ToString>(&mut self, message: T, level: String) {
        self.events.push(Event {
            timestamp: time(),
            message: message.to_string(),
            level,
        });
        while self.events.len() > 200 {
            self.events.remove(0);
        }
    }
}

pub enum Destination {
    Karma,
    Cycles,
}

impl State {
    fn spend_to_user_karma<T: ToString>(&mut self, id: UserId, amount: Cycles, log: T) {
        assert!(amount >= 0);
        let user = self.users.get_mut(&id).expect("no user found");
        user.change_karma(amount, log.to_string());
        if amount > CONFIG.voting_reward {
            self.logger.info(format!(
                "Spent `{}` cycles on @{}'s karma for {}.",
                amount,
                user.name,
                log.to_string()
            ));
        }
        self.burned_cycles -= amount;
    }

    fn spend<T: ToString>(&mut self, amount: Cycles, log: T) {
        assert!(amount >= 0);
        if amount > 5 {
            self.logger
                .info(format!("Spent `{}` cycles on {}.", amount, log.to_string()));
        }
        self.burned_cycles -= amount;
    }

    pub fn charge<T: ToString>(
        &mut self,
        id: UserId,
        amount: Cycles,
        log: T,
    ) -> Result<(), String> {
        if amount < 1 {
            return Err("non-positive amount".into());
        }
        let user = self.users.get_mut(&id).ok_or("no user found")?;
        user.change_cycles(-amount, log)?;
        self.burned_cycles += amount;
        Ok(())
    }

    pub fn cycle_transfer<T: ToString>(
        &mut self,
        sender: UserId,
        receiver: UserId,
        amount: Cycles,
        fee: Cycles,
        destination: Destination,
        log: T,
    ) -> Result<(), String> {
        assert!(amount >= 0 && fee >= 0);
        let sender = self.users.get_mut(&sender).expect("no sender found");
        sender.change_cycles(-(amount + fee), log.to_string())?;
        let receiver = self.users.get_mut(&receiver).expect("no receiver found");
        self.burned_cycles += fee;
        match destination {
            Destination::Karma => {
                receiver.change_karma(amount, log);
                Ok(())
            }
            Destination::Cycles => receiver.change_cycles(amount, log),
        }
    }

    pub fn load(&mut self) {
        crate::assets::load();
        self.last_upgrade = time();
    }

    pub fn hot_posts(&self, principal: Principal, page: usize) -> Vec<Post> {
        let current_realm = self
            .principal_to_user(principal)
            .and_then(|u| u.current_realm.clone());
        self.hot
            .iter()
            .filter_map(|post_id| self.posts.get(post_id))
            .filter(|post| current_realm.is_none() || post.realm == current_realm)
            .skip(page * CONFIG.feed_page_size)
            .take(CONFIG.feed_page_size)
            .cloned()
            .collect()
    }

    pub fn enter_realm(&mut self, principal: Principal, name: String) {
        let user = match self.principal_to_user_mut(principal) {
            Some(user) => user,
            _ => return,
        };
        if user.realms.contains(&name) {
            user.current_realm = Some(name);
            return;
        }
        user.current_realm = None;
    }

    pub fn toggle_realm_membership(&mut self, principal: Principal, name: String) -> bool {
        if !self.realms.contains_key(&name) {
            return false;
        }
        let user = match self.principal_to_user_mut(principal) {
            Some(user) => user,
            _ => return false,
        };
        let user_id = user.id;
        if user.realms.contains(&name) {
            user.realms.retain(|realm| realm != &name);
            if user.current_realm == Some(name.clone()) {
                user.current_realm = None
            }
            self.realms
                .get_mut(&name)
                .map(|realm| realm.members.remove(&user_id));
            return false;
        }
        user.realms.push(name.clone());
        self.realms
            .get_mut(&name)
            .map(|realm| realm.members.insert(user_id));
        true
    }

    pub fn edit_realm(
        &mut self,
        principal: Principal,
        name: String,
        logo: String,
        label_color: String,
        description: String,
        controllers: Vec<UserId>,
    ) -> Result<(), String> {
        if controllers.is_empty() {
            return Err("no controllers specified".into());
        }
        let user_id = self
            .principal_to_user_mut(principal)
            .ok_or("no user found")?
            .id;
        let realm = self.realms.get_mut(&name).ok_or("no realm found")?;
        if !realm.controllers.contains(&user_id) {
            return Err("not authorized".into());
        }
        if !logo.is_empty() {
            realm.logo = logo;
        }
        realm.description = description;
        realm.controllers = controllers;
        realm.label_color = label_color;
        Ok(())
    }

    pub fn create_realm(
        &mut self,
        principal: Principal,
        name: String,
        logo: String,
        label_color: String,
        description: String,
        controllers: Vec<UserId>,
    ) -> Result<(), String> {
        if controllers.is_empty() {
            return Err("no controllers specified".into());
        }

        if name.len() > CONFIG.max_realm_name {
            return Err("realm name too long".into());
        }

        if name
            .chars()
            .any(|c| !char::is_alphanumeric(c) && c != '_' && c != '-')
        {
            return Err("realm name should be an alpha-numeric string".into());
        }

        if name.chars().all(|c| char::is_ascii_digit(&c)) {
            return Err("realm name should have at least on character".into());
        }

        if self.realms.contains_key(&name) {
            return Err("realm name taken".into());
        }

        let user = self
            .principal_to_user(principal)
            .ok_or("no user found")?
            .clone();

        self.charge(user.id, CONFIG.realm_cost, "realm creation".to_string())
            .map_err(|err| {
                format!(
                    "couldn't charge {} cycles for realm creation: {}",
                    CONFIG.realm_cost, err
                )
            })?;

        self.realms.insert(
            name.clone(),
            Realm {
                logo,
                description,
                controllers,
                label_color,
                posts: Default::default(),
                members: vec![user.id].into_iter().collect(),
            },
        );

        self.logger.info(format!(
            "@{} created realm [{1}](/#/realm/{1}) 🎭",
            user.name, name
        ));

        Ok(())
    }

    pub fn tip(
        &mut self,
        principal: Principal,
        post_id: PostId,
        tip: Cycles,
    ) -> Result<(), String> {
        let ledger_log = format!("tipping for post {}", post_id);
        let tipper = self.principal_to_user(principal).ok_or("no user found")?;
        let tipper_id = tipper.id;
        let tipper_name = tipper.name.clone();
        let author_id = self.posts.get(&post_id).ok_or("post not found")?.user;
        self.cycle_transfer(
            tipper_id,
            author_id,
            tip,
            CONFIG.tipping_fee,
            Destination::Cycles,
            ledger_log,
        )?;
        let post = self.posts.get_mut(&post_id).expect("post not found");
        post.tips.push((tipper_id, tip));
        self.users
            .get_mut(&author_id)
            .expect("user not found")
            .notify_about_post(
                format!(
                    "@{} tipped you with `{}` cycles for your post",
                    tipper_name, tip,
                ),
                post_id,
            );
        Ok(())
    }

    pub fn tree(&self, id: PostId) -> HashMap<PostId, &'_ Post> {
        let mut backlog = vec![id];
        let mut posts: HashMap<_, _> = Default::default();
        while let Some(post) = backlog.pop().and_then(|id| self.posts.get(&id)) {
            backlog.extend_from_slice(post.children.as_slice());
            posts.insert(post.id, post);
        }
        posts
    }

    fn new_user(&mut self, principal: Principal, timestamp: u64, name: String) -> UserId {
        let id = self.new_user_id();
        let mut user = User::new(principal, id, timestamp, name);
        user.notify(format!("**Welcome!** 🎉 Use #{} as your personal blog, micro-blog or a photo blog. Use #hashtags to connect with others. Make sure you understand [how {0} works](/#/whitepaper). And finally, [say hello](#/new) and start earning karma!", CONFIG.name));
        self.principals.insert(principal, user.id);
        self.logger
            .info(format!("@{} joined {} 🚀", &user.name, CONFIG.name));
        self.users.insert(user.id, user);
        id
    }

    pub async fn create_user(
        &mut self,
        principal: Principal,
        name: String,
        invite: Option<String>,
    ) -> Result<(), String> {
        self.validate_username(&name)?;
        if let Some(user) = self.principal_to_user(principal) {
            return Err(format!("principal already assigned to user @{}", user.name));
        }
        if let Some((user_id, cycles)) = invite.and_then(|code| self.invites.remove(&code)) {
            let id = self.new_user(principal, time(), name.clone());
            let user = self.users.get_mut(&id).expect("no user found");
            user.change_cycles(cycles, "top up by invite".to_string())?;
            user.invited_by = Some(user_id);
            if let Some(inviter) = self.users.get_mut(&user_id) {
                inviter.notify(format!(
                    "Your invite was used by @{}! Thanks for helping #{} grow! 🤗",
                    name, CONFIG.name
                ));
            }
            return Ok(());
        }

        if let Ok(Invoice { paid: true, .. }) = self.mint_cycles(principal, 0).await {
            self.new_user(principal, time(), name);
            // After the user has beed created, transfer cycles.
            return self.mint_cycles(principal, 0).await.map(|_| ());
        }

        Err("payment missing or the invite is invalid".to_string())
    }

    pub fn invites(&self, principal: Principal) -> Vec<(String, Cycles)> {
        self.principal_to_user(principal)
            .map(|user| {
                self.invites
                    .iter()
                    .filter(|(_, (user_id, _))| user_id == &user.id)
                    .map(|(code, (_, cycles))| (code.clone(), *cycles))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    pub fn create_invite(&mut self, principal: Principal, cycles: Cycles) -> Result<(), String> {
        let min_cycles = CONFIG.min_cycles_for_inviting;
        let user_id = self
            .principal_to_user_mut(principal)
            .ok_or("no user found")?
            .id;
        if cycles < min_cycles {
            return Err(format!(
                "smallest invite must contain {} cycles",
                min_cycles
            ));
        }
        self.charge(user_id, cycles, "invite creation".to_string())?;
        let mut hasher = Sha256::new();
        hasher.update(principal.as_slice());
        hasher.update(time().to_be_bytes());
        let code = format!("{:x}", hasher.finalize())[..10].to_string();
        self.invites.insert(code, (user_id, cycles));
        Ok(())
    }

    fn critical<T: ToString>(&mut self, message: T) {
        self.logger
            .log(&message.to_string(), "CRITICAL".to_string());
        self.users.values_mut().for_each(|user| {
            user.notify(format!("CRITICAL SYSTEM ERROR: {}", message.to_string()))
        });
    }

    fn notify_with_predicate<T: AsRef<str>>(
        &mut self,
        filter: &dyn Fn(&User) -> bool,
        message: T,
        predicate: Predicate,
    ) {
        self.users
            .values_mut()
            .filter(|u| filter(u))
            .for_each(|u| u.notify_with_params(&message, Some(predicate.clone())));
    }

    pub fn notify_users<T: AsRef<str>>(&mut self, filter: &dyn Fn(&User) -> bool, message: T) {
        self.users
            .values_mut()
            .filter(|u| filter(u))
            .for_each(|u| u.notify(&message));
    }

    pub fn denotify_users(&mut self, filter: &dyn Fn(&User) -> bool) {
        let posts = &self.posts;
        let last_proposal_open = self
            .proposals
            .iter()
            .last()
            .map(|p| p.status == Status::Open)
            .unwrap_or_default();
        self.users
            .values_mut()
            .filter(|u| filter(u))
            .for_each(|user| {
                user.inbox.retain(|_, n| {
                    if let Notification::Conditional(_, predicate) = n {
                        return match predicate {
                            Predicate::ReportOpen(post_id) => posts
                                .get(post_id)
                                .and_then(|p| p.report.as_ref().map(|r| !r.closed))
                                .unwrap_or_default(),
                            Predicate::ProposalPending => last_proposal_open,
                        };
                    }
                    true
                })
            });
    }

    pub fn search(&self, principal: Principal, mut term: String) -> Vec<SearchResult> {
        const SNIPPET_LEN: usize = 100;
        term = term.to_lowercase();
        let boddy_snippet = |body: &str, i: usize| {
            if body.len() < SNIPPET_LEN {
                body.to_string()
            } else {
                body.chars()
                    .skip(i.saturating_sub(SNIPPET_LEN / 2))
                    .skip_while(|c| c.is_alphanumeric())
                    .take(SNIPPET_LEN)
                    .skip_while(|c| c.is_alphanumeric())
                    .collect::<String>()
            }
            .replace('\n', " ")
        };
        self.users
            .iter()
            .filter_map(|(id, User { name, about, .. })| {
                if format!("@{} {0} {} {}", name, id, about)
                    .to_lowercase()
                    .contains(&term)
                {
                    return Some(SearchResult {
                        id: *id,
                        user_id: 0,
                        relevant: about.clone(),
                        result: "user".to_string(),
                    });
                }
                None
            })
            .chain(
                self.recent_tags(principal, 500)
                    .into_iter()
                    .filter_map(|(tag, _)| {
                        if format!("#{} {0}", tag).to_lowercase().contains(&term) {
                            return Some(SearchResult {
                                id: 0,
                                user_id: 0,
                                relevant: tag,
                                result: "tag".to_string(),
                            });
                        }
                        None
                    }),
            )
            .chain(self.last_posts(principal, true).filter_map(
                |Post { id, body, user, .. }| {
                    if id.to_string() == term {
                        return Some(SearchResult {
                            id: *id,
                            user_id: *user,
                            relevant: boddy_snippet(body, 0),
                            result: "post".to_string(),
                        });
                    }
                    let search_body = body.to_lowercase();
                    if let Some(i) = search_body.find(&term) {
                        return Some(SearchResult {
                            id: *id,
                            user_id: *user,
                            relevant: boddy_snippet(body, i),
                            result: "post".to_string(),
                        });
                    }
                    None
                },
            ))
            .take(100)
            .collect()
    }

    async fn top_up(&mut self) {
        let children = self.storage.buckets.keys().cloned().collect::<Vec<_>>();

        // top up the main canister
        let balance = canister_balance();
        let target_balance =
            CONFIG.min_cycle_balance_main + children.len() as u64 * ICP_CYCLES_PER_XDR;
        if balance < target_balance {
            let xdrs = target_balance / ICP_CYCLES_PER_XDR;
            // subtract weekly burned cycles to reduce the revenue
            self.spend(xdrs as i64 * 1000, "canister top up");
            match invoices::topup_with_icp(&api::id(), xdrs).await {
                Err(err) => self.critical(format!(
                    "FAILED TO TOP UP THE MAIN CANISTER — {}'S FUNCTIONALITY IS ENDANGERED: {:?}",
                    CONFIG.name.to_uppercase(),
                    err
                )),
                Ok(_cycles) => self.logger.info(format!(
                    "The main canister was topped up with cycles (balance was `{}`, now `{}`).",
                    balance,
                    canister_balance()
                )),
            }
        }

        // top up all children canisters
        let mut topped_up = Vec::new();
        for canister_id in children {
            match crate::canisters::top_up(canister_id, ICP_CYCLES_PER_XDR).await {
                Ok(true) => topped_up.push(canister_id),
                Err(err) => self.critical(err),
                _ => {}
            }
        }
        if !topped_up.is_empty() {
            self.logger.info(format!(
                "Topped up canisters: {:?}.",
                topped_up
                    .into_iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
            ))
        }
    }

    pub fn distribute_revenue(&mut self, e8s_for_1000_kps: u64) -> HashMap<UserId, u64> {
        let burned_cycles = self.burned_cycles;
        if burned_cycles <= 0 {
            return Default::default();
        }
        let active_user_balances = self
            .balances
            .iter()
            .filter_map(|(acc, balance)| {
                let user = self.principal_to_user(acc.owner)?;
                if user.active_within_weeks(time(), CONFIG.revenue_share_activity_weeks) {
                    return Some((user.id, *balance));
                }
                None
            })
            .collect::<Vec<_>>();
        let supply_of_active_users: u64 = active_user_balances
            .iter()
            .map(|(_, balance)| balance)
            .sum();
        active_user_balances
            .into_iter()
            .map(|(user_id, balance)| {
                let revenue_share =
                    burned_cycles as f64 * balance as f64 / supply_of_active_users as f64;
                let e8s = (revenue_share / 1000.0 * e8s_for_1000_kps as f64) as u64;
                (user_id, e8s)
            })
            .collect()
    }

    pub fn mint(&mut self, rewards: HashMap<UserId, Karma>) {
        let mut minted_tokens = 0;
        let mut minters = Vec::new();
        let circulating_supply: Token = self.balances.values().sum();
        let base = 10_u64.pow(CONFIG.token_decimals as u32);
        let factor = (circulating_supply as f64 / CONFIG.total_supply as f64 * 10.0) as u64;
        if circulating_supply < CONFIG.total_supply {
            for (user_id, user_karma) in rewards {
                let user = match self.users.get_mut(&user_id) {
                    Some(user) => user,
                    _ => continue,
                };
                let acc = account(user.principal);
                let minted = (user_karma.max(0) as u64 / (1 << factor)).max(1) * base;
                user.notify(format!(
                    "{} minted `{}` ${} tokens for you! 💎",
                    CONFIG.name,
                    minted / base,
                    CONFIG.token_symbol,
                ));
                minters.push(format!("`{}` to @{}", minted / base, user.name));
                crate::token::mint(self, acc, minted);
                minted_tokens += minted / base;
            }

            // Mint team tokens
            for user in [0, 305]
                .iter()
                .filter_map(|id| self.users.get(id).cloned())
                .collect::<Vec<_>>()
            {
                let acc = account(user.principal);
                let vested = match self.team_tokens.get_mut(&user.id) {
                    Some(balance) if *balance > 0 => {
                        // 1% of circulating supply is vesting.
                        let vested = (circulating_supply / 100).min(*balance);
                        let veto_threshold = 100 - CONFIG.proposal_approval_threshold as u64;
                        let veto_power = (circulating_supply * veto_threshold) / 100;
                        // Vesting is allowed if the total voting power of the team member is below
                        // 1/2 of the veto power, or if 2/3 of total supply is minted.
                        if self.balances.get(&acc).copied().unwrap_or_default() < veto_power / 2
                            || circulating_supply * 2 > CONFIG.total_supply
                        {
                            *balance -= vested;
                            Some((vested, *balance))
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                if let Some((vested, remaining_balance)) = vested {
                    crate::token::mint(self, acc, vested);
                    self.logger.info(format!(
                        "Minted `{}` team tokens for @{} (still vesting: `{}`).",
                        vested / 100,
                        user.name,
                        remaining_balance / 100
                    ));
                }
            }
        }
        if minters.is_empty() {
            self.logger.info("no tokens were minted".to_string());
        } else {
            let ratio = 1 << factor;
            self.logger.info(format!(
                "{} minted `{}` ${} tokens 💎 from users' earned karma at the ratio `{}:1` as follows: {}",
                CONFIG.name,
                minted_tokens,
                CONFIG.token_symbol,
                ratio,
                minters.join(", ")
            ));
        }
    }

    pub fn distribute_rewards(&mut self, e8s_for_1000_kps: u64) -> HashMap<UserId, u64> {
        for user in self.users.values_mut() {
            user.ledger.clear();
        }
        self.users
            .values_mut()
            .filter(|u| u.karma_to_reward() > 0)
            .filter_map(|user| {
                if user.karma() < 0 {
                    return None;
                }
                let e8s = (user.karma_to_reward() as f64 / 1000.0 * e8s_for_1000_kps as f64) as u64;
                Some((user.id, e8s))
            })
            .collect()
    }

    pub async fn icp_transfer(
        &self,
        principal: Principal,
        recipient: String,
        amount: String,
    ) -> Result<(), String> {
        fn parse(amount: &str) -> Result<Tokens, String> {
            let parse = |s: &str| {
                s.parse::<u64>()
                    .map_err(|err| format!("Couldn't parse as u64: {:?}", err))
            };
            match &amount.split('.').collect::<Vec<_>>().as_slice() {
                [icpts] => Ok(Tokens::from_e8s(parse(icpts)? * 10_u64.pow(8))),
                [icpts, e8s] => {
                    let mut e8s = e8s.to_string();
                    while e8s.len() < 8 {
                        e8s.push('0');
                    }
                    let e8s = &e8s[..8];
                    Ok(Tokens::from_e8s(
                        parse(icpts)? * 10_u64.pow(8) + parse(e8s)?,
                    ))
                }
                _ => Err(format!("Can't parse amount {}", amount)),
            }
        }
        invoices::transfer(
            &recipient,
            parse(&amount)?,
            Some(principal_to_subaccount(&principal)),
        )
        .await
        .map(|_| ())
    }

    async fn distribute_icp(
        &mut self,
        rewards: HashMap<UserId, u64>,
        revenue: HashMap<UserId, u64>,
    ) -> HashMap<UserId, Karma> {
        let mut user_ids = Default::default();
        let treasury_balance = invoices::main_account_balance().await.e8s();
        let total_payout =
            rewards.values().copied().sum::<u64>() + revenue.values().copied().sum::<u64>();
        if treasury_balance < total_payout {
            self.logger
                .info("Treasury is too small, skipping the distributions...");
            return user_ids;
        }
        let mut payments = Vec::default();
        let bootcampers = self
            .users
            .values()
            .filter_map(|u| (!u.trusted()).then_some(u.id))
            .collect::<HashSet<_>>();
        let mut user_rewards = 0;
        let mut user_revenues = 0;
        for user in self.users.values_mut().filter(|u| u.account.len() == 64) {
            let user_reward = rewards.get(&user.id).copied().unwrap_or_default();
            let user_revenue = revenue.get(&user.id).copied().unwrap_or_default();
            let e8s = user_reward + user_revenue;
            if e8s < invoices::fee() * 100 {
                continue;
            }
            match invoices::transfer(&user.account, Tokens::from_e8s(e8s), None).await {
                Ok(_) => {
                    user_rewards += user_reward;
                    user_revenues += user_revenue;
                    user_ids.insert(user.id, user.karma_to_reward());
                    user.apply_rewards();
                    payments.push(format!("`{}` to @{}", e8s_to_icp(e8s), &user.name));
                    user.notify(format!(
                        "You received `{}` ICP as rewards and `{}` ICP as revenue! 💸",
                        e8s_to_icp(user_reward),
                        e8s_to_icp(user_revenue)
                    ));
                }
                Err(err) => {
                    self.logger
                        .error(format!("Couldn't transfer ICP to @{}: {}", &user.name, err));
                }
            }
        }
        self.spend(self.burned_cycles, "revenue distribution");
        self.burned_cycles_total += self.burned_cycles;
        self.total_rewards_shared += user_rewards;
        self.total_revenue_shared += user_revenues;
        self.logger.info(format!(
            "Paid out `{}` ICP as rewards and `{}` ICP as revenue as follows: {}",
            e8s_to_icp(user_rewards),
            e8s_to_icp(user_revenues),
            payments.join(", ")
        ));
        let mut graduation_list = Vec::new();
        for user in self
            .users
            .values_mut()
            .filter_map(|u| (u.trusted() && bootcampers.contains(&u.id)).then_some(u))
        {
            graduation_list.push(format!("@{}", user.name));
            user.notify(
                "Congratulation! 🎉 You graduated from the bootcamp and became a trusted user!",
            );
        }
        if !graduation_list.is_empty() {
            self.logger.info(format!(
                "These users graduated from the bootcamp 🎉: {}",
                graduation_list.join(", ")
            ));
        }
        user_ids
    }

    pub async fn chores(&mut self, now: u64) {
        if now - self.last_chores < CONFIG.chores_interval_hours {
            return;
        }
        self.last_chores += CONFIG.chores_interval_hours;

        self.top_up().await;

        if now - self.last_distribution >= CONFIG.distribution_interval_hours
            // We only mint and distribute if no open proposals exists
            && self.proposals.iter().all(|p| p.status != Status::Open)
        {
            self.last_distribution += CONFIG.distribution_interval_hours;

            self.clean_up();

            self.recompute_stalwarts(now);

            let user_ids = match invoices::get_xdr_in_e8s().await {
                Ok(e8s_for_1000_kps) => {
                    let rewards = self.distribute_rewards(e8s_for_1000_kps);
                    let revenues = self.distribute_revenue(e8s_for_1000_kps);
                    self.distribute_icp(rewards, revenues).await
                }
                Err(err) => {
                    self.logger
                        .error(format!("Couldn't fetch ICP/XDR rate: {:?}", err));
                    return;
                }
            };

            self.mint(user_ids);
        }

        for proposal_id in self
            .proposals
            .iter()
            .filter_map(|p| (p.status == Status::Open).then_some(p.id))
            .collect::<Vec<_>>()
        {
            if let Err(err) = proposals::execute_proposal(self, proposal_id, now).await {
                self.logger
                    .error(format!("Couldn't execute last proposal: {:?}", err));
            }
        }

        self.memory.report_health(&mut self.logger);
    }

    fn clean_up(&mut self) {
        let now = time();
        for user in self.users.values_mut() {
            if user.active_within_weeks(now, 1) {
                user.active_weeks += 1;
            } else {
                user.active_weeks = 0;
            }
            let inactive = !user.active_within_weeks(now, CONFIG.inactivity_duration_weeks);
            if inactive || user.is_bot() {
                user.clear_notifications(Vec::new())
            }
            if inactive && user.karma() > 0 {
                user.change_karma(
                    -CONFIG.inactivity_penalty.min(user.karma()),
                    "inactivity_penalty".to_string(),
                );
            }
        }
        for (id, cycles) in self
            .users
            .values()
            .filter(|user| {
                !user.active_within_weeks(now, CONFIG.inactivity_duration_weeks)
                    && user.cycles() > 0
            })
            .map(|u| (u.id, u.cycles()))
            .collect::<Vec<_>>()
        {
            if let Err(err) = self.charge(
                id,
                CONFIG.inactivity_penalty.min(cycles),
                "inactivity penalty".to_string(),
            ) {
                self.logger
                    .error(format!("Couldn't charge inactivity penalty: {:?}", err));
            };
        }

        self.accounting.clean_up();
    }

    fn recompute_stalwarts(&mut self, now: u64) {
        let mut users = self.users.values_mut().into_iter().collect::<Vec<_>>();
        users.sort_unstable_by_key(|a| std::cmp::Reverse(a.karma()));

        let mut stalwart_seats = users.len() * CONFIG.stalwart_percentage / 100;
        let mut left = Vec::new();
        let mut joined = Vec::new();
        for u in users {
            // Bots or new users are filtered out.
            if u.is_bot()
                || now.saturating_sub(u.timestamp)
                    < WEEK * CONFIG.min_stalwart_account_age_weeks as u64
            {
                u.stalwart = false;
                continue;
            }
            match (
                u.stalwart,
                u.active_weeks >= CONFIG.min_stalwart_activity_weeks as u32,
                stalwart_seats,
            ) {
                // A user is qualified and is already a stalwart but no seats available
                (true, true, 0) => {
                    u.stalwart = false;
                    left.push(format!("@{} (karma)", u.name));
                }
                // A user is qualified and is already a stalwart and seats available
                (true, true, _) => {
                    stalwart_seats = stalwart_seats.saturating_sub(1);
                }
                // A user is a stalwart but became inactive
                (true, false, _) => {
                    u.stalwart = false;
                    left.push(format!("@{} (inactivity)", u.name));
                }
                // A user is not a stalwart, but qualified and there are seats left
                (false, true, seats) if seats > 0 => {
                    u.stalwart = true;
                    joined.push(format!("@{}", u.name));
                    stalwart_seats = stalwart_seats.saturating_sub(1);
                    u.notify(format!(
                        "Congratulations! You are a {} stalwart now!",
                        CONFIG.name
                    ));
                }
                _ => {}
            };
        }

        self.logger.info(format!(
            "Weekly stalwart election ⚔️: {} joined; {} have left; `{}` seats vacant.",
            if joined.is_empty() {
                "no new users".to_string()
            } else {
                joined.join(", ")
            },
            if left.is_empty() {
                "no users".to_string()
            } else {
                left.join(", ")
            },
            stalwart_seats
        ));
    }

    pub async fn mint_cycles(
        &mut self,
        principal: Principal,
        kilo_cycles: u64,
    ) -> Result<Invoice, String> {
        let invoice = match self.accounting.outstanding(&principal, kilo_cycles).await {
            Ok(val) => val,
            Err(err) => {
                if kilo_cycles == 0 {
                    self.logger
                        .error(&format!("Couldn't generate invoice: {:?}", err));
                }
                return Err(err);
            }
        };
        let min_cycles_minted = CONFIG.min_cycles_minted;
        if invoice.paid {
            if let Some(user) = self.principal_to_user_mut(principal) {
                user.change_cycles(
                    ((invoice.paid_e8s as f64 / invoice.e8s as f64) * min_cycles_minted as f64)
                        as i64,
                    "top up with ICP".to_string(),
                )?;
                let user_name = user.name.clone();
                self.accounting.close(&principal);
                self.logger.info(format!(
                    "@{} minted cycles for `{}` ICP 💰",
                    user_name,
                    e8s_to_icp(invoice.paid_e8s)
                ));
            }
        }
        Ok(invoice)
    }

    pub fn clear_notifications(&mut self, principal: Principal, ids: Vec<String>) {
        if let Some(user) = self.principal_to_user_mut(principal) {
            user.clear_notifications(ids)
        }
    }

    pub fn validate_username(&self, name: &str) -> Result<(), String> {
        let name = name.to_lowercase();
        if self
            .users
            .values()
            .any(|user| user.name.to_lowercase() == name)
        {
            return Err("taken".into());
        }
        if name.len() < 2 || name.len() > 16 {
            return Err("should be between 2 and 16 characters".into());
        }
        if !name
            .chars()
            .all(|c| char::is_ascii(&c) && char::is_alphanumeric(c))
        {
            return Err("should be a latin alpha-numeric string".into());
        }
        if name
            .chars()
            .next()
            .map(|c| !char::is_ascii_alphabetic(&c))
            .unwrap_or_default()
        {
            return Err("first character can't be a number".into());
        }
        if name.chars().all(|c| char::is_ascii_digit(&c)) {
            return Err("should have at least one character".into());
        }
        if ["all", "stalwarts", "dao"].contains(&name.as_str()) {
            return Err("reserved handle".into());
        }
        Ok(())
    }

    pub fn posts_by_tags(
        &self,
        principal: Principal,
        tags: Vec<String>,
        users: Vec<UserId>,
        page: usize,
    ) -> Vec<Post> {
        let query: HashSet<_> = tags.into_iter().map(|tag| tag.to_lowercase()).collect();
        self.last_posts(principal, true)
            .filter(|post| {
                (users.is_empty() || users.contains(&post.user))
                    && post
                        .tags
                        .iter()
                        .map(|tag| tag.to_lowercase())
                        .collect::<HashSet<_>>()
                        .is_superset(&query)
            })
            .skip(page * CONFIG.feed_page_size)
            .take(CONFIG.feed_page_size)
            .cloned()
            .collect()
    }

    pub fn last_posts<'a>(
        &'a self,
        principal: Principal,
        with_comments: bool,
    ) -> Box<dyn Iterator<Item = &'a Post> + 'a> {
        let posts: Box<dyn Iterator<Item = PostId>> = match self
            .principal_to_user(principal)
            .and_then(|user| user.current_realm.as_ref())
            .and_then(|id| self.realms.get(id))
        {
            Some(realm) => Box::new(realm.posts.iter().cloned().rev()),
            None => {
                let last_id = self.next_post_id.saturating_sub(1);
                Box::new((0..=last_id).rev())
            }
        };
        Box::new(
            posts
                .filter_map(move |i| self.posts.get(&i))
                .filter(move |post| with_comments || post.parent.is_none()),
        )
    }

    pub fn recent_tags(&self, principal: Principal, n: u64) -> Vec<(String, u64)> {
        let mut tags: HashMap<String, (String, u64)> = Default::default();
        let mut tags_found = 0;
        'OUTER: for post in self.last_posts(principal, true) {
            for tag in &post.tags {
                let entry = tags.entry(tag.to_lowercase()).or_insert((tag.clone(), 0));
                entry.1 += 1;
                if entry.1 == 2 {
                    tags_found += 1;
                }
            }
            if tags_found >= n {
                break 'OUTER;
            }
        }
        // Don't display taggr, it's useless
        tags.remove("taggr");
        tags.into_iter()
            .map(|v| v.1)
            .filter(|(_, count)| *count > 1)
            .collect()
    }

    /// Returns an iterator of posts from the root post to the post `id`.
    pub fn thread(&self, id: PostId) -> Box<dyn Iterator<Item = PostId>> {
        let mut result = Vec::new();
        let mut curr = id;
        while let Some(Post { id, parent, .. }) = self.posts.get(&curr) {
            result.push(*id);
            if let Some(parent_id) = parent {
                curr = *parent_id
            } else {
                break;
            }
        }
        Box::new(result.into_iter().rev())
    }

    pub fn posts(&self, ids: Vec<PostId>) -> Vec<Post> {
        ids.iter()
            .filter_map(|id| self.posts.get(id).cloned())
            .collect()
    }

    pub fn user(&self, handle: &str) -> Option<&User> {
        handle
            .parse::<u64>()
            .ok()
            .and_then(|id| self.users.get(&id))
            .or_else(|| {
                self.users
                    .values()
                    .find(|user| user.name.to_lowercase() == handle.to_lowercase())
            })
    }

    pub fn change_principal(
        &mut self,
        principal: Principal,
        new_principal_str: String,
    ) -> Result<(), String> {
        let new_principal = Principal::from_text(new_principal_str).map_err(|e| e.to_string())?;
        if self.principals.contains_key(&new_principal) {
            return Err("principal already controls a user".into());
        }
        let user = self.principals.remove(&principal).ok_or("no user found")?;
        self.principals.insert(new_principal, user);
        let accounts = self
            .balances
            .keys()
            .filter(|acc| acc.owner == principal)
            .cloned()
            .collect::<Vec<_>>();
        for acc in accounts {
            crate::token::move_funds(self, &acc, account(new_principal))
                .map_err(|err| format!("couldn't transfer token funds: {:?}", err))?;
        }
        Ok(())
    }

    pub fn principal_to_user(&self, principal: Principal) -> Option<&User> {
        self.principals
            .get(&principal)
            .and_then(|id| self.users.get(id))
    }

    pub fn principal_to_user_mut(&mut self, principal: Principal) -> Option<&mut User> {
        let id = self.principals.get(&principal)?;
        self.users.get_mut(id)
    }

    fn new_user_id(&mut self) -> UserId {
        let id = self.next_user_id;
        self.next_user_id += 1;
        id
    }

    fn new_post_id(&mut self) -> PostId {
        let id = self.next_post_id;
        self.next_post_id += 1;
        id
    }

    pub fn logs(&self) -> &Vec<Event> {
        &self.logger.events
    }

    pub fn stats(&self, now: u64) -> Stats {
        let cycles: Cycles = self.users.values().map(|u| u.cycles()).sum();
        let posts = self.posts.values().filter(|p| p.parent.is_none()).count();
        let mut stalwarts = self
            .users
            .values()
            .filter(|u| u.stalwart)
            .collect::<Vec<_>>();
        let mut weekly_karma_leaders = self
            .users
            .values()
            .map(|u| (u.id, u.karma_to_reward()))
            .collect::<Vec<_>>();
        weekly_karma_leaders.sort_unstable_by_key(|k| k.1);
        weekly_karma_leaders = weekly_karma_leaders.into_iter().rev().take(24).collect();
        stalwarts.sort_unstable_by_key(|u1| std::cmp::Reverse(u1.karma()));
        Stats {
            meta: format!("Team tokens to mint: {:?}", &self.team_tokens),
            weekly_karma_leaders,
            bootcamp_users: self.users.values().filter(|u| !u.trusted()).count(),
            module_hash: self.module_hash.clone(),
            canister_id: ic_cdk::id(),
            last_upgrade: self.last_upgrade,
            last_chores: self.last_chores,
            canister_cycle_balance: canister_balance(),
            users: self.users.len(),
            posts,
            comments: self.posts.len() - posts,
            cycles,
            burned_cycles: self.burned_cycles,
            burned_cycles_total: self.burned_cycles_total,
            total_revenue_shared: self.total_revenue_shared,
            total_rewards_shared: self.total_rewards_shared,
            account: invoices::main_account().to_string(),
            last_distribution: self.last_distribution,
            users_online: self
                .users
                .values()
                .filter(|u| now - u.last_activity < CONFIG.online_activity_minutes)
                .count(),
            stalwarts: stalwarts.into_iter().map(|u| u.id).collect(),
            bots: self
                .users
                .values()
                .filter(|u| u.is_bot())
                .map(|u| u.id)
                .collect(),
            state_size: stable64_size() << 16,
            invited_users: self
                .users
                .values()
                .filter(|u| u.invited_by.is_some())
                .count(),
            active_users: self
                .users
                .values()
                .filter(|u| u.active_within_weeks(now, 1))
                .count(),
            buckets: self
                .storage
                .buckets
                .iter()
                .map(|(id, size)| (id.to_string(), *size))
                .collect(),
            circulating_supply: self.balances.values().sum(),
        }
    }

    pub fn vote_on_poll(
        &mut self,
        principal: Principal,
        time: u64,
        post_id: PostId,
        vote: u16,
    ) -> Result<(), String> {
        let user = self
            .principal_to_user(principal)
            .ok_or_else(|| "no user found".to_string())?;
        let (user_id, user_realms) = (user.id, user.realms.clone());
        self.posts
            .get_mut(&post_id)
            .ok_or_else(|| "no post found".to_string())?
            .vote_on_poll(user_id, user_realms, time, vote)
    }

    pub fn report(
        &mut self,
        principal: Principal,
        post_id: PostId,
        reason: String,
    ) -> Result<(), String> {
        let cycles_required = CONFIG.reporting_penalty / 2;
        let user = match self.principal_to_user(principal) {
            Some(user) if user.cycles() >= cycles_required => user.clone(),
            _ => {
                return Err(format!(
                    "You need at least {} cycles to report a post",
                    cycles_required
                ))
            }
        };
        let post = match self.posts.get_mut(&post_id) {
            Some(post) => {
                if post.report.is_some() {
                    return Err("This post is already reported".into());
                }
                post
            }
            _ => return Err("No post found".into()),
        };
        post.report(user.id, reason);
        let author_name = self
            .users
            .get(&post.user)
            .map(|user| user.name.clone())
            .unwrap_or_default();
        self.notify_with_predicate(
            &|u| u.stalwart && u.id != user.id,
            format!("@{} reported this post by @{}", user.name, author_name),
            Predicate::ReportOpen(post_id),
        );
        Ok(())
    }

    pub fn delete_post(
        &mut self,
        principal: Principal,
        post_id: PostId,
        versions: Vec<String>,
    ) -> Result<(), String> {
        let post = self.posts.get(&post_id).ok_or("no post found")?.clone();
        if self.principal_to_user(principal).map(|user| user.id) != Some(post.user) {
            return Err("not authorized".into());
        }

        let comments_tree_penalty =
            post.tree_size as Cycles * CONFIG.post_deletion_penalty_factor as Cycles;
        let reaction_costs = post
            .reactions
            .iter()
            .filter_map(|(r_id, users)| {
                CONFIG
                    .reactions
                    .iter()
                    .find(|(id, cost)| id == r_id && *cost > 0)
                    .map(|(_, cost)| (users, *cost))
            })
            .collect::<Vec<_>>();

        let costs: Cycles = CONFIG.post_cost
            + reaction_costs.iter().map(|(_, cost)| *cost).sum::<i64>()
            + comments_tree_penalty;
        if costs > self.users.get(&post.user).ok_or("no user found")?.cycles() {
            return Err(format!(
                "not enough cycles (this post requires {} cycles to be deleted)",
                costs
            ));
        }

        let mut karma_penalty = post.children.len() as i64 * CONFIG.response_reward;

        // refund rewards
        for (users, amount) in reaction_costs {
            for user_id in users {
                self.cycle_transfer(
                    post.user,
                    *user_id,
                    amount,
                    0,
                    Destination::Cycles,
                    format!("rewards refund after deletion of post {}", post.id),
                )?;
                karma_penalty += amount;
            }
        }

        // penalize for comments tree destruction
        self.charge(
            post.user,
            CONFIG.post_cost + comments_tree_penalty,
            format!("deletion of post {}", post.id),
        )?;

        // subtract all rewards from karma
        self.users
            .get_mut(&post.user)
            .expect("no user found")
            .change_karma(-karma_penalty, format!("deletion of post {}", post.id));

        self.posts
            .get_mut(&post_id)
            .expect("no post found")
            .delete(versions);
        Ok(())
    }

    pub fn react(
        &mut self,
        principal: Principal,
        post_id: PostId,
        reaction: u16,
        time: u64,
    ) -> Result<(), String> {
        let delta: i64 = match CONFIG.reactions.iter().find(|(id, _)| id == &reaction) {
            Some((_, delta)) => *delta,
            _ => return Err("unknown reaction".into()),
        };
        let user = self
            .principal_to_user(principal)
            .ok_or("no user for principal found")?
            .clone();
        let post = self.posts.get(&post_id).ok_or("post not found")?.clone();
        if post.user == user.id {
            return Err("reactions to own posts are forbidden".into());
        }
        if let Some(set) = post.reactions.get(&reaction) {
            if set.contains(&user.id) {
                return Err("double reactions are forbidden".into());
            }
        }

        let log = format!("reaction to post {}", post_id);
        // If the user is untrusted, they can only upvote, but this does not affect author's karma.
        if !user.trusted() {
            if delta < 0 {
                return Err("bootcamp users can't downvote".into());
            }
            self.charge(user.id, delta.abs() + CONFIG.reaction_fee, log)
                .expect("coudln't charge user");
        }
        // If the user is trusted, they initiate a cycle transfer for upvotes, but burn their own cycles on
        // down votes + cycles and karma of the author
        else if delta < 0 {
            self.users
                .get_mut(&post.user)
                .expect("user not found")
                .change_karma(delta, log.clone());
            self.charge(user.id, delta.abs().min(user.cycles()), log.clone())?;
            self.charge(
                post.user,
                delta
                    .abs()
                    .min(self.users.get(&post.user).expect("no user found").cycles()),
                log,
            )
            .expect("couldn't charge user");
        } else {
            self.cycle_transfer(
                user.id,
                post.user,
                delta,
                CONFIG.reaction_fee,
                Destination::Karma,
                log,
            )?;
            post.make_hot(&mut self.hot, self.users.len(), user.id);
        }

        self.principal_to_user_mut(principal)
            .expect("no user for principal found")
            .last_activity = time;
        let user_id = user.id;
        let post = self.posts.get_mut(&post_id).expect("no post found");
        post.reactions.entry(reaction).or_default().insert(user_id);
        Ok(())
    }

    pub fn toggle_following_user(&mut self, principal: Principal, followee_id: UserId) -> bool {
        let (added, (id, name)) = {
            let user = match self.principal_to_user_mut(principal) {
                Some(user) => user,
                _ => return false,
            };
            (
                if user.followees.contains(&followee_id) {
                    user.followees.remove(&followee_id);
                    false
                } else {
                    user.followees.insert(followee_id);
                    true
                },
                (user.id, user.name.clone()),
            )
        };
        let followee = self.users.get_mut(&followee_id).expect("User not found");
        if added {
            followee.followers.insert(id);
            followee.notify(format!("@{} followed you", name));
        } else {
            followee.followers.remove(&id);
        }
        added
    }

    pub fn toggle_following_post(&mut self, principal: Principal, post_id: PostId) -> bool {
        let user_id = match self.principal_to_user(principal) {
            Some(user) => user.id,
            _ => return false,
        };
        let post = self.posts.get_mut(&post_id).expect("No post found");
        if post.watchers.contains(&user_id) {
            post.watchers.remove(&user_id);
            return false;
        }
        post.watchers.insert(user_id);
        true
    }
}

// Extracts hashtags from a string.
fn tags(max_tag_length: usize, input: &str) -> BTreeSet<String> {
    tokens(max_tag_length, input, &['#', '$'])
}

// Extracts user names from a string.
fn user_handles(max_tag_length: usize, input: &str) -> BTreeSet<String> {
    tokens(max_tag_length, input, &['@'])
}

fn tokens(max_tag_length: usize, input: &str, tokens: &[char]) -> BTreeSet<String> {
    use std::iter::FromIterator;
    let mut tags = Vec::new();
    let mut tag = Vec::new();
    let mut token_found = false;
    let mut whitespace_seen = true;
    for c in input.chars() {
        match c {
            t if whitespace_seen && tokens.contains(&t) => {
                token_found = true;
            }
            c if token_found => {
                if c.is_alphanumeric() || ['-', '_'].iter().any(|v| v == &c) {
                    tag.push(c);
                } else {
                    tags.push(String::from_iter(tag.clone()));
                    tag.clear();
                    token_found = false;
                }
            }
            _ => {}
        }
        whitespace_seen = c == ' ' || c == '\n' || c == '\t';
    }
    tags.push(String::from_iter(tag));
    tags.into_iter()
        .filter(|tag| {
            let l = tag.chars().count();
            l > 0 && l <= max_tag_length
        })
        .collect::<BTreeSet<_>>()
}

// Checks if any feed represents the superset for the given tag set.
// The `strict` option requires the sets to be equal.
fn covered_by_feeds(
    feeds: &[BTreeSet<String>],
    tags: &BTreeSet<String>,
    strict: bool,
) -> Option<usize> {
    for (i, feed) in feeds.iter().enumerate() {
        if strict && tags.len() != feed.len() {
            continue;
        }
        if feed.iter().all(|tag| tags.contains(tag)) {
            return Some(i);
        }
    }
    None
}

struct IteratorMerger<'a, T> {
    iterators: Vec<std::iter::Peekable<Box<dyn Iterator<Item = &'a T> + 'a>>>,
}

impl<'a, T: Clone + PartialOrd> Iterator for IteratorMerger<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut max_val = None;
        let mut indexes = vec![];
        for (i, iter) in self.iterators.iter_mut().enumerate() {
            let candidate = iter.peek().cloned().cloned();
            if candidate == max_val {
                indexes.push(i);
            } else if candidate > max_val {
                max_val = candidate;
                indexes = vec![i];
            }
        }
        max_val.as_ref()?;
        indexes.into_iter().for_each(|i| {
            self.iterators[i].next();
        });
        max_val
    }
}

pub fn id() -> Principal {
    #[cfg(test)]
    return Principal::anonymous();
    #[cfg(not(test))]
    ic_cdk::id()
}

pub fn time() -> u64 {
    #[cfg(test)]
    return CONFIG.trusted_user_min_age_weeks * WEEK + 1;
    #[cfg(not(test))]
    api::time()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use post::{add, edit};

    pub fn pr(n: u8) -> Principal {
        let v = vec![n];
        Principal::from_slice(&v)
    }

    pub fn create_user(state: &mut State, p: Principal) -> UserId {
        create_user_with_params(state, p, &p.to_string().replace('-', ""), true)
    }

    pub fn create_untrusted_user(state: &mut State, p: Principal) -> UserId {
        create_user_with_params(state, p, &p.to_string().replace('-', ""), false)
    }

    fn create_user_with_params(
        state: &mut State,
        p: Principal,
        name: &str,
        trusted: bool,
    ) -> UserId {
        let id = state.new_user(p, 0, name.to_string());
        let u = state.users.get_mut(&id).unwrap();
        u.change_cycles(1000, "").unwrap();
        if trusted {
            u.change_karma(CONFIG.trusted_user_min_karma, "");
            u.apply_rewards();
        }
        id
    }

    #[test]
    fn test_principal_change() {
        let mut state = State::default();

        let mut eligigble = HashMap::default();
        for i in 1..3 {
            let p = pr(i);
            let id = create_user(&mut state, p);
            let user = state.users.get_mut(&id).unwrap();
            user.change_karma(i as Karma * 111, "test");
            assert_eq!(user.karma(), CONFIG.trusted_user_min_karma);
            assert!(user.trusted());
            eligigble.insert(id, user.karma_to_reward());
        }

        // mint tokens
        state.mint(eligigble);
        assert_eq!(state.ledger.len(), 2);
        assert_eq!(*state.balances.get(&account(pr(1))).unwrap(), 11100);

        let u_id = state.principal_to_user(pr(1)).unwrap().id;
        let new_principal_str: String =
            "yh4uw-lqajx-4dxcu-rwe6s-kgfyk-6dicz-yisbt-pjg7v-to2u5-morox-hae".into();
        assert!(state
            .change_principal(pr(1), new_principal_str.clone())
            .is_ok());
        let principal = Principal::from_text(new_principal_str).unwrap();
        assert_eq!(state.principal_to_user(principal).unwrap().id, u_id);
        assert!(state.balances.get(&account(pr(1))).is_none());
        assert_eq!(
            *state.balances.get(&account(principal)).unwrap(),
            11100 - CONFIG.transaction_fee
        );
    }

    #[actix_rt::test]
    async fn test_post_deletion() {
        let mut state = State::default();

        let id = create_user(&mut state, pr(0));
        let user = state.users.get_mut(&id).unwrap();
        assert_eq!(user.karma_to_reward(), 0);
        let upvoter_id = create_user(&mut state, pr(1));
        let user = state.users.get_mut(&upvoter_id).unwrap();
        let upvoter_cycles = user.cycles();
        user.change_karma(1000, "test");
        assert!(user.trusted());
        let uid = create_user(&mut state, pr(2));
        state
            .users
            .get_mut(&uid)
            .unwrap()
            .change_karma(1000, "test");

        let post_id = add(
            &mut state,
            "Test".to_string(),
            vec![],
            pr(0),
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        // Create 2 comments
        for i in 1..=2 {
            add(
                &mut state,
                "Test".to_string(),
                vec![],
                pr(i),
                0,
                Some(post_id),
                None,
                None,
            )
            .await
            .unwrap();
        }

        // React from both users
        assert!(state.react(pr(1), post_id, 100, 0).is_ok());
        assert!(state.react(pr(2), post_id, 50, 0).is_ok());

        assert_eq!(
            state.users.get(&id).unwrap().karma_to_reward(),
            10 + 5 + 2 * CONFIG.response_reward
        );

        assert_eq!(
            state.users.get_mut(&upvoter_id).unwrap().cycles(),
            // reward + fee + post creation
            upvoter_cycles - 10 - 1 - 2
        );

        let versions = vec!["a".into(), "b".into()];
        assert_eq!(
            state.delete_post(pr(1), post_id, versions.clone()),
            Err("not authorized".into())
        );

        state
            .charge(id, state.users.get(&id).unwrap().cycles(), "")
            .unwrap();
        assert_eq!(
            state.delete_post(pr(0), post_id, versions.clone()),
            Err("not enough cycles (this post requires 37 cycles to be deleted)".into())
        );

        state
            .users
            .get_mut(&id)
            .unwrap()
            .change_cycles(1000, "")
            .unwrap();

        assert_eq!(&state.posts.get(&0).unwrap().body, "Test");
        assert_eq!(state.delete_post(pr(0), post_id, versions.clone()), Ok(()));
        assert_eq!(&state.posts.get(&0).unwrap().body, "");
        assert_eq!(state.posts.get(&0).unwrap().hashes.len(), versions.len());

        assert_eq!(
            state.users.get(&upvoter_id).unwrap().cycles(),
            // reward received back
            upvoter_cycles - 10 - 1 - 2 + 10
        );
        assert_eq!(state.users.get(&id).unwrap().karma_to_reward(), 0);
    }

    #[actix_rt::test]
    async fn test_realms() {
        let mut state = State::default();
        let p0 = pr(0);
        let p1 = pr(1);
        let _u0 = create_user_with_params(&mut state, p0, "user1", true);
        let _u1 = create_user_with_params(&mut state, p1, "user2", true);

        let user1 = state.users.get_mut(&_u1).unwrap();
        assert_eq!(user1.cycles(), 1000);
        user1.change_cycles(-500, "").unwrap();
        assert_eq!(user1.cycles(), 500);

        let name = "SYNAPSE".to_string();
        let description = "Test description".to_string();
        let controllers = vec![_u0];

        // simple creation and description change edge cases
        assert_eq!(
            state.create_realm(
                pr(2),
                name.clone(),
                Default::default(),
                Default::default(),
                description.clone(),
                controllers.clone()
            ),
            Err("no user found".to_string())
        );

        assert_eq!(
            state.create_realm(
                p1,
                name.clone(),
                Default::default(),
                Default::default(),
                description.clone(),
                controllers.clone()
            ),
            Err("couldn't charge 1000 cycles for realm creation: not enough cycles".to_string())
        );

        assert_eq!(
            state.create_realm(
                p0,
                "THIS_NAME_IS_TOO_LONG".to_string(),
                Default::default(),
                Default::default(),
                description.clone(),
                controllers.clone()
            ),
            Err("realm name too long".to_string())
        );

        assert_eq!(
            state.create_realm(
                p0,
                name.clone(),
                Default::default(),
                Default::default(),
                description.clone(),
                vec![]
            ),
            Err("no controllers specified".to_string())
        );

        assert_eq!(
            state.create_realm(
                p0,
                "TEST NAME".to_string(),
                Default::default(),
                Default::default(),
                description.clone(),
                controllers.clone()
            ),
            Err("realm name should be an alpha-numeric string".to_string(),)
        );

        assert_eq!(
            state.create_realm(
                p0,
                name.clone(),
                Default::default(),
                Default::default(),
                description.clone(),
                controllers.clone()
            ),
            Ok(())
        );

        let user0 = state.users.get_mut(&_u0).unwrap();
        user0.change_cycles(1000, "").unwrap();

        assert_eq!(
            state.create_realm(
                p0,
                name.clone(),
                Default::default(),
                Default::default(),
                description.clone(),
                controllers.clone()
            ),
            Err("realm name taken".to_string())
        );

        assert_eq!(
            state.realms.get(&name).unwrap().description,
            "Test description".to_string()
        );

        let new_description = "New test description".to_string();

        assert_eq!(
            state.edit_realm(
                pr(2),
                name.clone(),
                Default::default(),
                Default::default(),
                new_description.clone(),
                vec![]
            ),
            Err("no controllers specified".to_string())
        );

        assert_eq!(
            state.edit_realm(
                pr(2),
                name.clone(),
                Default::default(),
                Default::default(),
                new_description.clone(),
                controllers.clone()
            ),
            Err("no user found".to_string())
        );

        assert_eq!(
            state.edit_realm(
                p0,
                "WRONGNAME".to_string(),
                Default::default(),
                Default::default(),
                new_description.clone(),
                controllers.clone()
            ),
            Err("no realm found".to_string())
        );

        assert_eq!(
            state.edit_realm(
                p1,
                name.clone(),
                Default::default(),
                Default::default(),
                new_description.clone(),
                controllers.clone()
            ),
            Err("not authorized".to_string())
        );

        assert_eq!(
            state.edit_realm(
                p0,
                name.clone(),
                Default::default(),
                Default::default(),
                new_description.clone(),
                controllers.clone()
            ),
            Ok(())
        );

        assert_eq!(
            state.realms.get(&name).unwrap().description,
            new_description
        );

        // Entering a realm without joining does not work
        state.enter_realm(p1, name.clone());
        assert!(state.users.get(&_u1).unwrap().realms.is_empty());
        assert_eq!(state.users.get(&_u1).unwrap().current_realm, None);

        // wrong user and wrong realm joining
        assert!(!state.toggle_realm_membership(pr(2), name.clone()));
        assert!(!state.toggle_realm_membership(p1, "WRONGNAME".to_string()));

        assert!(state.toggle_realm_membership(p1, name.clone()));
        assert!(state.users.get(&_u1).unwrap().realms.contains(&name));
        assert_eq!(state.users.get(&_u1).unwrap().current_realm, None);

        state.enter_realm(p1, name.clone());
        assert_eq!(
            state.users.get(&_u1).unwrap().current_realm,
            Some(name.clone())
        );

        // creating a post in a realm
        let post_id = add(
            &mut state,
            "Realm post".to_string(),
            vec![],
            p1,
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(state.posts.get(&post_id).unwrap().realm, Some(name.clone()));
        assert!(state.realms.get(&name).unwrap().posts.contains(&post_id));

        // comments not possible if user is not in the realm
        assert_eq!(
            add(
                &mut state,
                "comment".to_string(),
                vec![],
                p0,
                0,
                Some(0),
                None,
                None
            )
            .await,
            Err("not a member of the realm SYNAPSE".to_string())
        );

        assert!(state.toggle_realm_membership(p0, name.clone()));
        assert_eq!(
            add(
                &mut state,
                "comment".to_string(),
                vec![],
                p0,
                0,
                Some(0),
                None,
                None
            )
            .await,
            Ok(1)
        );
        assert!(state.realms.get(&name).unwrap().posts.contains(&1));

        // Create post without a realm
        state.enter_realm(p1, Default::default());
        let post_id = add(
            &mut state,
            "No realm post".to_string(),
            vec![],
            p1,
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let comment_id = add(
            &mut state,
            "comment".to_string(),
            vec![],
            p0,
            0,
            Some(post_id),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(state.posts.get(&comment_id).unwrap().realm, None);

        // Creating post without entering the realm
        let realm_name = "NEW_REALM".to_string();
        assert_eq!(
            add(
                &mut state,
                "test".to_string(),
                vec![],
                p0,
                0,
                None,
                Some(realm_name.clone()),
                None
            )
            .await,
            Err(format!("not a member of the realm {}", realm_name))
        );

        // create a new realm
        let user0 = state.users.get_mut(&_u0).unwrap();
        user0.change_cycles(1000, "").unwrap();
        assert_eq!(
            state.create_realm(
                p0,
                realm_name.clone(),
                Default::default(),
                Default::default(),
                description,
                controllers
            ),
            Ok(())
        );

        // we still can't post into it, because we didn't join
        assert_eq!(
            add(
                &mut state,
                "test".to_string(),
                vec![],
                p0,
                0,
                None,
                Some(realm_name.clone()),
                None
            )
            .await,
            Err(format!("not a member of the realm {}", realm_name))
        );

        // join the realm and create the post without entering
        assert!(state.toggle_realm_membership(p1, realm_name.clone()));
        assert!(state.users.get(&_u1).unwrap().realms.contains(&name));
        assert_eq!(
            add(
                &mut state,
                "test".to_string(),
                vec![],
                p1,
                0,
                None,
                Some(realm_name.clone()),
                None
            )
            .await,
            Ok(4)
        );

        // Make sure the user is in SYNAPSE realm
        assert!(state
            .users
            .get(&_u1)
            .unwrap()
            .realms
            .contains(&"SYNAPSE".to_string()));

        // Move the post to non-joined realm
        assert_eq!(
            edit(
                &mut state,
                4,
                "changed".to_string(),
                vec![],
                "".to_string(),
                Some("SYNAPSE_X".to_string()),
                p1,
                time(),
            )
            .await,
            Err("you're not in the realm".into()),
        );

        // Move post to SYNAPSE realms
        assert_eq!(state.posts.get(&4).unwrap().realm, Some(realm_name));
        assert_eq!(
            edit(
                &mut state,
                4,
                "changed".to_string(),
                vec![],
                "".to_string(),
                Some("SYNAPSE".to_string()),
                p1,
                time(),
            )
            .await,
            Ok(())
        );
        assert_eq!(
            state.posts.get(&4).unwrap().realm,
            Some("SYNAPSE".to_string())
        );
    }

    #[test]
    #[should_panic]
    fn test_negative_transfer_panic() {
        let mut state = State::default();
        let p = pr(0);
        let u1 = create_user_with_params(&mut state, p, "user1", true);
        let u2 = create_user_with_params(&mut state, pr(1), "user2", true);
        state
            .cycle_transfer(u1, u2, -1, 0, Destination::Cycles, "")
            .unwrap();
    }

    #[actix_rt::test]
    async fn test_tipping() {
        let mut state = State::default();
        let p = pr(0);
        let u1 = create_user_with_params(&mut state, p, "user1", true);
        let u2 = create_user_with_params(&mut state, pr(1), "user2", true);
        let post_id = add(
            &mut state,
            "This is a #post with #tags".to_string(),
            vec![],
            p,
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let u = state.users.get_mut(&u1).unwrap();
        assert_eq!(u.karma(), CONFIG.trusted_user_min_karma);
        let cycles_before = u.cycles();

        assert_eq!(state.tip(pr(1), post_id, 500), Ok(()));
        assert_eq!(
            state.tip(pr(1), post_id, 600),
            Err("not enough cycles".into())
        );

        let u = state.users.get_mut(&u1).unwrap();
        assert_eq!(u.karma_to_reward(), 0);
        assert_eq!(u.cycles(), cycles_before + 500);
        let u = state.users.get_mut(&u2).unwrap();
        assert_eq!(u.cycles(), 1000 - 500 - CONFIG.tipping_fee);

        let p = state.posts.get(&post_id).unwrap();
        assert_eq!(p.tips, vec![(u2, 500)]);
    }

    #[test]
    fn test_covered_by_feed() {
        let m = |v: Vec<&str>| v.into_iter().map(|v| v.to_string()).collect();
        let tests = vec![
            (
                vec![m(vec!["tag1", "tag2"]), m(vec!["tag2", "tag3"])],
                m(vec!["tag1"]),
                true,
                None,
            ),
            (
                vec![m(vec!["tag1", "tag2"]), m(vec!["tag2", "tag3"])],
                m(vec!["tag1", "tag2"]),
                false,
                Some(0),
            ),
            (
                vec![m(vec!["tag1", "tag2"]), m(vec!["tag2", "tag3"])],
                m(vec!["tag1", "tag2"]),
                true,
                Some(0),
            ),
            (
                vec![m(vec!["tag1", "tag2"]), m(vec!["tag2", "tag3"])],
                m(vec!["tag1", "tag2", "tag3"]),
                true,
                None,
            ),
            (
                vec![m(vec!["tag1", "tag2"]), m(vec!["tag2", "tag3"])],
                m(vec!["tag1", "tag2", "tag3"]),
                false,
                Some(0),
            ),
            (
                vec![m(vec!["tag1", "tag2"]), m(vec!["tag2", "tag3"])],
                m(vec!["tagX", "tag2", "tag3"]),
                false,
                Some(1),
            ),
        ];

        for (i, t) in tests.iter().enumerate() {
            let (feeds, tags, strict, result) = t;
            if covered_by_feeds(feeds, tags, *strict) != *result {
                panic!("Test {} failed", i)
            }
        }
    }

    #[test]
    fn test_user_by_handle() {
        let mut state = State::default();

        let u1 = create_user_with_params(&mut state, pr(0), "user1", true);
        let u2 = create_user_with_params(&mut state, pr(1), "user2", true);
        let u3 = create_user_with_params(&mut state, pr(2), "user3", true);

        assert_eq!(state.user("user1").unwrap().id, u1);
        assert_eq!(state.user("0").unwrap().id, u1);
        assert_eq!(state.user("user2").unwrap().id, u2);
        assert_eq!(state.user("1").unwrap().id, u2);
        assert_eq!(state.user("user3").unwrap().id, u3);
        assert_eq!(state.user("2").unwrap().id, u3);
        assert!(state.user("user22").is_none());
    }

    #[actix_rt::test]
    async fn test_personal_feed() {
        let mut state = State::default();

        // create a post author and one post for its principal
        let p = pr(0);
        let post_author_id = create_user(&mut state, p);
        let post_id = add(
            &mut state,
            "This is a #post with #tags".to_string(),
            vec![],
            p,
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let anon = Principal::anonymous();

        // create a user and make sure his feed is empty
        let pr1 = pr(1);
        let user_id = create_user(&mut state, pr1);
        assert!(state
            .user(&user_id.to_string())
            .unwrap()
            .personal_feed(anon, &state, 0, true)
            .next()
            .is_none());

        // now we follow post_author_id
        let _user = state.users.get_mut(&user_id).unwrap();
        assert!(state.toggle_following_user(pr1, post_author_id));

        // make sure the feed contains exactly one post from post_author_id
        let feed = state
            .users
            .get(&user_id)
            .unwrap()
            .personal_feed(anon, &state, 0, true)
            .map(|post| post.id)
            .collect::<Vec<_>>();
        assert_eq!(feed.len(), 1);
        assert!(feed.contains(&post_id));

        // now we follow a feed #post+#tags
        let user = state.users.get_mut(&user_id).unwrap();
        assert!(user.toggle_following_feed(
            vec!["post".to_owned(), "tags".to_owned()]
                .into_iter()
                .collect()
        ));

        // make sure the feed still contains the same post
        let feed = state
            .users
            .get(&user_id)
            .unwrap()
            .personal_feed(anon, &state, 0, true)
            .map(|post| post.id)
            .collect::<Vec<_>>();
        assert_eq!(feed.len(), 1);
        assert!(feed.contains(&post_id));

        // now a different post with the same tags appears
        let p = pr(2);
        let _post_author_id = create_user(&mut state, p);
        let post_id2 = add(
            &mut state,
            "This is a different #post, but with the same #tags".to_string(),
            vec![],
            p,
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        // make sure the feed contains both posts
        let feed = state
            .users
            .get(&user_id)
            .unwrap()
            .personal_feed(anon, &state, 0, true)
            .map(|post| post.id)
            .collect::<Vec<_>>();
        assert_eq!(feed.len(), 2);
        assert!(feed.contains(&post_id));
        assert!(feed.contains(&post_id2));

        // yet another post appears
        let p = pr(3);
        let _post_author_id = create_user(&mut state, p);
        let post_id3 = add(
            &mut state,
            "Different #post, different #feed".to_string(),
            vec![],
            p,
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        // make sure the feed contains the same old posts
        let feed = state
            .users
            .get(&user_id)
            .unwrap()
            .personal_feed(anon, &state, 0, true)
            .map(|post| post.id)
            .collect::<Vec<_>>();
        assert_eq!(feed.len(), 2);
        assert!(feed.contains(&post_id));
        assert!(feed.contains(&post_id2));

        // now we follow a feed "post"
        let user = state.users.get_mut(&user_id).unwrap();
        let tags: Vec<_> = vec!["post".to_string()].into_iter().collect();
        assert!(user.toggle_following_feed(tags.clone()));
        // make sure the feed contains the new post
        let feed = state
            .users
            .get(&user_id)
            .unwrap()
            .personal_feed(anon, &state, 0, true)
            .map(|post| post.id)
            .collect::<Vec<_>>();
        assert_eq!(feed.len(), 3);
        assert!(feed.contains(&post_id));
        assert!(feed.contains(&post_id2));
        assert!(feed.contains(&post_id3));

        // Make sure we can unsubscribe and the feed gets back to 2 posts
        let user = state.users.get_mut(&user_id).unwrap();
        assert!(!user.toggle_following_feed(tags));
        let feed = state
            .users
            .get(&user_id)
            .unwrap()
            .personal_feed(anon, &state, 0, true)
            .map(|post| post.id)
            .collect::<Vec<_>>();
        assert_eq!(feed.len(), 2);
        assert!(feed.contains(&post_id));
        assert!(feed.contains(&post_id2));
    }

    #[test]
    fn test_hashtag_extraction() {
        let tags = |body| {
            let c = CONFIG;
            let mut t: Vec<_> = tags(c.max_tag_length, body).into_iter().collect();
            t.sort_unstable();
            t.join(" ")
        };
        assert_eq!(tags("This is a string without hashtags!"), "");
        assert_eq!(tags("This is a #string with hashtags!"), "string");
        assert_eq!(tags("#This is a #string with two hashtags!"), "This string");
        assert_eq!(tags("This string has no tags.#bug"), "");
        assert_eq!(
            tags("#This is a #string with #333 hashtags!"),
            "333 This string"
        );
        assert_eq!(tags("#year2021"), "year2021");
        assert_eq!(tags("#year2021 #year2021 #"), "year2021");
        assert_eq!(tags("#Ta1 #ta2"), "Ta1 ta2");
        assert_eq!(tags("#Tag #tag"), "Tag tag");
        assert_eq!(tags("Это #тест-строка"), "тест-строка");
        assert_eq!(tags("This is a #feature-request"), "feature-request");
        assert_eq!(tags("Support #under_score"), "under_score");
    }

    #[actix_rt::test]
    async fn test_cycles_accounting() {
        let mut state = State::default();
        let p0 = pr(0);
        let post_author_id = create_user(&mut state, p0);
        let post_id = add(
            &mut state,
            "test".to_string(),
            vec![],
            p0,
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let p = pr(1);
        let lurker_id = create_user(&mut state, p);
        let farmer_id = create_untrusted_user(&mut state, pr(111));
        let c = CONFIG;
        assert_eq!(state.burned_cycles, c.post_cost);
        state
            .users
            .get_mut(&lurker_id)
            .unwrap()
            .change_karma(10, "");
        state.users.get_mut(&lurker_id).unwrap().apply_rewards();
        // make author to a new user
        state
            .users
            .get_mut(&post_author_id)
            .unwrap()
            .change_karma(-CONFIG.trusted_user_min_karma, "");
        let author = state.users.get(&post_author_id).unwrap();
        let farmer = state.users.get(&farmer_id).unwrap();
        let lurker = state.users.get(&lurker_id).unwrap();
        assert!(!author.trusted());
        assert!(!farmer.trusted());
        assert!(lurker.trusted());
        assert_eq!(author.cycles(), c.min_cycles_minted - c.post_cost);
        assert_eq!(lurker.cycles(), c.min_cycles_minted);

        assert_eq!(author.karma(), 0);

        // react on the new post
        assert!(state.react(pr(111), post_id, 1, 0).is_err());
        // this is a noop for author
        assert!(state.react(pr(111), post_id, 100, 0).is_ok());
        let burned_cycles_by_reaction_from_untrusted = 11;
        assert_eq!(
            state.users.get(&post_author_id).unwrap().cycles(),
            c.min_cycles_minted - c.post_cost
        );
        assert!(state.react(p, post_id, 50, 0).is_ok());
        assert!(state.react(p, post_id, 100, 0).is_ok());
        let mut reaction_costs = 6 + 11;
        let burned_cycles_by_reactions = 1 + 1;
        let mut rewards_from_reactions = 5 + 10;

        // try to self upvote (should be a no-op)
        assert!(state.react(p0, post_id, 100, 0).is_err());

        let author = state.users.get(&post_author_id).unwrap();
        assert_eq!(author.cycles(), c.min_cycles_minted - c.post_cost);
        assert_eq!(author.karma_to_reward(), rewards_from_reactions);
        assert_eq!(
            state.burned_cycles,
            c.post_cost + burned_cycles_by_reactions + burned_cycles_by_reaction_from_untrusted
        );

        let lurker = state.users.get(&lurker_id).unwrap();
        assert_eq!(lurker.cycles(), c.min_cycles_minted - reaction_costs);

        // downvote
        assert!(state.react(p, post_id, 1, 0).is_ok());
        let reaction_penalty = -3;
        rewards_from_reactions -= 3;
        reaction_costs += 3;
        let author = state.users.get(&post_author_id).unwrap();
        let lurker = state.users.get(&lurker_id).unwrap();
        assert_eq!(
            author.cycles(),
            c.min_cycles_minted - c.post_cost + reaction_penalty
        );
        assert_eq!(author.karma_to_reward(), rewards_from_reactions);
        assert_eq!(lurker.cycles(), c.min_cycles_minted - reaction_costs);
        assert_eq!(
            state.burned_cycles,
            c.post_cost
                + burned_cycles_by_reactions
                + burned_cycles_by_reaction_from_untrusted
                + 2 * 3
        );

        add(
            &mut state,
            "test".to_string(),
            vec![],
            p0,
            0,
            Some(0),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            state.burned_cycles,
            2 * c.post_cost
                + burned_cycles_by_reactions
                + burned_cycles_by_reaction_from_untrusted
                + 2 * 3
        );
        let author = state.users.get(&post_author_id).unwrap();
        assert_eq!(
            author.cycles(),
            c.min_cycles_minted - c.post_cost - c.post_cost + reaction_penalty
        );

        let author = state.users.get_mut(&post_author_id).unwrap();
        author
            .change_cycles(c.post_cost - 1 - author.cycles(), "")
            .unwrap();
        assert!(add(
            &mut state,
            "test".to_string(),
            vec![],
            p0,
            0,
            None,
            None,
            None
        )
        .await
        .is_err());

        let lurker = state.users.get_mut(&lurker_id).unwrap();
        lurker.change_cycles(-lurker.cycles(), "").unwrap();
        assert_eq!(
            state.react(p, post_id, 10, 0),
            Err("not enough cycles".into())
        );
    }

    #[test]
    fn test_following() {
        let mut state = State::default();
        let p = pr(0);
        let id = create_user(&mut state, p);

        let u1 = create_user(&mut state, pr(1));
        let u2 = create_user(&mut state, pr(2));
        let u3 = create_user(&mut state, pr(3));

        assert!(state.toggle_following_user(p, 1));
        assert!(state.toggle_following_user(p, 2));
        assert!(state.toggle_following_user(p, 3));

        let f1 = &state.user(&u1.to_string()).unwrap().followers;
        assert_eq!(f1.len(), 1);
        assert!(f1.contains(&id));
        let f2 = &state.user(&u2.to_string()).unwrap().followers;
        assert_eq!(f2.len(), 1);
        assert!(f2.contains(&id));
        let f3 = &state.user(&u3.to_string()).unwrap().followers;
        assert_eq!(f3.len(), 1);
        assert!(f3.contains(&id));

        assert!(!state.toggle_following_user(p, 1));
        assert!(!state.toggle_following_user(p, 2));
        assert!(!state.toggle_following_user(p, 3));

        let f1 = &state.user(&u1.to_string()).unwrap().followers;
        assert!(!f1.contains(&id));
        let f2 = &state.user(&u2.to_string()).unwrap().followers;
        assert!(!f2.contains(&id));
        let f3 = &state.user(&u3.to_string()).unwrap().followers;
        assert!(!f3.contains(&id));

        let tags: Vec<_> = vec!["tag1".to_string(), "tag2".to_string()]
            .into_iter()
            .collect();
        let tags2: Vec<_> = vec!["tag1".to_owned()].into_iter().collect();
        let user = state.users.get_mut(&id).unwrap();
        assert!(user.toggle_following_feed(tags.clone()));
        assert!(user.toggle_following_feed(tags2.clone()));
        assert!(!user.toggle_following_feed(tags));
        assert!(!user.toggle_following_feed(tags2));
    }

    #[test]
    fn test_stalwarts() {
        let mut state = State::default();
        let now = CONFIG.min_stalwart_account_age_weeks as u64 * WEEK;

        for i in 0..100 {
            let id = create_user(&mut state, pr(i as u8));
            let user = state.users.get_mut(&id).unwrap();
            user.change_karma(i as i64, "");
            user.apply_rewards();
            // every second user was active
            if i % 2 == 0 {
                user.last_activity = now;
                user.active_weeks = CONFIG.min_stalwart_activity_weeks as u32;
                user.timestamp = 0;
            }
        }

        state.recompute_stalwarts(now + WEEK * 2);

        // make sure we have right number of stalwarts
        assert_eq!(
            state.users.values().filter(|u| u.stalwart).count(),
            CONFIG.stalwart_percentage
        );
    }

    #[actix_rt::test]
    async fn test_invites() {
        let mut state = State::default();
        let principal = pr(1);
        let id = create_user(&mut state, principal);

        // use too many cycles
        assert_eq!(
            state.create_invite(principal, 1111),
            Err("not enough cycles".to_string())
        );

        // use enough cycles and make sure they were deducted
        let prev_balance = state.users.get(&id).unwrap().cycles();
        assert_eq!(state.create_invite(principal, 111), Ok(()));
        let new_balance = state.users.get(&id).unwrap().cycles();
        assert_eq!(new_balance, prev_balance - 111);
        let invite = state.invites(principal);
        assert_eq!(invite.len(), 1);
        let (code, cycles) = invite.get(0).unwrap();
        assert_eq!(*cycles, 111);

        // use the invite
        let err = state
            .create_user(pr(2), "name".to_string(), Some(code.clone()))
            .await;
        assert!(err.is_ok())
    }
}
