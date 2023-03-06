use std::collections::HashMap;

use env::{
    canisters::upgrade_main_canister,
    config::CONFIG,
    post::{Extension, Post, PostId},
    proposals::{Payload, Release, Status},
    token::Account,
    user::{User, UserId},
    *,
};
use env::{heap_address, State};
use ic_cdk::{
    api::{
        self,
        call::{arg_data_raw, reply_raw},
    },
    caller, id, println, spawn, timer,
};
use ic_cdk_macros::*;
use serde_bytes::ByteBuf;

mod assets;
mod env;
mod http;

const BACKUP_PAGE_SIZE: u32 = 1024 * 1024;

static mut STATE: Option<State> = None;

pub fn state<'a>() -> &'a State {
    unsafe { STATE.as_ref().expect("read access failed: no state") }
}

fn state_mut<'a>() -> &'a mut State {
    unsafe { STATE.as_mut().expect("write access failed: no state") }
}

// sets a timer triggering chores
fn set_timer() {
    timer::set_timer_interval(std::time::Duration::from_secs(15 * 60), || {
        spawn(state_mut().chores(api::time()))
    });
}

#[init]
fn init() {
    let mut state: State = Default::default();
    state.storage.init();
    state.load();
    unsafe {
        STATE = Some(state);
    }
    set_timer();
}

#[pre_upgrade]
fn pre_upgrade() {
    heap_to_stable_core(state_mut());
}

#[post_upgrade]
fn post_upgrade() {
    // This should prevent accidental deployments of dev releases.
    #[cfg(feature = "dev")]
    {
        let config: &str = include_str!("../../canister_ids.json");
        if config.contains(&api::id().to_string()) {
            panic!("dev feature is enabled!")
        }
    }
    stable_to_heap_core();
    state_mut().load();
    set_timer();

    // temporary post upgrade logic goes here
    pub fn validate_username(name: &str) -> Result<(), String> {
        if name.len() < 2 || name.len() > 16 {
            return Err("should be between 2 and 16 characters".into());
        }
        if !name
            .chars()
            .all(|c| char::is_ascii(&c) && char::is_alphanumeric(c))
        {
            return Err("should be an alpha-numeric string".into());
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
        Ok(())
    }
    let s = state_mut();
    for u in s.users.values_mut() {
        match validate_username(&u.name) {
            Err(msg) if msg != "taken" && u.id != 0 => {
                ic_cdk::println!("{} renamed", u.name);
                u.name = format!("u{}", u.id);
            }
            _ => {}
        }
    }

    // rename users according to their comments on https://taggr.link/#/thread/18784
    s.users.get_mut(&759).unwrap().name = "virtualmachine".into();

    // Mint missing tokens to @Um (see https://taggr.link/#/thread/18735)
    crate::env::token::mint(
        s,
        Account {
            owner: candid::Principal::from_text(
                "ntzj4-wy4e6-33mt3-56bwa-tm3tr-rtvtf-bofnk-7pv6y-lyynh-hjnwc-tqe",
            )
            .unwrap(),
            subaccount: None,
        },
        4300,
    );
}

/*
 * Updates
 */

// #[update]
// async fn fix() {
// }

#[cfg(feature = "dev")]
#[update]
fn stable_mem_write(input: Vec<(u64, ByteBuf)>) {
    if let Some((page, buffer)) = input.get(0) {
        if buffer.is_empty() {
            return;
        }
        let offset = page * BACKUP_PAGE_SIZE as u64;
        let current_size = api::stable::stable64_size();
        let needed_size = ((offset + buffer.len() as u64) >> 16) + 1;
        let delta = needed_size.saturating_sub(current_size);
        if delta > 0 {
            api::stable::stable64_grow(delta).unwrap_or_else(|_| panic!("couldn't grow memory"));
        }
        api::stable::stable64_write(offset, buffer);
    }
}

#[cfg(feature = "dev")]
#[update]
fn stable_to_heap() {
    stable_to_heap_core();
}

fn stable_to_heap_core() {
    let (offset, len) = heap_address();
    println!(
        "Reading heap from coordinates: {:?}, stable memory size: {}",
        (offset, len),
        (api::stable::stable64_size() << 16)
    );

    let mut bytes = Vec::with_capacity(len);
    bytes.spare_capacity_mut();
    unsafe {
        bytes.set_len(len);
    }

    // Restore heap
    api::stable::stable64_read(offset, &mut bytes);
    unsafe {
        STATE = Some(serde_cbor::from_slice(&bytes).expect("couldn't deserialize"));
    };
}

fn heap_to_stable_core(state: &mut State) {
    let buffer: Vec<u8> = serde_cbor::to_vec(state).expect("couldn't serialize the state");
    let (offset, len) = state.storage.temporal_write(&buffer);
    // Save the heap address on stable memory
    api::stable::stable64_write(0, &offset.to_be_bytes());
    api::stable::stable64_write(8, &(len as u64).to_be_bytes());
}

#[export_name = "canister_update heap_to_stable"]
fn heap_to_stable() {
    let s = state_mut();
    let user = s
        .principal_to_user(caller())
        .expect("no user found")
        .clone();
    if user.stalwart {
        heap_to_stable_core(s);
        s.logger.info(format!(
            "@{} dumped heap to stable memory for backup purposes.",
            user.name
        ));
    }
    reply_raw(&[]);
}

#[export_name = "canister_update execute_upgrade"]
fn execute_upgrade() {
    let state = state_mut();
    let proposal = state
        .proposals
        .iter_mut()
        .last()
        .expect("no proposals found");
    if proposal.status != Status::Executed {
        return;
    }
    if let Payload::Release(release) = &mut proposal.payload {
        let binary = std::mem::take(&mut release.binary);
        if !binary.is_empty() {
            state.logger.info("Executing the canister upgrade...");
            upgrade_main_canister(&binary);
        }
    }
    reply_raw(&[]);
}

#[export_name = "canister_update finalize_upgrade"]
fn finalize_upgrade() {
    spawn(async {
        let hash: String = parse(&arg_data_raw());
        let state = state_mut();
        let proposal = state.proposals.iter().last().expect("no proposals found");
        reply(if proposal.status != Status::Executed {
            Err("no executed proposals found".into())
        } else if let Payload::Release(payload) = &proposal.payload {
            if !payload.binary.is_empty() {
                Err("no upgrades to finalize".into())
            } else if hash != payload.hash {
                Err("no upgrades for the provided hash".into())
            } else {
                let current = canisters::settings(id())
                    .await
                    .ok()
                    .and_then(|s| s.module_hash.map(hex::encode))
                    .unwrap_or_default();
                if hash != current {
                    let msg = format!(
                        "Upgrade failed: the main canister is on version `{}`",
                        &current[0..8]
                    );
                    state.logger.error(&msg);
                    Err(msg)
                } else {
                    state.module_hash = hash.clone();
                    state.logger.info(format!(
                        "Upgrade succeeded: new version is `{}`.",
                        &current[0..8]
                    ));

                    if !proposal.description.contains("#chore") {
                        state.notify_users(
                            &|user| user.active_within_weeks(time(), 1),
                            format!("New release `{}` [was deployed](#/proposals).", &hash[..8]),
                        );
                    }
                    Ok(())
                }
            }
        } else {
            Err("wrong proposal type".into())
        });
    });
}

#[export_name = "canister_update vote_on_report"]
fn vote_on_report() {
    let (post_id, vote): (PostId, bool) = parse(&arg_data_raw());
    env::reports::vote_on_report(state_mut(), caller(), post_id, vote);
    reply(());
}

#[export_name = "canister_update vote_on_poll"]
fn vote_on_poll() {
    let (post_id, vote): (PostId, u16) = parse(&arg_data_raw());
    reply(state_mut().vote_on_poll(caller(), api::time(), post_id, vote));
}

#[export_name = "canister_update report"]
fn report() {
    let (post_id, reason): (PostId, String) = parse(&arg_data_raw());
    reply(state_mut().report(caller(), post_id, reason));
}

#[export_name = "canister_update clear_notifications"]
fn clear_notifications() {
    let ids: Vec<String> = parse(&arg_data_raw());
    state_mut().clear_notifications(caller(), ids);
    reply_raw(&[]);
}

#[export_name = "canister_update tip"]
fn tip() {
    let (post_id, tip): (PostId, Cycles) = parse(&arg_data_raw());
    reply(state_mut().tip(caller(), post_id, tip));
}

#[export_name = "canister_update react"]
fn react() {
    let (post_id, reaction): (PostId, u16) = parse(&arg_data_raw());
    reply(state_mut().react(caller(), post_id, reaction, api::time()));
}

#[export_name = "canister_update update_last_activity"]
fn update_last_activity() {
    if let Some(user) = state_mut().principal_to_user_mut(caller()) {
        user.last_activity = api::time()
    }
    reply_raw(&[]);
}

#[export_name = "canister_update change_principal"]
fn change_principal() {
    let principal: String = parse(&arg_data_raw());
    reply(state_mut().change_principal(caller(), principal));
}

#[export_name = "canister_update update_user"]
fn update_user() {
    let (about, account, principals, settings): (String, String, Vec<String>, String) =
        parse(&arg_data_raw());
    let state = state_mut();
    let mut response: Result<(), String> = Ok(());
    if !User::valid_info(&about, &account, &settings) {
        response = Err("invalid user info".to_string());
        reply(response);
        return;
    }
    if let Some(user) = state.principal_to_user_mut(caller()) {
        user.update(about, account, principals, settings);
    } else {
        response = Err("no user found".into());
    }
    reply(response);
}

#[export_name = "canister_update create_user"]
fn create_user() {
    let (name, invite): (String, Option<String>) = parse(&arg_data_raw());
    spawn(async {
        reply(state_mut().create_user(caller(), name, invite).await);
    });
}

#[export_name = "canister_update buy_cycles"]
fn buy_cycles() {
    spawn(async { reply(state_mut().buy_cycles(caller()).await) });
}

#[export_name = "canister_update create_invite"]
fn create_invite() {
    let cycles: Cycles = parse(&arg_data_raw());
    reply(state_mut().create_invite(caller(), cycles));
}

#[update]
fn propose_release(description: String, commit: String, binary: ByteBuf) -> Result<u32, String> {
    proposals::propose(
        state_mut(),
        caller(),
        description,
        proposals::Payload::Release(Release {
            commit,
            binary: binary.to_vec(),
            hash: Default::default(),
        }),
    )
}

#[export_name = "canister_update propose_controller"]
fn propose_controller() {
    let (description, controller): (String, String) = parse(&arg_data_raw());
    reply(proposals::propose(
        state_mut(),
        caller(),
        description,
        proposals::Payload::SetController(controller),
    ))
}

#[export_name = "canister_update propose_funding"]
fn propose_funding() {
    let (description, receiver, tokens): (String, String, u64) = parse(&arg_data_raw());
    reply(proposals::propose(
        state_mut(),
        caller(),
        description,
        proposals::Payload::Fund(receiver, tokens),
    ))
}

#[export_name = "canister_update vote_on_proposal"]
fn vote_on_proposal() {
    spawn(async {
        let (proposal_id, approved): (u32, bool) = parse(&arg_data_raw());
        reply(
            proposals::vote_on_proposal(state_mut(), time(), caller(), proposal_id, approved).await,
        )
    })
}

#[export_name = "canister_update cancel_proposal"]
fn cancel_proposal() {
    let proposal_id: u32 = parse(&arg_data_raw());
    proposals::cancel_proposal(state_mut(), caller(), proposal_id);
    reply(());
}

#[update]
fn add_post(
    body: String,
    blobs: Vec<(String, Blob)>,
    parent: Option<PostId>,
    realm: Option<String>,
    extension: Option<ByteBuf>,
) -> Result<PostId, String> {
    let extension: Option<Extension> = extension.map(|bytes| parse(&bytes));
    post::add(
        state_mut(),
        body,
        blobs,
        caller(),
        api::time(),
        parent,
        realm,
        extension,
    )
}

#[update]
fn edit_post(
    id: PostId,
    body: String,
    blobs: Vec<(String, Blob)>,
    patch: String,
    realm: Option<String>,
) -> Result<(), String> {
    post::edit(
        state_mut(),
        id,
        body,
        blobs,
        patch,
        realm,
        caller(),
        api::time(),
    )
}

#[export_name = "canister_update delete_post"]
fn delete_post() {
    let (post_id, versions): (PostId, Vec<String>) = parse(&arg_data_raw());
    reply(state_mut().delete_post(caller(), post_id, versions));
}

#[export_name = "canister_update toggle_bookmark"]
fn toggle_bookmark() {
    let post_id: PostId = parse(&arg_data_raw());
    if let Some(user) = state_mut().principal_to_user_mut(caller()) {
        return reply(user.toggle_bookmark(post_id));
    };
    reply(true);
}

#[export_name = "canister_update toggle_following_post"]
fn toggle_following_post() {
    let post_id: PostId = parse(&arg_data_raw());
    reply(state_mut().toggle_following_post(caller(), post_id));
}

#[export_name = "canister_update toggle_following_user"]
fn toggle_following_user() {
    let followee_id: UserId = parse(&arg_data_raw());
    reply(state_mut().toggle_following_user(caller(), followee_id))
}

#[export_name = "canister_update toggle_following_feed"]
fn toggle_following_feed() {
    let tags: Vec<String> = parse(&arg_data_raw());
    reply(
        state_mut()
            .principal_to_user_mut(caller())
            .map(|user| user.toggle_following_feed(tags))
            .unwrap_or_default(),
    )
}

#[export_name = "canister_update edit_realm"]
fn edit_realm() {
    let (name, logo, label_color, description, controllers): (
        String,
        String,
        String,
        String,
        Vec<UserId>,
    ) = parse(&arg_data_raw());
    reply(state_mut().edit_realm(caller(), name, logo, label_color, description, controllers))
}

#[export_name = "canister_update enter_realm"]
fn enter_realm() {
    let name: String = parse(&arg_data_raw());
    state_mut().enter_realm(caller(), name);
    reply(());
}

#[export_name = "canister_update create_realm"]
fn create_realm() {
    let (name, logo, label_color, description, controllers): (
        String,
        String,
        String,
        String,
        Vec<UserId>,
    ) = parse(&arg_data_raw());
    reply(state_mut().create_realm(caller(), name, logo, label_color, description, controllers))
}

#[export_name = "canister_update toggle_realm_membership"]
fn toggle_realm_membership() {
    let name: String = parse(&arg_data_raw());
    reply(state_mut().toggle_realm_membership(caller(), name))
}

/*
 * QUERIES
 */

#[export_name = "canister_query balances"]
fn balances() {
    let state = state();
    reply(
        state
            .balances
            .iter()
            .fold(HashMap::new(), |mut map, (account, balance)| {
                map.entry(account.owner)
                    .and_modify(|b| *b += *balance)
                    .or_insert(*balance);
                map
            })
            .into_iter()
            .map(|(principal, balance)| {
                (
                    principal,
                    balance,
                    state.principal_to_user(principal).map(|u| u.id),
                )
            })
            .collect::<Vec<_>>(),
    );
}

#[export_name = "canister_query transactions"]
fn transactions() {
    let (page, search_term): (usize, String) = parse(&arg_data_raw());
    let iter = state().ledger.iter().enumerate();
    let iter: Box<dyn DoubleEndedIterator<Item = _>> = if search_term.is_empty() {
        Box::new(iter)
    } else {
        Box::new(iter.filter(|(_, t)| t.to.owner.to_string().contains(&search_term)))
    };
    reply(
        iter.rev()
            .skip(page * CONFIG.feed_page_size)
            .take(CONFIG.feed_page_size)
            .collect::<Vec<(usize, _)>>(),
    );
}

#[export_name = "canister_query proposals"]
fn proposals() {
    let page: usize = parse(&arg_data_raw());
    reply(
        state()
            .proposals
            .iter()
            .rev()
            .skip(page * CONFIG.feed_page_size)
            .take(CONFIG.feed_page_size)
            .collect::<Vec<_>>(),
    )
}

#[export_name = "canister_query realm_posts"]
fn realm_posts() {
    let (name, page, with_comments): (String, usize, bool) = parse(&arg_data_raw());
    let state = state();
    match state.realms.get(&name) {
        None => reply_raw(&[]),
        Some(realm) => reply(
            realm
                .posts
                .iter()
                .rev()
                .filter_map(|id| state.posts.get(id))
                .filter(move |post| with_comments || post.parent.is_none())
                .skip(page * CONFIG.feed_page_size)
                .take(CONFIG.feed_page_size)
                .cloned()
                .collect::<Vec<Post>>(),
        ),
    }
}

#[export_name = "canister_query realms"]
fn realms() {
    reply(&state().realms);
}

#[export_name = "canister_query read"]
fn read() {
    let args = &arg_data_raw();
    let offset = bytes_to_u64(args, 0);
    let len = bytes_to_u64(args, 8);
    reply_raw(&state().storage.read(offset, len as usize));
}

#[export_name = "canister_query tree"]
fn tree() {
    let post_id: PostId = parse(&arg_data_raw());
    reply(state().tree(post_id));
}

#[export_name = "canister_query user"]
fn user() {
    let input: Vec<String> = parse(&arg_data_raw());
    reply(resolve_handle(input.into_iter().next()).map(|mut user| {
        let state = state();
        let balance = state
            .principals
            .iter()
            .find_map(|(p, v)| (v == &user.id).then_some(p))
            .and_then(|p| {
                state.balances.get(&Account {
                    owner: *p,
                    subaccount: None,
                })
            })
            .copied()
            .unwrap_or_default();
        user.balance = balance;
        user
    }));
}

#[export_name = "canister_query invites"]
fn invites() {
    reply(state().invites(caller()));
}

#[export_name = "canister_query posts"]
fn posts() {
    let ids: Vec<PostId> = parse(&arg_data_raw());
    let state = state();
    reply(state.posts(ids).into_iter().collect::<Vec<Post>>());
}

#[export_name = "canister_query journal"]
fn journal() {
    let (handle, page): (String, usize) = parse(&arg_data_raw());
    let state = state();
    reply(
        state
            .user(&handle)
            .map(|user| {
                user.posts
                    .iter()
                    .rev()
                    .filter_map(|id| state.posts.get(id))
                    // we filter out responses and root posts starting with tagging another user
                    .filter(|post| post.parent.is_none() && !post.body.starts_with('@'))
                    .skip(page * CONFIG.feed_page_size)
                    .take(CONFIG.feed_page_size)
                    .cloned()
                    .collect::<Vec<Post>>()
            })
            .unwrap_or_default(),
    );
}

#[export_name = "canister_query hot_posts"]
fn hot_posts() {
    let page: usize = parse(&arg_data_raw());
    reply(state().hot_posts(caller(), page));
}

#[export_name = "canister_query last_posts"]
fn last_posts() {
    let (page, with_comments): (usize, bool) = parse(&arg_data_raw());
    let state = state();
    reply(
        state
            .last_posts(caller(), with_comments)
            .skip(page * CONFIG.feed_page_size)
            .take(CONFIG.feed_page_size)
            .cloned()
            .collect::<Vec<Post>>(),
    );
}

#[export_name = "canister_query posts_by_tags"]
fn posts_by_tags() {
    let (tags, users, page): (Vec<String>, Vec<UserId>, usize) = parse(&arg_data_raw());
    reply(
        state()
            .posts_by_tags(caller(), tags, users, page)
            .into_iter()
            .collect::<Vec<Post>>(),
    );
}

#[export_name = "canister_query personal_feed"]
fn personal_feed() {
    let (id, page, with_comments): (UserId, usize, bool) = parse(&arg_data_raw());
    let state = state();
    reply(match state.user(id.to_string().as_str()) {
        None => Default::default(),
        Some(user) => user
            .personal_feed(caller(), state, page, with_comments)
            .cloned()
            .collect::<Vec<Post>>(),
    });
}

#[export_name = "canister_query thread"]
fn thread() {
    let id: PostId = parse(&arg_data_raw());
    let state = state();
    reply(
        state
            .thread(id)
            .filter_map(|id| state.posts.get(&id))
            .cloned()
            .collect::<Vec<Post>>(),
    );
}

#[export_name = "canister_query validate_username"]
fn validate_username() {
    let name: String = parse(&arg_data_raw());
    reply(state().validate_username(&name));
}

#[export_name = "canister_query recent_tags"]
fn recent_tags() {
    let n: u64 = parse(&arg_data_raw());
    reply(state().recent_tags(caller(), n));
}

#[export_name = "canister_query users"]
fn users() {
    reply(
        state()
            .users
            .values()
            .map(|user| (user.id, user.name.clone(), user.karma()))
            .collect::<Vec<(UserId, String, Karma)>>(),
    );
}

#[export_name = "canister_query config"]
fn config() {
    reply(CONFIG);
}

#[export_name = "canister_query stats"]
fn stats() {
    reply(state().stats(api::time()));
}

#[export_name = "canister_query search"]
fn search() {
    let term: String = parse(&arg_data_raw());
    reply(state().search(caller(), term));
}

#[query]
fn stable_mem_read(page: u64) -> Vec<(u64, Blob)> {
    let offset = page * BACKUP_PAGE_SIZE as u64;
    let (heap_offset, len) = heap_address();
    let memory_end = heap_offset + len as u64;
    if offset > memory_end {
        return Default::default();
    }
    let chunk_size = (BACKUP_PAGE_SIZE as u64).min(memory_end - offset) as usize;
    let mut buf = Vec::with_capacity(chunk_size);
    buf.spare_capacity_mut();
    unsafe {
        buf.set_len(chunk_size);
    }
    api::stable::stable64_read(offset, &mut buf);
    vec![(page, ByteBuf::from(buf))]
}

fn parse<'a, T: serde::Deserialize<'a>>(bytes: &'a [u8]) -> T {
    serde_json::from_slice(bytes).expect("couldn't parse the input")
}

fn reply<T: serde::Serialize>(data: T) {
    reply_raw(serde_json::json!(data).to_string().as_bytes());
}

fn resolve_handle(handle: Option<String>) -> Option<User> {
    match handle {
        Some(handle) => state().user(&handle).cloned(),
        None => Some(state().principal_to_user(caller())?.clone()),
    }
}

fn bytes_to_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut number_bytes: [u8; 8] = Default::default();
    number_bytes.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_be_bytes(number_bytes)
}
