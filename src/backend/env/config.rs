use crate::token::Token;

use super::{Cycles, Karma, HOUR, WEEK};
use ic_cdk::export::candid::CandidType;
use serde::Serialize;

pub const ICP_CYCLES_PER_XDR: u64 = 1_000_000_000_000;

#[derive(CandidType, Serialize)]
pub struct Config {
    pub name: &'static str,
    pub domains: &'static [&'static str],
    pub twitter: &'static str,
    pub logo: &'static str,

    pub transaction_fee: u64,
    pub token_decimals: u8,
    pub token_symbol: &'static str,
    pub total_supply: Token,

    pub proposal_approval_threshold: u16,
    pub proposal_controversy_threashold: u16,
    pub proposal_rejection_penalty: u32,

    pub min_cycle_balance_main: u64,

    pub max_bucket_size: u64,

    pub max_posts_per_hour: u8,
    pub max_comments_per_hour: u8,

    pub feed_page_size: usize,

    pub min_cycles_minted: Cycles,
    pub reporting_penalty: Cycles,

    pub minimal_tip: Cycles,
    pub tipping_fee: Cycles,

    pub trusted_user_min_karma: i64,
    pub trusted_user_min_age_weeks: u64,

    pub post_cost: Cycles,
    pub tag_cost: Cycles,
    pub blob_cost: Cycles,
    pub poll_cost: Cycles,
    pub realm_cost: Cycles,

    pub max_realm_name: usize,
    pub max_realm_logo_len: usize,

    pub response_reward: Karma,

    pub inactivity_penalty: i64,
    pub inactivity_duration_weeks: u64,

    pub voting_reward: i64,

    // top x percentage of users selected as stalwarts
    pub stalwart_percentage: usize,
    pub min_stalwart_activity_weeks: u8,
    pub min_stalwart_account_age_weeks: u8,
    pub stalwart_moderation_reward: i64,

    // percentage of stalwarts needed to confirm a report
    pub report_confirmation_percentage: u16,

    pub hot_post_reactions_percentage: f32,
    pub hot_post_comments_percentage: f32,

    pub max_post_length: usize,
    pub max_tag_length: usize,
    pub max_user_info_length: usize,
    pub max_blob_size_bytes: usize,

    pub min_cycles_for_inviting: Cycles,

    pub chores_interval_hours: u64,
    pub online_activity_minutes: u64,

    pub revenue_share_activity_weeks: u64,

    pub distribution_interval_hours: u64,

    pub reactions: &'static [(u16, Cycles)],

    pub min_positive_reaction_id: u16,

    pub reaction_fee: i64,

    pub max_funding_amount: u64,

    pub post_deletion_penalty_factor: u32,
}

pub const CONFIG: &Config = &Config {
    name: "Taggr",
    domains: &["taggr.link", "taggr.network", "taggr.club", "taggr.top"],
    logo: include_str!("../../frontend/assets/logo.min.svg"),
    twitter: "TaggrNetwork",

    token_symbol: "TAGGR",
    token_decimals: 2,
    transaction_fee: 1,

    #[cfg(feature = "dev")]
    proposal_approval_threshold: 1,
    #[cfg(not(feature = "dev"))]
    proposal_approval_threshold: 66,
    proposal_controversy_threashold: 10,
    proposal_rejection_penalty: 1000,

    total_supply: 100_000_000,

    min_cycle_balance_main: 2 * ICP_CYCLES_PER_XDR,

    #[cfg(feature = "dev")]
    report_confirmation_percentage: 10,
    #[cfg(test)]
    report_confirmation_percentage: 15,
    #[cfg(not(any(test, feature = "dev")))]
    report_confirmation_percentage: 25,

    trusted_user_min_karma: 25,
    trusted_user_min_age_weeks: 4,

    minimal_tip: 1,
    tipping_fee: 1,

    max_bucket_size: 1024 * 1024 * 1024 * 31, // 31Gb

    max_posts_per_hour: 3,
    max_comments_per_hour: 15,

    feed_page_size: 30,

    min_cycles_minted: 1000,

    reporting_penalty: 200,

    min_cycles_for_inviting: 50,

    post_cost: 2,
    tag_cost: 3,
    blob_cost: 10,
    poll_cost: 3,
    realm_cost: 1000,

    max_realm_name: 12,
    max_realm_logo_len: 16 * 1024,

    post_deletion_penalty_factor: 10,

    voting_reward: 5,

    response_reward: 1,

    inactivity_penalty: 45,
    inactivity_duration_weeks: 4,
    revenue_share_activity_weeks: 2,

    stalwart_percentage: 3,
    min_stalwart_activity_weeks: 6,
    min_stalwart_account_age_weeks: 26,
    stalwart_moderation_reward: 20,

    hot_post_reactions_percentage: 0.01,
    hot_post_comments_percentage: 0.006,

    max_post_length: 15000,
    max_tag_length: 20,
    max_user_info_length: 500,
    max_blob_size_bytes: 358400,

    online_activity_minutes: 10 * 60000000000_u64,
    chores_interval_hours: 24 * HOUR,

    distribution_interval_hours: WEEK,

    reactions: &[(1, -3), (100, 10), (50, 5), (51, 5), (10, 1)],

    min_positive_reaction_id: 10,

    reaction_fee: 1,

    max_funding_amount: 10000,
};
