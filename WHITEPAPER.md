$name is a fully autonomous decentralized social network.

## Key Points

- #$name is a blend of forums and blogs.
- Its purpose is to be a simple way to publish content on a public compute infrastructure.
- [Posts](#/post/0) containing #tags will appear in feeds comprised of these tags.
- Users can follow tag [feeds](#/feed/$name), other [users](#/user/0) and watch activity on posts.
- $name is owned and governed by its community.
- $name rewards its users with ICP and shares its revenue with token holders.

## THE SOCIAL EXPERIMENT

The experimental part of $name is that _the community_ decides what the rules are.
The auto-regulation is enforced through a scheme of incentives:
 - Every user starts with at least `$min_cycles_minted` cycles minted for `1` [XDR](https://en.wikipedia.org/wiki/Special_drawing_rights) in ICP.
 - Every mutable operation on $name burns user's cycles.
 - Users earn or lose "karma" by writing posts and getting reactions and comments.
 - Users can mint new cycles at any point paying at least `1` XDR in ICP.
 - All payments go to [$name's Treasury](https://dashboard.internetcomputer.org/account/cc5fb202723a8cfb43128dda9c0d64c03f22d455070271f63a60afff14f360ec) holding ICP rewards and $name's revenue.

## Tokenomics

$name has a total supply of `$total_supply` tokens. Tokens can only be mined.
Currently, all users who earn karma, automatically mine `$token_symbol` tokens.
The token minting happens weekly by converting the earned karma to `$token_symbol` tokens at an exponentially declining ratio.
The ratio starts with `1:1` for the first `10%` of supply, then drops to `2:1` for the next `10%`, then to `4:1` and so on.
Hence the last `10%` of supply will be minted at the ratio `1024:1`.

The utility of tokens is governance and ownership of $name's revenue.

## Rewards and Revenue Distributions

- During positive engagements users can receive karma from other users.
- The received karma points will be converted to rewards during the next distribution.
- The rewards are computed by converting `$min_cycles_minted` karma points to ICP at the same rate as cycle minting (`1 XDR` for `$min_cycles_minted` karma points).
- Additionally to rewards, users that own tokens and were active within the last `$revenue_share_activity_weeks` weeks receive a share of $name's revenue proportional to their token share.
- Users are excluded from both distributions if their ICP amount are smaller than `100` times the transaction fee. These users carry over their accumulated karma to the next round.

## Bootcamp

Every new user goes through a "bootcamp" period of `$trusted_user_min_age_weeks` weeks.
During this time the user is marked with *️⃣ on their profile and cannot affect anybody's karma through the engagements, cannot downvote posts or vote on proposals.
If after the bootcamp period the user still has less than `$trusted_user_min_karma` karma points, the user stays in the bootcamp until the karma threshold is reached.

## Realms

Realms represent sub-communities grouped by a certain topic.
Every realm can have its own term and conditions, so that any violation of them can lead to flagging of user's post to stalwarts.
By joining a realm, a user implicitly agrees with its terms and conditions.

## STALWARTS

Stalwarts are the top `$stalwart_percentage%` of users with the highest karma being active during the last `$min_stalwart_activity_weeks` consecutive weeks, with accounts older than `$min_stalwart_account_age_weeks` weeks.
They are marked with ⚔️ on their profiles and count as trusted members of the community.
Stalwarts can carry out moderating actions and submit proposals.

## The content policy

Decentralization does not imply lawlessness!
The content allowed on $name is the content tolerated by the community which agreed on a moderation in the following cases:
- the post is threatening $name as a public service (e.g., it is illegal in most jurisdictions),
- the post was created with a nefarious intent, e.g. to game the $name system and/or is threatening $name's community members, sustainability or decentralization,
- the post breaks the rules of a realm.

The policy is vague on purpose and will require a social consensus among the stalwarts.

**Posts violating this policy are a subject to moderation.**

## Moderation

Moderation on $name is decentralized: it can be triggered by anyone and can be exercised by the _stalwarts_.
Whenever a post gets reported, all stalwarts get notified and are expected to confirm or reject the report.
As soon as `$report_confirmation_percentage%` of stalwarts agree on confirmation or rejection of the report, the report gets closed.
If most stalwarts confirm the report:
- the post author loses `$reporting_penalty` karma points and at least `$reporting_penalty` cycles,
- the user who created the report receives half of the penalty `$reporting_penalty` as karma points.

If the stalwarts reject the report, the user who created the report loses `$reporting_penalty` cycles and karma points.  
In both cases, every stalwart participating in voting receives an equal share of karma points from the penalty fee, but not more than `$stalwart_moderation_reward`.

## Cost Table

Using $name costs cycles. Here's a breakdown of all costs.

| Function | Cycles 🔥 | Comments |
|:-------:|-------:|:--------------|
|New post or comment  | `$post_cost`     | without hashtags |
|Hashtags             | `T * $tag_cost` | for `T` unique hashtags in a post or comment |
|On-chain pictures    | `B * $blob_cost`| for `B` pictures in a post or comment |
|Poll                 | `$poll_cost`     | for extending a post or comment with a poll |
|Reacting with ❤️      | `2`     | burns `$reaction_fee` cycle, adds `1`  point to author's karma|
|Reacting with 🔥, 😆 | `6`     | burns `$reaction_fee` cycle, adds `5`  points to author's karma|
|Reacting with ⭐️     | `11`    | burns `$reaction_fee` cycle, adds `10` points to author's karma|
|Reacting with 👎     | `3`     | burns `3` cycles **and** karma points of the post author|
|New realm creation   | `$realm_cost`  | burns `$realm_cost` cycles |

Additionally:
1. Every response to a post increases author's karma for creating a resonance (by `$response_reward` karma point).
2. The karma and cycles of every user inactive for longer than `$inactivity_duration_weeks` weeks decreases by `$inactivity_penalty` per week.
3. Users with a negative karma do not participate in distributions.

### Tipping

Additionally to rewards users can tip each other with any amount of cycles. The fee for tipping is `$tipping_fee` cycle.
The cycles transferred via tipping get added to receiver's cycle balance and do not contribute to karma.

## Proposals

A proposal passes if `$proposal_approval_threshold%` of users approve it or it fails if `(100-$proposal_approval_threshold)%` of users reject it.
Only tokens of registered users count as participating votes.
To prevent low-quality proposals, if a proposal is rejected with a rejected/adopted ratio of less than `$proposal_controversy_threashold%`, the proposer loses `$proposal_rejection_penalty` karma points and cycles.

The total voting power of all registered users needed to adopt or reject a proposal decreases daily by `1%` as long as the proposal stays open.
This is achieved by multiplying the total available voting power by the factor `d%` with `d` being the number of days on which the proposal remained open.
That allows any proposal to pass eventually within `100` days.

Voting is rewarded with `$voting_reward` karma points.  
While a proposal stays open, the system defers the reward distributions and token minting until the proposal is rejected or adopted.

## Invites

Every user can invite new users to $name by creating invites charged with cycles.
The profile of every invited user shows their "host", s.t. users can be held accountable for their invites.

## Autonomy

$name is designed with decentralization in mind which means **full autonomy**.
Hence, $name is automatically creating new storage canisters if it runs out of space.
$name also automatically tops up its canisters if they are low on cycles using the ICP from the Treasury.
All information on the current state of the system and past events can be found on the [dashboard](/#/dashboard).

## Bots

Every $name user can be turned into a bot by adding one or more principal ids in the account settings.
Bots are marked with the 🤖 emoji on their profiles.
Those principal ids (canisters or self-authenticating) can then call $name's `add_post` method specified in Candid format as follows:

    "add_post": (text, vec record { text; blob}, opt nat64, opt text) -> (variant { Ok: nat64; Err: text });

With arguments:
- `text`: the body text;
- `vec record {text; blob}`: the vector of attached pictures where each tuple contains a blob id and the blob itself; the tuple must satisfy the following requirements:
  - the id should be shorter than `9` characters,
  - the blob should contain less than `358401` bytes,
  - every picture needs to be referenced from the post by the URL `/blob/<id>`.
- `opt nat64`: the id of the parent post;
- `opt text`: the realm name.

Note that currently #IC does not support messages larger than `2Mb` in total size.
The result of the `add_post` post method will contain the id of the new post if the post could be added successfully, or an error message otherwise.

## Code and the Bug Bounty Program

$name's [code](https://github.com/TaggrNetwork/taggr) is open source and has a GPL license.

$name's DAO has agreed with a bug bounty program with the following bug classification and corresponding rewards nominated in `$token_symbol`.

| SEV | DESCRIPTION | PRIZE |
|:---:|-------------|--------:|
| 0 | The bug enables  an unsanctioned state mutation affecting the monetary value of user assets like cycles, karma, tokens, the Treasury or **critically** endanger $name's functionality, autonomy or decentralization. | `1000` |
| 1 | The bug enables an unsanctioned state mutation affecting important data, like posts, comments, rewards and so on or has a negative but not critical impact on $name's decentralization and autonomy.                  | `400` |
| 2 | The bug enables an unsanctioned state mutation but cannot be easily leveraged to endanger $name or its data.                                                                                                          | `100` |

If you found a bug that falls under any of these categories, please immediately reach out to stalwarts to coordinate further actions.
