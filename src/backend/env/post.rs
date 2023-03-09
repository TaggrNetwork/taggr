use super::user::UserId;
use super::*;
use crate::reports::Report;
use serde::{Deserialize, Serialize};

pub type PostId = u64;

#[derive(Clone, Serialize, Deserialize)]
pub struct Poll {
    options: Vec<String>,
    votes: BTreeMap<u16, BTreeSet<UserId>>,
    deadline: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Extension {
    Poll(Poll),
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Post {
    pub id: PostId,
    pub body: String,
    pub user: UserId,
    pub timestamp: u64,
    pub children: Vec<PostId>,
    pub parent: Option<PostId>,
    pub watchers: BTreeSet<UserId>,
    pub tags: BTreeSet<String>,
    pub reactions: BTreeMap<u16, BTreeSet<UserId>>,
    pub patches: Vec<(u64, String)>,
    pub files: BTreeMap<String, (u64, usize)>,
    pub tree_size: u32,
    pub tree_update: u64,
    pub report: Option<Report>,
    pub tips: Vec<(UserId, Cycles)>,
    extension: Option<Extension>,
    pub realm: Option<String>,
    #[serde(default)]
    pub hashes: Vec<String>,
}

impl Post {
    pub fn new(
        user: UserId,
        tags: BTreeSet<String>,
        body: String,
        timestamp: u64,
        parent: Option<PostId>,
        mut extension: Option<Extension>,
        realm: Option<String>,
    ) -> Self {
        // initialize all extensions properly
        if let Some(Extension::Poll(poll)) = &mut extension {
            poll.votes.clear()
        };

        Self {
            id: 0,
            tags,
            body,
            user,
            timestamp,
            children: Default::default(),
            watchers: [user].iter().cloned().collect(),
            reactions: Default::default(),
            parent,
            patches: Default::default(),
            files: Default::default(),
            tips: Default::default(),
            hashes: Default::default(),
            tree_size: 0,
            tree_update: timestamp,
            report: None,
            extension,
            realm,
        }
    }

    pub fn vote_on_poll(
        &mut self,
        user_id: UserId,
        user_realms: Vec<String>,
        time: u64,
        vote: u16,
    ) -> Result<(), String> {
        if let Some(realm) = self.realm.as_ref() {
            if !user_realms.contains(realm) {
                return Err(format!("you're not in realm {}", realm));
            }
        }
        if let Some(Extension::Poll(poll)) = self.extension.as_mut() {
            // no multiple choice
            if poll.votes.values().flatten().any(|id| id == &user_id) {
                return Err("double vote".to_string());
            }
            if time < self.timestamp + HOUR * poll.deadline && poll.options.len() as u16 > vote {
                poll.votes.entry(vote).or_default().insert(user_id);
            }
        }
        Ok(())
    }

    pub fn valid(&self, blobs: &[(String, Blob)]) -> Result<(), String> {
        if self.body.is_empty() || self.body.chars().count() > CONFIG.max_post_length {
            return Err("invalid post content".into());
        }
        if !blobs.iter().all(|(key, blob)| {
            key.len() <= 8 && blob.len() > 0 && blob.len() <= CONFIG.max_blob_size_bytes
        }) {
            return Err("invalid blobs".into());
        }
        Ok(())
    }

    pub async fn save_blobs(
        &mut self,
        state: &mut State,
        blobs: Vec<(String, Blob)>,
    ) -> Result<(), String> {
        for (id, blob) in blobs.into_iter() {
            // only if the id is new, add it.
            if self.files.keys().any(|file_id| file_id.contains(&id)) {
                continue;
            }
            match state
                .storage
                .write_to_bucket(&mut state.logger, blob.as_slice())
                .await
                .clone()
            {
                Ok((bucket_id, offset)) => {
                    self.files
                        .insert(format!("{}@{}", id, bucket_id), (offset, blob.len()));
                }
                Err(err) => {
                    state
                        .logger
                        .error(format!("Couldn't write a blob to bucket: {:?}", err));
                    return Err(err);
                }
            };
        }
        Ok(())
    }

    pub fn vote_on_report(&mut self, stalwarts: usize, stalwart: UserId, confirmed: bool) {
        // No voting on own posts.
        if self.user == stalwart {
            return;
        }
        if let Some(report) = self.report.as_mut() {
            report.vote(stalwarts, stalwart, confirmed);
            let approved = report.closed && report.confirmed_by.len() > report.rejected_by.len();
            if approved {
                self.delete(vec![self.body.clone()]);
            }
        }
    }

    pub fn delete(&mut self, versions: Vec<String>) {
        self.files.clear();
        self.body.clear();
        self.patches.clear();
        self.hashes = versions
            .into_iter()
            .map(|value| {
                let mut hasher = Sha256::new();
                hasher.update(value.as_bytes());
                format!("{:x}", hasher.finalize())
            })
            .collect();
    }

    pub fn report(&mut self, reporter: UserId, reason: String) {
        self.report = Some(Report {
            reporter,
            reason,
            ..Default::default()
        })
    }

    pub fn costs(&self, blobs: usize) -> Cycles {
        let tags = self.tags.len() as u64;
        CONFIG.post_cost.max(tags as i64 * CONFIG.tag_cost)
            + blobs as i64 * CONFIG.blob_cost
            + if self.extension.is_some() {
                CONFIG.poll_cost
            } else {
                0
            }
    }

    pub fn make_hot(&self, hot_list: &mut VecDeque<PostId>, total_users: usize, user_id: UserId) {
        // if it's a comment or reaction is from the users itself, exit
        if self.parent.is_some() || self.user == user_id {
            return;
        };
        // if too few reactions or too few comments, exit
        if self
            .reactions
            .iter()
            .filter_map(|(id, users)| {
                if *id >= CONFIG.min_positive_reaction_id {
                    Some(users.len())
                } else {
                    None
                }
            })
            .sum::<usize>() as f32
            / (total_users as f32)
            < CONFIG.hot_post_reactions_percentage
            && self.tree_size as f32 / (total_users as f32) < CONFIG.hot_post_comments_percentage
        {
            return;
        }
        // negative reactions balance
        if self
            .reactions
            .iter()
            .filter_map(|(id, _)| CONFIG.reactions.iter().find(|(rid, _)| id == rid))
            .map(|(_, cost)| *cost)
            .sum::<i64>()
            < 0
        {
            return;
        }

        let prev_len = hot_list.len();
        hot_list.retain(|post_id| *post_id != self.id);
        hot_list.push_front(self.id);
        if hot_list.len() > prev_len {
            hot_list.pop_back();
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn edit(
    state: &mut State,
    id: PostId,
    body: String,
    blobs: Vec<(String, Blob)>,
    patch: String,
    picked_realm: Option<String>,
    principal: Principal,
    timestamp: u64,
) -> Result<(), String> {
    let user = state
        .principal_to_user(principal)
        .ok_or("no user found")?
        .clone();
    let mut post = state.posts.get(&id).ok_or("no post found")?.clone();
    if post.user != user.id {
        // unauthorized
        return Err("unauthorized".into());
    }
    if let Some(false) = picked_realm.as_ref().map(|name| user.realms.contains(name)) {
        // user didn't join this realm
        return Err("you're not in the realm".into());
    }
    let user_id = user.id;
    post.tags = tags(CONFIG.max_tag_length, &body);
    post.body = body;
    post.valid(&blobs)?;
    let files_before = post.files.len();
    post.save_blobs(state, blobs).await?;
    let costs = post.costs(post.files.len().saturating_sub(files_before));
    state.charge(user_id, costs, format!("editing of post {}", id))?;
    post.patches.push((post.timestamp, patch));
    post.timestamp = timestamp;

    // We only change the realm on root posts.
    if post.parent.is_none() {
        // remove post from the previous realm
        if let Some(realm) = post
            .realm
            .as_ref()
            .and_then(|name| state.realms.get_mut(name))
        {
            realm.posts.retain(|post_id| post_id != &id);
        }
        post.realm = picked_realm.clone();
        // add post to the new realm
        if let Some(realm) = picked_realm
            .as_ref()
            .and_then(|name| state.realms.get_mut(name))
        {
            realm.posts.push(id);
        }
    }

    state
        .posts
        .insert(id, post)
        .expect("previous post should exists");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn add(
    state: &mut State,
    body: String,
    blobs: Vec<(String, Blob)>,
    principal: Principal,
    timestamp: u64,
    parent: Option<PostId>,
    picked_realm: Option<String>,
    extension: Option<Extension>,
) -> Result<PostId, String> {
    let user = match state.principal_to_user(principal) {
        Some(user) => user,
        // look for an authorized controller
        None => {
            let controller_id = principal.to_string();
            match state
                .users
                .values()
                .find(|u| u.controllers.contains(&controller_id))
            {
                Some(user) => user,
                None => return Err(format!("no user with controller {} found", controller_id)),
            }
        }
    };

    if user.is_bot() && parent.is_some() {
        return Err("Bots can't create comments currently".into());
    }

    let limit = if parent.is_none() {
        CONFIG.max_posts_per_hour
    } else {
        CONFIG.max_comments_per_hour
    } as usize;
    if user
        .posts
        .iter()
        .rev()
        .filter_map(|id| state.posts.get(id))
        .filter(|post| {
            !(parent.is_none() ^ post.parent.is_none())
                && post.timestamp > timestamp.saturating_sub(HOUR)
        })
        .count()
        > limit
    {
        return Err(format!(
            "not more than {} {} per hour are allowed",
            limit,
            if parent.is_none() {
                "posts"
            } else {
                "comments"
            }
        ));
    }
    let realm = match parent.and_then(|id| state.posts.get(&id)) {
        None => picked_realm.or_else(|| user.current_realm.clone()),
        Some(post) => post.realm.clone(),
    };
    if let Some(name) = &realm {
        if !user.realms.contains(name) {
            return Err(format!("not a member of the realm {}", name));
        }
    }
    let user_id = user.id;
    let mut post = Post::new(
        user_id,
        tags(CONFIG.max_tag_length, &body),
        body,
        timestamp,
        parent,
        extension,
        realm.clone(),
    );
    let costs = post.costs(blobs.len());
    post.valid(&blobs)?;
    let trusted_user = user.trusted();
    state.charge(user_id, costs, "new post".to_string())?;
    post.save_blobs(state, blobs).await?;
    let id = state.new_post_id();
    let user = state.users.get_mut(&user_id).expect("no user found");
    user.posts.push(id);
    post.id = id;
    if let Some(realm) = realm.and_then(|name| state.realms.get_mut(&name)) {
        realm.posts.push(id);
    }
    if let Some(parent_post) = post
        .parent
        .and_then(|parent_id| state.posts.get_mut(&parent_id))
    {
        parent_post.children.push(id);
        parent_post.watchers.insert(user_id);
        let parent_post_author = parent_post.user;
        if parent_post.user != user_id && trusted_user {
            let log = format!("response to post {}", parent_post.id);
            // Reward user for spawning activity with his post.
            state.spend_to_user_karma(parent_post_author, CONFIG.response_reward, log)
        }
    }
    state.posts.insert(post.id, post.clone());
    notify_about(state, &post);

    state
        .thread(id)
        .filter(|post_id| post_id != &id)
        .for_each(|id| {
            if let Some(post) = state.posts.get_mut(&id) {
                post.tree_size += 1;
                post.tree_update = timestamp;
                post.make_hot(&mut state.hot, state.users.len(), user_id);
            }
        });

    Ok(id)
}

fn notify_about(state: &mut State, post: &Post) {
    let post_user_name = state
        .users
        .get(&post.user)
        .expect("no user found")
        .name
        .clone();
    let mut notified: HashSet<_> = HashSet::new();
    // Don't notify the author
    notified.insert(post.user);
    if let Some(parent) = post
        .parent
        .and_then(|parent_id| state.posts.get(&parent_id))
    {
        let parent_author = parent.user;
        if parent_author != post.user {
            if let Some(user) = state.users.get_mut(&parent_author) {
                user.notify_about_post(
                    format!("@{} replied to your post", post_user_name,),
                    post.id,
                );
                notified.insert(user.id);
            }
        }
    }

    user_handles(CONFIG.max_tag_length, &post.body)
        .into_iter()
        .filter_map(|handle| state.user(&handle).map(|user| user.id))
        .filter(|id| !notified.contains(id))
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|mentioned_user_id| {
            let user = state
                .users
                .get_mut(&mentioned_user_id)
                .expect("no user found");
            user.notify_about_post(
                format!("@{} mentioned you in a post", post_user_name),
                post.id,
            );
            notified.insert(user.id);
        });

    state
        .thread(post.id)
        .filter_map(|id| state.posts.get(&id))
        .flat_map(|post| {
            post.watchers
                .clone()
                .into_iter()
                .map(move |user_id| (post.id, user_id))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|(post_id, user_id)| {
            if notified.contains(&user_id) {
                return;
            }
            if let Some(user) = state.users.get_mut(&user_id) {
                user.notify_about_watched_post(post_id, post.id);
            }
            notified.insert(user_id);
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_costs() {
        let mut p = Post::default();
        // empty post
        assert_eq!(p.costs(Default::default()), CONFIG.post_cost);

        // one tag
        p.tags = ["world"].iter().map(|x| x.to_string()).collect();
        assert_eq!(p.costs(0), CONFIG.tag_cost);

        // two tags
        p.tags = ["hello", "world"].iter().map(|x| x.to_string()).collect();
        assert_eq!(p.costs(0), 2 * CONFIG.tag_cost);

        // two tags and a blob
        p.tags = ["hello", "world"].iter().map(|x| x.to_string()).collect();
        assert_eq!(p.costs(1), 2 * CONFIG.tag_cost + CONFIG.blob_cost);
    }

    #[test]
    fn test_validity() {
        let mut p = Post::default();
        // empty body
        assert!(p.valid(Default::default()).is_err());

        // too long body
        p.body = String::from_utf8(
            "test"
                .as_bytes()
                .iter()
                .cycle()
                .take(CONFIG.max_post_length + 1)
                .cloned()
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(p.valid(Default::default()).is_err());

        // valid body
        p.body = "Hello world!".to_string();
        assert!(p.valid(Default::default()).is_ok());

        // too long blob id
        assert!(p
            .valid(
                vec![(
                    "abcdefghX".to_string(),
                    ByteBuf::from(
                        [0, 1]
                            .iter()
                            .cycle()
                            .take(CONFIG.max_blob_size_bytes)
                            .cloned()
                            .collect::<Vec<_>>()
                    )
                )]
                .as_slice()
            )
            .is_err());

        // valid blob
        assert!(p
            .valid(
                vec![(
                    "abcdefgh".to_string(),
                    ByteBuf::from(
                        [0, 1]
                            .iter()
                            .cycle()
                            .take(CONFIG.max_blob_size_bytes)
                            .cloned()
                            .collect::<Vec<_>>()
                    )
                )]
                .as_slice()
            )
            .is_ok());

        // empty blob
        assert!(p
            .valid(vec![("abcdefgh".to_string(), Default::default())].as_slice())
            .is_err());
    }
}
