use super::user::UserId;
use super::*;
use candid::Principal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Report {
    pub reporter: UserId,
    pub confirmed_by: Vec<UserId>,
    pub rejected_by: Vec<UserId>,
    pub closed: bool,
    pub reason: String,
}

impl Report {
    pub fn vote(&mut self, stalwarts: usize, stalwart: UserId, confirmed: bool) {
        if stalwart == self.reporter
            || self.confirmed_by.contains(&stalwart)
            || self.rejected_by.contains(&stalwart)
        {
            return;
        }
        if confirmed {
            self.confirmed_by.push(stalwart);
        } else {
            self.rejected_by.push(stalwart);
        }
        let votes = self.confirmed_by.len().max(self.rejected_by.len()) as u16;
        if votes * 100 >= CONFIG.report_confirmation_percentage * stalwarts as u16 {
            self.closed = true;
            return;
        }
    }
}

pub fn vote_on_report(state: &mut State, principal: Principal, post_id: PostId, vote: bool) {
    let user = state
        .principal_to_user(principal)
        .expect("no user found")
        .clone();
    if !user.stalwart {
        return;
    }
    let stalwarts = state.users.values().filter(|u| u.stalwart).count();
    let post = state.posts.get_mut(&post_id).expect("no post found");
    post.vote_on_report(stalwarts, user.id, vote);
    let report = match &post.report {
        Some(report) if report.closed => report.clone(),
        _ => return,
    };
    let post_author_id = post.user;
    let (sponsor_id, unit) = if report.confirmed_by.len() > report.rejected_by.len() {
        // penalty for the post author
        let post_author = state.users.get_mut(&post.user).expect("no user found");
        post_author.notify_about_post(
            format!(
                "Your post was reported by users and deleted by stalwarts. Reason: {}",
                report.reason
            ),
            post.id,
        );
        post_author.change_karma(-CONFIG.reporting_penalty, "moderation penalty");
        let unit = CONFIG.reporting_penalty.min(post_author.cycles()) / 2;
        let reporter = state
            .users
            .get_mut(&report.reporter)
            .expect("no user found");
        reporter.notify_about_post(format!("The post reported by you was deleted by stalwarts. Thanks for keeping {} safe and clean!", CONFIG.name), post.id);
        state
            .transfer(
                post_author_id,
                report.reporter,
                unit,
                0,
                Destination::Karma,
                "moderation rewards",
            )
            .expect("couldn't reward reporter");
        (post_author_id, unit)
    } else {
        // penalty for reporter
        let reporter = state
            .users
            .get_mut(&report.reporter)
            .expect("no user found");
        reporter.notify_about_post("Your report was rejected by stalwarts", post.id);
        let unit = CONFIG.reporting_penalty.min(reporter.cycles());
        let log = "false report penalty";
        reporter.change_karma(-CONFIG.reporting_penalty / 2, log);
        let reporter_id = reporter.id;
        (reporter_id, unit)
    };
    let stalwarts = report
        .confirmed_by
        .iter()
        .chain(report.rejected_by.iter())
        .cloned()
        .collect::<Vec<_>>();
    let stalwart_reward = (unit / stalwarts.len() as i64).min(CONFIG.stalwart_moderation_reward);
    let mut total_stalwart_rewards = 0;
    let log = "moderation rewards for stalwarts";
    for stalwart_id in stalwarts.iter() {
        let moderator = state.users.get(stalwart_id).expect("no user found").id;
        state
            .transfer(
                sponsor_id,
                moderator,
                stalwart_reward,
                0,
                Destination::Karma,
                log,
            )
            .expect("couldn't reward stalwarts");
        total_stalwart_rewards += stalwart_reward;
    }
    if unit > total_stalwart_rewards {
        state
            .charge(
                sponsor_id,
                unit - total_stalwart_rewards,
                "moderation penalty",
            )
            .expect("couldn't charge user");
    }
    state.denotify_users(&|u| u.stalwart);
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::env::tests::*;
    use crate::post::*;

    #[actix_rt::test]
    async fn test_reporting() {
        let mut state = State::default();

        let p = pr(0);
        let u1 = create_user(&mut state, p);
        let user = state.users.get_mut(&u1).unwrap();
        user.change_cycles(-800 + 123, "").unwrap();
        user.change_karma(100, "");

        assert_eq!(user.inbox.len(), 1);
        assert_eq!(user.cycles(), CONFIG.reporting_penalty + 123);
        assert_eq!(user.karma_to_reward(), 100);

        for i in 1..20 {
            let id = create_user(&mut state, pr(i as u8));
            let user = state.users.get_mut(&id).unwrap();
            user.stalwart = true;
        }

        let post_id = add(
            &mut state,
            "bad post".to_string(),
            vec![],
            p,
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let user = state.users.get(&u1).unwrap();
        assert_eq!(
            user.cycles(),
            CONFIG.reporting_penalty + 123 - CONFIG.post_cost
        );

        let p = state.posts.get(&post_id).unwrap();
        assert!(p.report.is_none());

        let reporter = pr(7);
        // The reporter can only be a user with at least one post.
        let _ = add(
            &mut state,
            "some post".to_string(),
            vec![],
            pr(7),
            0,
            None,
            None,
            None,
        )
        .await;

        // report should work becasue theuser needs 500 cycles
        let reporter_user = state.principal_to_user_mut(reporter).unwrap();
        assert_eq!(reporter_user.cycles(), 998);
        reporter_user.change_cycles(-998, "").unwrap();
        assert_eq!(reporter_user.cycles(), 0);
        assert_eq!(
            state.report(reporter, post_id, String::new()),
            Err("You need at least 100 cycles to report a post".into())
        );
        let p = state.posts.get(&post_id).unwrap();
        assert!(&p.report.is_none());

        let reporter_user = state.principal_to_user_mut(reporter).unwrap();
        reporter_user.change_cycles(500, "").unwrap();
        assert_eq!(reporter_user.cycles(), 500);
        state.report(reporter, post_id, String::new()).unwrap();

        // make sure the reporter is correct
        let p = state.posts.get(&post_id).unwrap();
        let report = &p.report.clone().unwrap();
        assert!(report.reporter == state.principal_to_user(reporter).unwrap().id);

        // Another user cannot overwrite the report
        assert_eq!(
            state.report(pr(8), post_id, String::new()),
            Err("This post is already reported".into())
        );
        // the reporter is stil lthe same
        assert!(report.reporter == state.principal_to_user(reporter).unwrap().id);

        // stalwart 3 confirmed the report
        vote_on_report(&mut state, pr(3), post_id, true);
        let p = state.posts.get(&post_id).unwrap();
        let report = &p.report.clone().unwrap();
        assert_eq!(report.confirmed_by.len(), 1);
        assert_eq!(report.rejected_by.len(), 0);
        // repeated confirmation is a noop
        vote_on_report(&mut state, pr(3), post_id, true);
        let p = state.posts.get(&post_id).unwrap();
        let report = &p.report.clone().unwrap();
        assert_eq!(report.confirmed_by.len(), 1);
        assert!(!report.closed);

        // stalwart 6 rejected the report
        vote_on_report(&mut state, pr(6), post_id, false);
        let p = state.posts.get(&post_id).unwrap();
        let report = &p.report.clone().unwrap();
        assert_eq!(report.confirmed_by.len(), 1);
        assert_eq!(report.rejected_by.len(), 1);
        assert!(!report.closed);

        // make sure post still exists
        assert_eq!(&p.body, "bad post");

        // stalwarts 12 & 13 confirmed too
        vote_on_report(&mut state, pr(12), post_id, true);
        let p = state.posts.get(&post_id).unwrap();
        let report = &p.report.clone().unwrap();
        assert_eq!(report.confirmed_by.len(), 2);
        assert_eq!(report.rejected_by.len(), 1);

        // stalwart has no karma to reward
        assert_eq!(state.principal_to_user(pr(3)).unwrap().karma_to_reward(), 0);

        vote_on_report(&mut state, pr(13), post_id, true);
        let p = state.posts.get(&post_id).unwrap();
        let report = &p.report.clone().unwrap();
        assert_eq!(report.confirmed_by.len(), 3);
        assert_eq!(report.rejected_by.len(), 1);

        // make sure the report is closed and post deleted
        assert!(report.closed);
        assert_eq!(&p.body, "");

        let user = state.users.get(&u1).unwrap();
        assert_eq!(user.inbox.len(), 2);
        assert_eq!(user.cycles(), 123 - CONFIG.post_cost);
        assert_eq!(user.karma(), -75);

        let reporter = state.principal_to_user(reporter).unwrap();
        assert_eq!(reporter.karma_to_reward(), CONFIG.reporting_penalty / 2);
        // stalwarts rewarded too
        assert_eq!(
            state.principal_to_user(pr(3)).unwrap().karma_to_reward(),
            CONFIG.stalwart_moderation_reward
        );
        assert_eq!(
            state.principal_to_user(pr(6)).unwrap().karma_to_reward(),
            CONFIG.stalwart_moderation_reward
        );
        assert_eq!(
            state.principal_to_user(pr(12)).unwrap().karma_to_reward(),
            CONFIG.stalwart_moderation_reward
        );

        // REJECTION TEST

        let p = pr(100);
        let u = create_user(&mut state, p);
        let user = state.users.get_mut(&u).unwrap();
        user.change_karma(100, "");
        user.change_cycles(-800, "").unwrap();
        let post_id = add(
            &mut state,
            "good post".to_string(),
            vec![],
            p,
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let user = state.users.get(&u).unwrap();
        assert_eq!(user.cycles(), CONFIG.reporting_penalty - CONFIG.post_cost);

        let reporter = pr(7);
        state.report(reporter, post_id, String::new()).unwrap();
        // set cycles to 777
        let reporter_user = state.principal_to_user_mut(reporter).unwrap();
        reporter_user
            .change_cycles(-reporter_user.cycles() + 777, "")
            .unwrap();
        assert_eq!(reporter_user.cycles(), 777);
        reporter_user.apply_rewards();
        assert_eq!(reporter_user.karma(), 125);

        vote_on_report(&mut state, pr(6), post_id, false);
        let p = state.posts.get(&post_id).unwrap();
        let report = &p.report.clone().unwrap();
        assert_eq!(report.confirmed_by.len(), 0);
        assert_eq!(report.rejected_by.len(), 1);
        assert!(!report.closed);
        assert_eq!(&p.body, "good post");

        vote_on_report(&mut state, pr(9), post_id, false);
        let p = state.posts.get(&post_id).unwrap();
        let report = &p.report.clone().unwrap();
        assert_eq!(report.confirmed_by.len(), 0);
        assert_eq!(report.rejected_by.len(), 2);

        vote_on_report(&mut state, pr(10), post_id, false);
        let p = state.posts.get(&post_id).unwrap();
        let report = &p.report.clone().unwrap();
        assert_eq!(report.confirmed_by.len(), 0);
        assert_eq!(report.rejected_by.len(), 3);
        assert!(report.closed);

        // karma and cycles stay untouched
        let user = state.users.get(&u).unwrap();
        assert_eq!(user.cycles(), CONFIG.reporting_penalty - CONFIG.post_cost);
        assert_eq!(user.karma_to_reward(), 100);

        // reported got penalized
        let reporter = state.principal_to_user(reporter).unwrap();
        let unit = CONFIG.reporting_penalty / 2;
        assert_eq!(reporter.cycles(), 777 - 2 * unit);
        assert_eq!(reporter.karma(), 25);

        assert_eq!(
            state.principal_to_user(pr(9)).unwrap().karma_to_reward(),
            CONFIG.stalwart_moderation_reward
        );
        // he voted twice
        assert_eq!(
            state.principal_to_user(pr(6)).unwrap().karma_to_reward(),
            CONFIG.stalwart_moderation_reward * 2
        );
    }
}
