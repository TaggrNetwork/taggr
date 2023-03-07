import * as React from "react";
import { Form } from './form';
import { Content } from './content';
import { Poll } from './poll';
import { isRoot, BurgerButton, reactions, timeAgo, ToggleButton, NotFound, applyPatch, loadPostBlobs, ShareButton, commaSeparated, Loading, objectReduce, reactionCosts, postUserToPost, loadPost, ReactionToggleButton, RealmRibbon, setTitle, ButtonWithLoading, bigScreen } from './common';
import {PostFeed} from "./post_feed";
import {reaction2icon, Chat, Edit, Save, Unsave, Watch, Unwatch, Repost, Coin, Flag, New, Comment, CarretRight, Trash } from "./icons";

export const postDataProvider = (id, preloadedData = null, rootOnly = false) => {
    const provider = { root: id, source: {} };
    const load = async () => {
        if (rootOnly) {
            const post = await loadPost(api, id);
            if (!post) provider.notFound = true;
            else provider.source[id] = post;
            return
        }
        const tree = await api.query("tree", id);
        if (Object.keys(tree).length == 0) {
            provider.notFound = true;
            return;
        }
        Object.keys(tree).forEach(id => tree[id] = postUserToPost(tree[id]));
        provider.source = tree;
    }
    provider.load = load;
    if (preloadedData) provider.source[id] = preloadedData;
    return provider;
};

export const Post = ({id, data, version, isFeedItem, repost, classNameArg, isCommentView, isThreadView, focused, highlighted = [], level = 0}) => {
    const [post, setPost] = React.useState(data.source[id]);
    const [blobs, setBlobs] = React.useState({});
    const [showComments, toggleComments] = React.useState(!isFeedItem && !repost);
    const [showInfo, toggleInfo] = React.useState(false);
    const [expanded, toggleExpansion] = React.useState(!isFeedItem && !repost);
    const [fullTreeIsLoading, setFullTreeIsLoading] = React.useState(false);
    const [rendering, setRendering] = React.useState(true);
    const [safeToOpen, setSafeToOpen] = React.useState(false);
    const [commentIncoming, setCommentIncoming] = React.useState(false);

    const loadData = async (force) => {
        const fullTreeLoadRequired = id in data.source && showComments && data.source[id].children.length + 1 > Object.keys(data.source).length;
        if (force || !(id in data.source) || fullTreeLoadRequired) {
            setFullTreeIsLoading(true);
            await data.load();
            setFullTreeIsLoading(false);
            if (data.notFound) {
                setPost(null);
                return;
            }
        }
        const post = data.source[id];
        setPost(post);
        setBlobs(await loadPostBlobs(post.files));
    };

    React.useEffect(() => { loadData(); }, [version, data, showComments]);
    React.useEffect(() => { setRendering(false); }, []);

    if (!post) {
        if (data.notFound) return <NotFound />;
        return <Loading />;
    }

    post.effBody = post.body;
    if (!isNaN(version)) {
        for (let i = post.patches.length-1; i >= version; i--) {
            const [_timestamp, patch] = post.patches[i]
            post.effBody = applyPatch(post.effBody, patch)[0];
        }
    }

    const commentSubmissionCallback = async (comment, blobs) => {
        const result = await api.add_post(comment, blobs, [post.id], [], []);
        if (result.Err) {
            return alert(`Error: ${result.Err}`);
        }
        // delete outdated root post data
        delete data.source[id];
        await loadData();
        toggleInfo(false);
        toggleComments(true);
    };

    const expand = e => {
        if (repost) location.href = `/#/post/${id}`;
        if(!isFeedItem || window.getSelection().toString().length > 0) return;
        if (["A", "IMG", "INPUT"].includes(e.target.tagName) || skipClicks(e.target)) return;
        if (expanded) {
            toggleComments(false);
            toggleInfo(false);
        };
        toggleExpansion(!expanded);
    };

    const react = id => {
        if (!api._user) return;
        let user_id = api._user?.id;
        if (!(id in post.reactions)) {
            post.reactions[id] = [];
        };
        let users = post.reactions[id];
        if (users.includes(user_id)) return;
        api.call("react", post.id, parseInt(id)).then(response => { 
            if ("Err" in response) alert(`Error: ${response.Err}`);
            api._reloadUser();
        });
        users.push(user_id);
        setPost({...post});
        toggleInfo(commentIncoming);
    }
    const costTable = reactionCosts();
    const sum = objectReduce(post.reactions, (acc, id, users) => acc + costTable[parseInt(id)] * users.length, 0);
    const treeLoaded = Object.keys(data.source).length > 1;
    const highlightOp = treeLoaded && post.user.id == data.source[data.root]?.user.id;
    const user = api._user;
    const showReport = post.report && !post.report.closed && user && user.stalwart;
    const deleted = post.hashes.length > 0;
    const deletedByModeration = post.report && post.report.closed && post.report.confirmed_by.length > post.report.rejected_by.length;
    const isComment = !isRoot(post);
    const commentAsPost = isComment && !isCommentView;
    const realmPost = (!isComment || !isCommentView) && post.realm && (!user || user.current_realm != post.realm);
    const isGallery = post.effBody.startsWith("![");
    const postCreated = post.patches.length > 0 ? post.patches[0][0] : post.timestamp;
    const isPrime = !isCommentView && !isFeedItem && !repost;
    const isNSFW = post.effBody.toLowerCase().includes("#nsfw") && isFeedItem && !safeToOpen;
    version = isNaN(version) && post.patches.length > 0 ? post.patches.length : version;

    if (isPrime) setTitle(`Post #${post.id} by @${backendCache.users[post.user.id]}`);

    if (deletedByModeration) return <h4 className="banner">DELETED VIA MODERATION</h4>;

    let cls = "";
    if (!deleted && !isNSFW && !showReport) {
        if (realmPost) cls = "realm_post";
        cls += isGallery ? " gallery_post" : " text_post";
    }

    return <div ref={post => { if(post && focused && rendering) post.scrollIntoView({ behavior: "smooth" }); }}
        className={classNameArg || null}>
        <div className={`post_box ${sum < 0 ? "inactive" : ""} ${cls}`} style={{position: "relative"}}>
            {showReport && <ReportBanner post={post} />}
            {isNSFW && <div className="post_head banner2 x_large_text" onClick={() => setSafeToOpen(true)}>#NSFW</div>}
            {deleted && <div className="post_head banner3 small_text monospace"><h3>Post deleted</h3>
                <ol>{post.hashes.map(hash => <li key={hash}><code>{bigScreen() ? hash : hash.slice(0,16)}</code></li>)}</ol>
            </div>}
            {realmPost && <RealmRibbon name={post.realm} />}
            {commentAsPost  && <a className="reply_tag external monospace" href={`#/thread/${post.id}`}>{post.parent} &#8592;</a>}
            {isComment && !commentAsPost && <span className="thread_button clickable"
                onClick={() => location.href = `#/thread/${post.id}`}><Comment classNameArg="action" /></span>}
            {!isNSFW && <article onClick={expand} className={isPrime ? "prime" : null}>
                {/* The key is needed to render different content for different versions to avoid running into diffrrent
                 number of memorized pieces inside content */}
                <Content key={post.effBody} post={true} value={post.effBody} blobs={blobs} collapse={!expanded} primeMode={isRoot(post) && !repost} />
                {post.extension && <Poll poll={post.extension.Poll} post_id={post.id} created={postCreated} />}
            </article>}
            <PostBar post={post} react={react} highlightOp={highlightOp} repost={repost} highlighted={highlighted}
                showComments={showComments} toggleComments={toggleComments} postCreated={postCreated}
                showInfo={showInfo} toggleInfo={toggleInfo} isThreadView={isThreadView} level={level} />
        </div>
        {showInfo && <div className="top_framed top_spaced">
            <div className="left_half_spaced right_half_spaced bottom_spaced top_spaced">
                <div className="row_container vcentered bottom_spaced flex_ended">
                    <a href={`#/post/${post.id}`}>{`#${post.id}${post.patches.length > 0 ? "*" : ""}`}</a>
                    <ShareButton classNameArg="left_spaced"
                        url={`${isComment ? "thread" : "post"}/${post.id}${isNaN(version) ? "" : "/" + version}`}
                        title={`Post ${post.id} on ${backendCache.config.name}`} />
                    <div className="max_width_col"></div>
                    {user && <ReactionsPicker post={post} react={react} />}
                </div>
                {user && post.realm && !user.realms.includes(post.realm) && <div className="text_centered framed">JOIN REALM <a href={`#/realm/${post.realm.toLowerCase()}`}>{post.realm}</a> TO COMMENT</div>}
                {user && (!post.realm || user.realms.includes(post.realm)) &&
                    <Form submitCallback={commentSubmissionCallback} postId={post.id}
                        writingCallback={() => setCommentIncoming(true)}
                        comment={true} />}
                {<PostInfo post={post} version={version} postCreated={postCreated} callback={async () => await loadData(true)} />}
            </div>
        </div>}
        {fullTreeIsLoading && <Loading />}
        {!fullTreeIsLoading && showComments && treeLoaded && post.children.length > 0 &&
            <PostFeed data={data} heartbeat={post.id + "_" + Object.keys(data.source).length} comments={true} level={level+1}
                feedLoader={async () => post.children.map(comment_id => { return {...data.source[comment_id]} })} highlighted={highlighted}
                classNameArg="left_spaced" />}
    </div>;
};

const PostInfo = ({post, version, postCreated, callback}) => {
    const linkToProfile = id => <a key={id} href={`/#/user/${id}`}>{`${window.backendCache.users[id]}`}</a> ;
    const postAuthor = api._user?.id == post.user.id;
    return <>
        {api._user && <div className="row_container top_half_spaced">
            {!postAuthor && <ButtonWithLoading classNameArg="max_width_col" onClick={async () => {
                let reason = prompt("You are reporting this post to stalwarts. If the report gets rejected, you'll lose cycles and karma. If you want to continue, please justify the report:");
                if (reason) {
                    if (reason.length > 256) {
                        alert("Please limit your message to 256 characters.");
                        return;
                    }
                    let response = await api.call("report", post.id, reason);
                    if ("Err" in response) {
                        alert(`Error: ${response.Err}`);
                        return;
                    }
                    alert("Report accepted! Thank you!")
                };
            }} label={<Flag />} />}
            <ToggleButton classNameArg="max_width_col" offLabel={<Watch />} onLabel={<Unwatch />}
                currState={() => post.watchers.includes(api._user?.id)} 
                toggler={() => api.call("toggle_following_post", post.id)} />
            <button className="max_width_col" onClick={() => location.href = `/#/new/repost/${post.id}`}><Repost /></button>
            <ToggleButton classNameArg="max_width_col"
                offLabel={<Save />} onLabel={<Unsave />}
                currState={() => api._user.bookmarks.includes(post.id)} 
                toggler={() => api.call("toggle_bookmark", post.id).then(api._reloadUser)} />
            <ButtonWithLoading classNameArg="max_width_col" onClick={async () => {
                const cycles = prompt(`Tip @${post.user.name} with cycles (tipping fee: ${backendCache.config.tipping_fee}):`, 20);
                if(cycles == null) return;
                const tip = parseInt(cycles);
                if(isNaN(tip)) alert("Couldn't parse the number of cycles.");
                let response = await api.call("tip", post.id, tip);
                if ("Err" in response) {
                    alert(`Error: ${response.Err}`);
                } else await callback();
            }} label={<Coin />} />
            {postAuthor && <>
                <button className="max_width_col" onClick={() => location.href=`/#/edit/${post.id}`}><Edit /></button>
                {post.hashes.length == 0 && <ButtonWithLoading classNameArg="max_width_col" onClick={async () => {
                    const { post_cost, post_deletion_penalty_factor } = backendCache.config;
                    const cost = objectReduce(post.reactions, (acc, id, users) => {
                        const costTable = reactionCosts();
                        let cost = costTable[parseInt(id)];
                        return acc + (cost > 0 ? cost : 0) * users.length;
                    }, 0) + post_cost + post.tree_size * post_deletion_penalty_factor;
                    if (!confirm(`Please confirm the post deletion: it will costs ${cost} cycles.`)) return;
                    let current = post.body;
                    const versions = [current];
                    for (let i = post.patches.length-1; i >= 0; i--) {
                        const [_timestamp, patch] = post.patches[i]
                        current = applyPatch(current, patch)[0];
                        versions.push(current);
                    }
                    versions.reverse();
                    let response = await api.call("delete_post", post.id, versions);
                    if ("Err" in response) {
                        alert(`Error: ${response.Err}`);
                    } else await callback();
                }} label={<Trash />} />}
            </>}
        </div>}
        <div className="small_text top_spaced">
            <b>CREATED</b>: {new Date(parseInt(postCreated) / 1000000).toLocaleString()}
            {post.patches.length > 0 && <div>
                <b>VERSIONS</b>: {commaSeparated((post.patches.concat([[post.timestamp, ""]])).map(([timestamp, _], v) => version == v 
                    ? `${version} (${timeAgo(timestamp)})`
                    : <span key={v}><a href={`/#/post/${post.id}/${v}`}>{`${v}`}</a> ({timeAgo(timestamp)})</span>))}</div>}
            {post.watchers.length > 0 && <div>
                <b>WATCHERS</b>: {commaSeparated(post.watchers.map(linkToProfile))}
            </div>}
            {post.tips.length > 0 && <div>
                <b>TIPS</b>: {commaSeparated(post.tips.map(([id, tip]) => <span key={id + tip}><code>{tip}</code> from {linkToProfile(id)}</span>))}
            </div>}
            {Object.keys(post.reactions).length > 0 && <div className="top_spaced">
                {Object.keys(post.reactions).map(id => {
                    let users = post.reactions[id];
                    const [reactId, _cost] = reactions().find(([reaction_id, _cost, _]) => reaction_id == id);
                    return <div key={id}>
                        {reaction2icon(reactId)} {commaSeparated(users.map(id => linkToProfile(id)))}
                    </div>;
                })}</div>}
        </div>
    </>;
};

const PostBar = ({post, react, highlighted, highlightOp, repost, showInfo, toggleInfo, showComments, toggleComments, postCreated, isThreadView, level}) => {
    const time = timeAgo(postCreated);
    const replies = post.tree_size;
    const createdRecently = (Number(new Date()) - parseInt(postCreated) / 1000000) < 30 * 60 * 1000;
    const updatedRecently = (Number(new Date()) - parseInt(post.tree_update) / 1000000) < 30 * 60 * 1000;
    const newPost = api._user && highlighted.includes(post.id) || (postCreated > api._last_visit || createdRecently)
    const newComments = api._user && (post.tree_update > api._last_visit || updatedRecently);
    const bar = React.useRef();
    const showCarret = level > (bar.current?.clientWidth > 900 ? 13 : 3);
    const goInside = () => location.href = `#/post/${post.id}`;
    return <div ref={bar} className="post_bar row_container_static vcentered smaller_text flex_ended">
        <div className="row_container">
            <a className={`right_spaced ${highlightOp ? "accent" : ""}`}
                href={`#/user/${post.user.id}`}>{`${post.user.name}`}</a>
            <span className="no_wrap">
                <span className="right_spaced">{time}</span>
                {newPost && <New classNameArg="accent vertically_aligned" /> }
            </span>
        </div>
        {!repost && <div className="row_container_static max_width_col flex_ended">
            <Reactions reactionsMap={post.reactions} react={react} />
            {replies > 0 && !isThreadView && <ReactionToggleButton pressed={showComments}
                onClick={showCarret ? goInside : () => { toggleInfo(false); toggleComments(!showComments) }}
                icon={<><Chat classNameArg={newComments ? "accent" : null} />&nbsp;{`${replies}`}</>}
            />}
            {!isThreadView && !showCarret && <BurgerButton onClick={() => { toggleInfo(!showInfo); toggleComments(false) }} pressed={showInfo} />}
            {(isThreadView || showCarret) && <button className="reaction_button unselected"
                onClick={goInside}><CarretRight /></button>}
        </div>}
    </div>;
}

export const ReactionsPicker = ({react}) => {
    return <div>
        {reactions().map(([id, _]) => <button key={id} className="left_half_spaced" onClick={() => react(id)}>{reaction2icon(id)}</button>)}
    </div>;
};

export const Reactions = ({reactionsMap, react}) => {
    if (Object.keys(reactionsMap).length == 0) return null;
    return <div className="row_container_static vcentered flex_ended">
        {Object.keys(reactionsMap).map(id => {
            const users = reactionsMap[id];
            const reacted = users.includes(api._user?.id);
            const reaction = reactions().find(([reaction_id, _cost, _]) => reaction_id == id);
            if (!reaction) return null;
            const [reactId, _cost] = reaction;
            return <button data-meta="skipClicks" key={id} className={"reaction_button " + (reacted ? "selected" : "unselected")}
                onClick={() => react(id)}>
                {reaction2icon(reactId)}&nbsp;{`${users.length}`}
            </button>;
        })}
    </div>;
};

const ReportBanner = ({post}) => {
    const [report, setReport] = React.useState(post.report);
    const { reporter, confirmed_by, rejected_by} = report;
    let tookAction = rejected_by.concat(confirmed_by).includes(api._user.id) ;

    return <div className="post_head banner">
        <h3>
            This post was <b>REPORTED</b> by <a href={`/#/user/${reporter}`}>@{backendCache.users[reporter]}</a>.
            Please confirm the deletion or reject the report.
        </h3>
        <h4>Reason: {post.report.reason}</h4>
        {tookAction && <div>
            {confirmed_by.length > 0 && <div>CONFIRMED BY {commaSeparated(confirmed_by.map(id => `@${backendCache.users[id]}`))}</div>}
            {rejected_by.length > 0 && <div>REJECTED BY {commaSeparated(rejected_by.map(id => `@${backendCache.users[id]}`))}</div>}
        </div>}
        {!tookAction && <div className="row_container" style={{justifyContent: "center"}}>
            {[["🛑 DISAGREE", false], ["✅ AGREE", true]].map(([label, val]) =>
            <ButtonWithLoading key={label} onClick={async () => {
                await api.call("vote_on_report", post.id, val);
                setReport((await loadPost(api, post.id)).report);
            }} label={label} />)}
        </div>}
    </div>;
}

const skipClicks = elem => elem && (elem.dataset["meta"] == "skipClicks" || skipClicks(elem.parentElement));

