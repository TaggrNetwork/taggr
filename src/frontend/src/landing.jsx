import * as React from "react";
import {Content} from './content';
import {PostFeed} from "./post_feed";
import { Dashboard } from './dashboard';
import {Search} from './search';
import {bigScreen, Loading, RealmSpan, setTitle} from './common';
import {New, User, Fire } from './icons';

const FEED_KEY = "_feed";

export const Landing = ({heartbeat}) => {
    const user = api._user;
    const [feed, setFeed] = React.useState(localStorage.getItem(FEED_KEY) || "HOT");
    const headline = `# Welcome to #${backendCache.config.name}!\nA fully autonomous decentralized social network.\n\n[WHITE PAPER &#x279C;](/#/about)`;
    const title = <div className="text_centered vertically_spaced">
        {[ {icon: <New />, id: "NEW"}, {icon: <Fire />, id: "HOT"}, user && { icon: <User />, id: "FOLLOWED" }].filter(Boolean).map(
            ({icon, id}) => <button key={id} onClick={() => {localStorage.setItem(FEED_KEY, id); setFeed(id)}}
                className={"medium_text " + (feed == id ? "active" :"unselected")}>
                {icon} {id}
            </button>
        )}
    </div>;
    return <>
        {!api._user && <Content value={headline} classNameArg="spaced text_centered" />}
        <Search />
        {!api._user && <>
            <Dashboard />
            <RealmsDashboard />
        </>}
        <TagCloud size={bigScreen() ? 60 : 30} heartbeat={heartbeat} />
        <PostFeed heartbeat={heartbeat + feed} title={title} grid={true} feedLoader={
            async page => {
                setTitle(feed);
                if (feed == "FOLLOWED") return await api.query("personal_feed", user.id, page, false);
                if (feed == "HOT") return await api.query("hot_posts", page);
                else return await api.query("last_posts", page, false);
            }} />
    </>;
}; 

const RealmsDashboard = () => {
    const realmNames = backendCache.realms;
    return <div className="vertically_spaced text_centered">
        <div className="row_container" style={{opacity: 0.8, margin: "0.5em", marginBottom: "1em"}}>
            {realmNames.slice(0, 10).map(([name, col]) => 
            <RealmSpan key={name} col={col} name={name} onClick={() => location.href = `/#/realm/${name}`}
                classNameArg="clickable max_width_col medium_text monospace padded_rounded marginized" />)}
        </div>
        <a href="#/realms">ALL REALMS &#x279C;</a>
    </div>;
}

export const TagCloud = ({size, heartbeat}) => {
    const [tags, setTags] = React.useState(null);
    const loadTags = async () => {
        let tags = await api.query("recent_tags", size);
        const occurences = tags.map(([_, N]) => parseInt(N));
        const min = Math.min(...occurences);
        const max = Math.max(...occurences);
        const bucket = (max - min) / 10;
        tags = tags.map(([tag, N]) => [tag, Math.ceil((parseInt(N) - min) / bucket)]);
        tags.sort((a, b) => a[0] > b[0] ? 1 : -1);
        setTags(tags);
    };
    React.useEffect(() => { loadTags(); }, [heartbeat]);
    if (tags == null) return <Loading />;
    return <div id="tag_cloud" className="row_container vertically_spaced">
        {tags.map(([tag, size]) => <a key={tag} className={`tag size${size}`} href={`#/feed/${tag}`}>{tag}</a>)}
    </div>;
};
