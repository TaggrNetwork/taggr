import * as React from "react";
import { ICPAccountBalance, intFromBEBytes, timeAgo, hoursTillNext, bigScreen, HeadBar, userList, token, UserLink, icpCode, IcpAccountLink } from "./common";
import {Content} from "./content";
import {ActiveUser, Binary, Bootcamp, Box, Canister, Canisters, Cash, CashCoin, Comment, Crowd, Cycles, Document, Fire, Gear, Gem, Globe, HourGlass, Online, Post, StorageCanister, Treasury, Trophy, User} from "./icons";

const show = (number, unit = null) => <code>{number.toLocaleString()}{unit}</code>;

export const Dashboard = ({fullMode}) => {
    const stats = window.backendCache.stats;
    const [logs, setLogs] = React.useState([]);

    React.useEffect(() => { api.query("logs").then(logs => {
        logs.reverse();
        setLogs(logs);
    });}, []);

    const { config: {distribution_interval_hours}, stats: {last_distribution}} = backendCache;
    return <>
        {fullMode && <HeadBar title="Dashboard" shareLink="dashboard" />}
        {!fullMode && 
            <div className="text_centered">
                <div className={`${bigScreen() ? "four_column_grid" : "two_column_grid"} monospace bottom_spaced`}>
                    <div className="db_cell"><label><User /> USERS</label>{show(stats.users)}</div>
                    <div className="db_cell"><label><ActiveUser /> ACTIVE (7d)</label>{show(stats.active_users)}</div>
                    <div className="db_cell"><label><Online /> ONLINE</label>{show(Math.max(1, backendCache.stats.users_online))}</div>
                    <div className="db_cell"><label><Comment /> POSTS</label>{show(stats.posts + stats.comments)}</div>
                    <div className="db_cell"><label><Box /> APP STATE</label>{sizeMb(stats.state_size + stats.buckets.reduce((acc, [, e]) => acc + e, 0), "xx_large_text")}</div>
                    <div className="db_cell"><label><Gem /> TOKEN SUPPLY</label><code className="xx_large_text">{token(stats.circulating_supply)}</code></div>
                    <div className="db_cell"><label><CashCoin /> REWARDS SHARED</label>{icpCode(stats.total_rewards_shared)}</div>
                    <div className="db_cell"><label><Cash /> REVENUE SHARED</label>{icpCode(stats.total_revenue_shared)}</div>
                </div>
                <a className="top_spaced bottom_spaced" href="/#/dashboard">DASHBOARD &#x279C;</a>
            </div>}
        {fullMode && 
            <div className="text_centered">
                <div className={`${bigScreen() ? "four_column_grid" : "two_column_grid"} monospace`}>
                    <div className="db_cell"><label><User /> USERS</label>{show(stats.users)}</div>
                    <div className="db_cell"><label><ActiveUser /> ACTIVE (7d)</label>{show(stats.active_users)}</div>
                    <div className="db_cell"><label><Online /> ONLINE</label>{show(Math.max(1, backendCache.stats.users_online))}</div>
                    <div className="db_cell"><label><Crowd /> INVITED</label>{show(stats.invited_users)}</div>
                    <div className="db_cell"><label><Post /> POSTS</label>{show(stats.posts)}</div>
                    <div className="db_cell"><label><Comment /> COMMENTS</label>{show(stats.comments)}</div>
                    <div className="db_cell"><label><Bootcamp /> BOOTCAMPERS</label>{show(stats.bootcamp_users)}</div>
                    <div className="db_cell"><label><Box /> APP STATE</label>{sizeMb(stats.state_size + stats.buckets.reduce((acc, [, e]) => acc + e, 0), "xx_large_text")}</div>
                    <div className="db_cell"><label><Treasury /> <IcpAccountLink address={stats.account} label={"TREASURY"}/></label><ICPAccountBalance address={stats.account} /></div>
                    <div className="db_cell"><label><HourGlass /> DISTRIBUTION</label><code className="xx_large_text">{`${hoursTillNext(distribution_interval_hours, last_distribution)}h`}</code></div>
                    <div className="db_cell"><label><Cycles /> CYCLES SUPPLY</label>{show(stats.cycles)}</div>
                    <div className="db_cell"><label><Fire /> CYCLES BURNED</label>{show(stats.burned_cycles_total)}</div>
                    <div className="db_cell"><label><Cash /> WEEK'S REVENUE</label>{show(stats.burned_cycles)}</div>
                    <div className="db_cell"><label><Gem /> TOKEN SUPPLY</label><code className="xx_large_text">{token(stats.circulating_supply)}</code></div>
                    <div className="db_cell"><label><CashCoin /> REWARDS SHARED</label>{icpCode(stats.total_rewards_shared)}</div>
                    <div className="db_cell"><label><Cash /> REVENUE SHARED</label>{icpCode(stats.total_revenue_shared)}</div>
                </div>
            </div>}
        {fullMode &&
            <div className="monospace spaced">
                <hr />
                <div className="text_centered">
                    <h1><Canisters /> Canisters</h1>
                    <div className={bigScreen() ? "four_column_grid" : "two_column_grid"}>
                        <div className="db_cell">
                            <a href={`https://dashboard.internetcomputer.org/canister/${backendCache.stats.canister_id}`}><Canister /> MAIN</a>
                            <div className="db_cell top_spaced bottom_spaced">
                                <label><Box /> STATE</label> {sizeMb(stats.state_size)}
                            </div>
                            <div className="db_cell">
                                <label><Cycles /> IC-CYCLES</label> {show(stats.canister_cycle_balance / 10**12, "T")}
                            </div>
                        </div>
                        {stats.buckets.map(([bucket_id, size], i) => <div key={bucket_id} className="db_cell">
                            <a href={`https://dashboard.internetcomputer.org/canister/${bucket_id}`}>
                                <StorageCanister /> STORAGE {i}
                            </a>
                            <div className="db_cell top_spaced bottom_spaced"><label><Box /> STATE</label> {sizeMb(size)}</div>
                            <div className="db_cell"><label><Cycles /> IC-CYCLES</label> <CycleBalance id={bucket_id}/></div>
                        </div>)}
                        <div className="db_cell bottom_spaced">
                            <label><Gear /> UPGRADE</label>
                            <code>{timeAgo(stats.last_upgrade)}</code>
                        </div>
                        <div className="db_cell">
                            <label><Binary /> VERSION</label>
                            <a className="monospace xx_large_text" href="#/proposals">{(stats.module_hash || "").slice(0,8)}</a>
                        </div>
                    </div>
                </div>
                <hr />
                <div className="text_centered">
                    <h1><Globe /> Domains</h1>
                    <div className={bigScreen() ? "four_column_grid" : "two_column_grid"} style={{rowGap: "1em"}}>
                        {backendCache.config.domains.map(domain => <a key={domain} href={`https://${domain}`}>{domain}</a>)}
                    </div>
                </div>
                <hr />
                <div className={bigScreen() ? "two_column_grid_flex" : null}>
                    <div>
                        <h2>⚔️ Stalwarts</h2>
                        {userList(stats.stalwarts)}
                    </div>
                    <div>
                        <h2>🤖 Bots</h2>
                        {userList(stats.bots)}
                    </div>
                </div>
                <hr />
                <h2><Trophy /> WEEKLY KARMA LEADERS</h2>
                <hr />
                <div className={bigScreen() ? "four_column_grid" : "two_column_grid_flex"}>
                    {stats.weekly_karma_leaders.map(([id, karma]) => <div key={id}><UserLink id={id} /> (<span className="accent">{karma.toLocaleString()}</span>)</div>)}
                </div>
                <hr />
                <h2><Document /> Logs</h2>
                <hr />
                <Content value={logs.map(({timestamp, level, message}) => 
                    `\`${shortDate(new Date(parseInt(timestamp) / 1000000))}\`: ` + 
                    `${level2icon(level)} ` +
                    `${message}`).join("\n- - -\n")} classNameArg="monospace" />
            </div>
        }
    </>;
}

const shortDate = date => {
    let options = {  month: 'short', day: 'numeric', hour: 'numeric', minute: 'numeric', second: 'numeric' };
    return new Intl.DateTimeFormat('default', options).format(date);
}

const level2icon = level => {
    switch (level) {
        case "INFO":
            return "";
        case "ERROR":
            return "⚠️";
        case "CRITICAL":
            return "❌";
        default:
            return "❓";
    }
};

const sizeMb = size => <code className="xx_large_text">{Math.ceil(parseInt(size) / 1024 / 1024).toLocaleString()}MB</code>;

const CycleBalance = ({id}) => {
    const [cycles, setCycles] = React.useState(-1);
    React.useEffect(() => {
        api.query_raw(id, "balance").then(response => setCycles(intFromBEBytes(Array.from(response))));
    }, [id])
    return <code className="xx_large_text">{show(cycles/ 10**12, "T")}</code>
}

