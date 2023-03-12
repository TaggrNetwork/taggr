import * as React from "react";
import {userList} from "./common";
import { Content } from './content';

export const Poll = ({poll, post_id, created}) => {
    const [data, setData] = React.useState(poll);

    React.useEffect(() => setData(poll), [poll]);

    const user_id = api._user?.id;
    const voted = Object.values(data.votes).flat().includes(user_id);
    const totalVotes = Object.values(data.votes).map(votes => votes.length).reduce((acc, e) => acc + e ,0);
    const createdHoursAgo = Math.floor((Number(new Date()) - parseInt(created) / 1000000) / 1000 / 3600);
    const expired = createdHoursAgo > poll.deadline;
    const showVoting = !isNaN(user_id) && !voted && !expired;

    return <div className="column_container poll" data-meta="skipClicks">
        {data.options.map((option, id) => {
            const votes = (data.votes[id] || []).length;
            const pc = totalVotes > 0 ? Math.ceil(votes / totalVotes * 100) : 0;
            return <label key={id} className={showVoting ? "vcentered" : null} style={{display: "flex", flexDirection: showVoting ? "row" : "column"}}>
                {showVoting && <input type="radio" value={id} name={id} className="right_spaced" style={{marginTop: 0}}
                    onChange={e => {
                        if (isNaN(post_id) || !api._user) return;
                        let vote = e.target.value;
                        api.call("vote_on_poll", post_id, parseInt(vote)).then(response => {
                            if (response.Err) {
                                alert(`Error: ${response.Err}!`);
                                return;
                            }});
                        const list = poll.votes[vote] || [];
                        list.push(user_id);
                        poll.votes[vote] = list;
                        setData({...poll});
                    }} />}
                <Content classNameArg="max_width_col clickable" value={option} />
                {!showVoting && <div className="column_container" style={{margin: "0.5em", width: "96%"}}>
                    <div className="vcentered">
                        <code className="right_half_spaced small_text"
                            style={{width: "6em", textAlign: "right", alignSelf: "flex-start"}}>{`${votes} (${pc} %)`}</code>
                        <div className="max_width_col">
                            <div style={{width: `${pc}%`, height: "0.6em", marginTop: "0.1em"}} className="active"></div>
                            <div className="small_text top_half_spaced">{userList(data.votes[id])}</div>
                        </div>
                    </div>
                </div>}
            </label>})}
        {!expired && <span className="top_spaced small_text text_centered inactive">EXPIRES IN {printDelta(data.deadline - createdHoursAgo)}</span>}
    </div>;
}

const printDelta = delta => {
    const days = Math.floor(delta / 24);
    if (days > 0) return `${days} DAY${days == 1 ? "" : "S"}`;
    return `${Math.max(1, delta)}H`;
};
