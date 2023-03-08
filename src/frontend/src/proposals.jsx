import {HeadBar, Loading, ButtonWithLoading, bigScreen, timeAgo, token, userList, percentage} from "./common";
import * as React from "react";
import {Content} from "./content";
import {loadFile, MAX_POST_SIZE_BYTES} from "./form";

const REPO="https://github.com/TaggrNetwork/taggr/commit";

export const Proposals = () => {
    const [proposals, setProposals] = React.useState(null);
    const [currProp, setCurrProp] = React.useState(-1);
    const [page, setPage] = React.useState(0);
    const [noMoreData, setNoMoreData] = React.useState(false);
    const [showMask, toggleMask] = React.useState(false);
    const [binary, setBinary] = React.useState(null);
    const [commit, setCommit] = React.useState("");
    const [description, setDescription] = React.useState("");
    const [status, setStatus] = React.useState("");

    const loadState = async () => {
        const props = await api.query("proposals", page);
        if (props.length == 0) {
            setNoMoreData(true);
        }
        const data = page == 0 ? props : (proposals || []).concat(props);
        setProposals(data);
        return data;
    };

    React.useEffect(() => { loadState(); }, [page]);

    const users = backendCache.users;

    const statusEmoji = status => { return {"OPEN": "✨", "REJECTED": "🟥", "CANCELLED": "❌", "EXECUTED": "✅" }[status] || "⏳" };

    const vote = async (proposal_id, adopted) => {
        setCurrProp(proposal_id);
        const prevStatus = proposals[0].status;
        const result = await api.call("vote_on_proposal", proposal_id, adopted);
        if ("Err" in result) {
            setStatus(`Error: ${result.Err}`);
            return;
        }
        const data = await loadState();
        const lastProposal = data[0];
        const newStatus = lastProposal.status;
        if (prevStatus == "Open" && newStatus == "Executed" && "Release" in lastProposal.payload) {
            setStatus("Executing the upgrade...");
            await api.call("execute_upgrade");
            setStatus("Finalizing the upgrade...");
            if (await api.call("finalize_upgrade", lastProposal.payload.Release.hash)) {
                setStatus(newStatus.toUpperCase());
                await loadState();
            } else {
                setStatus("Upgrade execution failed.");
            }
        }
    };

    return <>
        <HeadBar title="Proposals" shareLink="proposals" menu={true}
            content={<div className="row_container">
                <ButtonWithLoading classNameArg="max_width_col" label="FUNDING" onClick={async () => {
                    let receiver = prompt("Enter the principal of the receiver.");
                    let amount = parseInt(prompt(`Enter the token amount (max. allowed amount is ${backendCache.config.max_funding_amount.toLocaleString()})`));
                    let description = prompt("Enter the proposal description.");
                    let response = await api.call("propose_funding", description, receiver, amount);
                    if ("Err" in response) {
                        alert(`Error: ${response.Err}`);
                    }
                    await loadState();
                }} />
                <ButtonWithLoading classNameArg="max_width_col" label="CONTROLLER" onClick={async () => {
                    if(!confirm("This proposal will add a new controller to the main canister! " +
                        "It is needed for emergency cases, when the upgrade mechanisms stops working due to a bug. " +
                        "Do you want to continue?")) return;
                    let controller = prompt("Enter the principal of the controller.");
                    let description = prompt("Enter the proposal description.");
                    let response = await api.call("propose_controller", description, controller);
                    if ("Err" in response) {
                        alert(`Error: ${response.Err}`);
                    }
                    await loadState();
                }} />
                <button className="max_width_col active" onClick={() => toggleMask(!showMask)}>RELEASE</button>
            </div>} />
        <div className="vertically_spaced">
            {!proposals && <Loading />}
            {showMask && <div className="spaced column_container monospace">
                <div className="row_container vcentered bottom_half_spaced">COMMIT<input type="text" className="monospace left_spaced max_width_col" onChange={async ev => { setCommit(ev.target.value); }} /></div>
                <div className="row_container vcentered bottom_half_spaced">BINARY<input type="file" className="monospace left_spaced max_width_col" onChange={async ev => {
                    const file = (ev.dataTransfer || ev.target).files[0];
                    const content = new Uint8Array(await loadFile(file));
                    if (content.byteLength > MAX_POST_SIZE_BYTES) {
                        alert(`Error: the binary cannot be larger than ${MAX_POST_SIZE_BYTES} bytes.`);
                        return;
                    }
                    setBinary(content);
                }} /></div>
                <div className="bottom_half_spaced monospace">DESCRIPTION</div>
                <textarea className="monospace bottom_spaced" rows={10} value={description} onChange={event => setDescription(event.target.value)}></textarea>
                {description && <Content value={description} preview={true} classNameArg="bottom_spaced framed" />}
                <ButtonWithLoading classNameArg="active" onClick={async () => {
                    if (!description || !binary) {
                        alert("Error: incomplete data.");
                        return;
                    }
                    const result = await api.propose_release(description, commit, binary);
                    if ("Err" in result) {
                        alert(`Error: ${result.Err}`);
                        return;
                    }
                    toggleMask(!showMask);
                    await loadState();
                }} label="SUBMIT" />
            </div>}
            {proposals && proposals.map(proposal => {
                const voted = proposal.bulletins.some(vote => api._user.id == vote[0]);
                const adopted = proposal.bulletins.reduce((acc, [_, adopted, votes]) => adopted ? acc + votes : acc, 0);
                const rejected = proposal.bulletins.reduce((acc, [_, adopted, votes]) => !adopted ? acc + votes : acc, 0);
                const open = proposal.status == "Open";
                const commit = proposal.payload.Release ? chunks(proposal.payload.Release.commit).join(" ") : null;
                const hash = proposal.payload.Release ? chunks(proposal.payload.Release.hash).join(" ") : null;
                const dailyDrop = proposal.voting_power / 100;
                const t = backendCache.config.proposal_approval_threshold;
                const days = Math.ceil((proposal.voting_power - (adopted > rejected ? adopted / t : rejected / (100 - t)) * 100) / dailyDrop);
                const propStatus = currProp == proposal.id && status ? status : proposal.status.toUpperCase();
                return <div key={proposal.timestamp}
                    className="stands_out column_container">
                    <div className="monospace bottom_half_spaced">ID: <code>{proposal.id}</code></div>
                    <div className="monospace bottom_half_spaced">TYPE: {Object.keys(proposal.payload)[0].toUpperCase()}</div>
                    <div className="monospace bottom_half_spaced">PROPOSER: <a href={`#/user/${proposal.proposer}`}>{`@${users[proposal.proposer]}`}</a></div>
                    <div className="monospace bottom_half_spaced">DATE: {timeAgo(proposal.timestamp)}</div>
                    <div className="monospace bottom_spaced">STATUS: {statusEmoji(propStatus)} <span className={open ? "accent" : null}>{propStatus}</span></div>
                    {"Release" in proposal.payload && <div className="monospace bottom_spaced">
                        {commit && <div className="row_container bottom_half_spaced">COMMIT:<a className="monospace left_spaced" href={`${REPO}/${proposal.payload.Release.commit}`}>{commit}</a></div>}
                        <div className="row_container"><span>HASH:</span><code className="left_spaced monospace">{hash}</code></div>
                    </div>}
                    {"SetController" in proposal.payload && <div className="monospace bottom_half_spaced">Principal: <code>{proposal.payload.SetController}</code></div>}
                    {"Fund" in proposal.payload && <>
                        <div className="monospace bottom_half_spaced">Receiver: <code>{proposal.payload.Fund[0]}</code></div>
                        <div className="monospace bottom_half_spaced">Amount: <code>{proposal.payload.Fund[1].toLocaleString()}</code></div>
                    </>}
                    <Content value={proposal.description} classNameArg="bottom_spaced" />
                    <div className="monospace bottom_spaced">
                        EFFECTIVE VOTING POWER: <code>{token(proposal.voting_power)}</code>
                    </div>
                    {open && !isNaN(days) && <div className="monospace bottom_spaced">
                        EXECUTION DEADLINE: <code>{days}</code> DAYS
                    </div>}
                    <div className="monospace bottom_spaced">
                        <div className="bottom_half_spaced">ADOPTED: <b className={adopted > rejected && open ? "accent" : null}>{token(adopted)}</b> ({percentage(adopted, proposal.voting_power)})</div>
                        <div className="small_text">{users && userList(proposal.bulletins.filter(vote => vote[1]).map(vote => vote[0]))}</div>
                    </div>
                    <div className="monospace bottom_spaced">
                        <div className="bottom_half_spaced">REJECTED: <b className={adopted < rejected && open ? "accent" : null}>{token(rejected)}</b> ({percentage(rejected, proposal.voting_power)})</div>
                        <div className="small_text">{users && userList(proposal.bulletins.filter(vote => !vote[1]).map(vote => vote[0]))}</div>
                    </div>
                    {api._user && open && !voted && <>
                        <div className="row_container">
                            <ButtonWithLoading onClick={() => vote(proposal.id, false)} classNameArg="max_width_col large_text" label="REJECT" />
                            <ButtonWithLoading onClick={() => vote(proposal.id, true)} classNameArg="max_width_col large_text" label="ADOPT" />
                        </div>
                    </>}
                    {api._user && api._user.id == proposal.proposer && open &&
                        <ButtonWithLoading onClick={async () => {
                            await api.call("cancel_proposal", proposal.id);
                            await loadState();
                        }} classNameArg="top_spaced max_width_col large_text" label="CANCEL" />}
                </div>;
            })}</div>
        {!noMoreData && <div style={{display:"flex", justifyContent: "center"}}>
            <ButtonWithLoading classNameArg="active" onClick={() => setPage(page + 1)} label="MORE" />
        </div>}
    </>;
}

const chunks = s => s ? [s.slice(0, 8)].concat(chunks(s.slice(8))) : [];
