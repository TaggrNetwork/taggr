import * as React from "react";
import {PostFeed} from "./post_feed";
import {loadFile} from "./form";
import {bigScreen, ButtonWithLoading, HeadBar, Loading, NotFound, RealmRibbon, setTitle, userList, } from "./common";
import { Content } from './content';
import {Edit} from "./icons";

export const RealmForm = ({existingName}) => {
    const editing = !!existingName;
    const users = window.backendCache.users;
    const name2Id = Object.keys(users).reduce((acc, id) => {
        acc[users[id]] = id;
        return acc;
    }, {});
    const userId = api._user.id;

    const [name, setName] = React.useState("");
    const [logo, setLogo] = React.useState("");
    const [labelColor, setLabelColor] = React.useState("");
    const [description, setDescription] = React.useState("");
    const [controllersString, setControllersString] = React.useState(users[userId]);
    const [controllers, setControllers] = React.useState([userId]);
    const [loading, setLoading] = React.useState(false);

    const loadRealm = async () => {
        let result = await api.query("realm", existingName);
        if ("Err" in result) {
            alert(`Error: ${result.Err}`);
            return;
        }
        const realm = result.Ok;
        setName(existingName);
        setDescription(realm.description);
        setControllers(realm.controllers);
        setLabelColor(realm.label_color || "#ffffff");
        setControllersString(realm.controllers.map(id => users[id]).join(", "));
    };
    React.useEffect(() => { if (editing) loadRealm() }, []);

    const valid = name && description && controllers.length > 0;
    return <div className="spaced">
        <h1 className="vcentered">
            {logo && <img alt="Logo" className="right_spaced" style={{ maxWidth: "70px"}} src={`data:image/png;base64, ${logo}`} />}
            <span className="max_width_col">{editing ? "EDIT" : "CREATE"} REALM</span>
        </h1>
        <div className="column_container">
            {editing && <div className="column_container bottom_spaced monospace">
                <div className="bottom_half_spaced">LOGO ({`${backendCache.config.max_realm_logo_len / 1024}`}KB MAX, resize <a href="https://imageresizer.com">here</a>)</div>
                <input type="file" onChange={async ev => {
                    const file = (ev.dataTransfer || ev.target).files[0];
                    const content = new Uint8Array(await loadFile(file));
                    const actualSize = content.byteLength, expectedSize = backendCache.config.max_realm_logo_len;
                    if (content.byteLength > backendCache.config.max_realm_logo_len) {
                        alert(`Logo size must be below ${Math.ceil(expectedSize/1024)}KB, while yours has ${Math.ceil(actualSize/1024)}KB.`);
                        return;
                    }
                    setLogo(btoa(String.fromCharCode.apply(null, new Uint8Array(content))));
                }} />
            </div>}
            {!editing && <div className="column_container bottom_spaced monospace">
                <div className="bottom_half_spaced">REALM NAME
                    {name.length > backendCache.config.max_realm_name && 
                        <span>&nbsp;[⚠️ MUST BE {backendCache.config.max_realm_name} CHARACTERS OR LESS!]</span>}
                </div>
                <input className="monospace" placeholder="alphanumeric" type="text" value={name}
                    onChange={event => {
                        const name = event.target.value.toUpperCase();
                        setName(name);
                    }} />
            </div>}
            <div className="bottom_spaced monospace" style={{position: "relative"}}>
                LABEL COLOR<br />
                <input type="color" value={labelColor} onChange={ev => setLabelColor(ev.target.value)} />
                <RealmRibbon col={labelColor} name={name} />
            </div>
            <div className="column_container bottom_spaced monospace">
                <div className="bottom_half_spaced">DESCRIPTION</div>
                <textarea rows={10} value={description}
                    onChange={event => setDescription(event.target.value)}></textarea>
            </div>
            <div className="framed bottom_spaced">
                <Content value={description} preview={true} classNameArg="bottom_spaced" />
            </div>
            <div className="column_container bottom_spaced monospace">
                <div className="bottom_half_spaced">REALM CONTROLLERS (COMMA-SEPARATED)</div>
                <input className="monospace" type="text"
                    value={controllersString} onChange={event => {
                        const input = event.target.value;
                        const ids = input.split(",")
                            .map(id => name2Id[id.replace("@", "").trim()])
                            .filter(Boolean);
                        setControllersString(input);
                        setControllers(ids);
                    }} />
            </div>
            {controllers.length > 0 &&
            <div className="column_container bottom_spaced monospace">
                <div className="bottom_half_spaced">VALID CONTROLLERS: {userList(controllers)}</div>
            </div>}
            {loading && <Loading spaced={false} />}
            {!loading && <button className={valid ? "active" : "inactive"} onClick={async () => {
                if (!valid) return;
                setLoading(true);
                const response = await api.call(editing ? "edit_realm" : "create_realm",
                    name, logo, labelColor, description, controllers.map(id => parseInt(id)));
                await window.reloadCache();
                setLoading(false);
                if ("Err" in response) {
                    alert(`Error: ${response.Err}`);
                    return;
                }
                else location.href = `/#/realm/${name}`;
            }}>{editing ? "SAVE" : "CREATE"}</button>}
        </div>
    </div>
}

export const RealmPage = ({name}) => {
    const [realm, setRealm] = React.useState(null);
    const [showMembers, setShowMembers] = React.useState(false);
    const loadRealm = async () => {
        let result = await api.query("realm", name);
        if ("Err" in result) {
            setRealm(404);
            return;
        }
        setRealm(result.Ok);
    };
    React.useEffect(() => { loadRealm(); }, []);
    if (realm == 404) return <NotFound />;
    if (!realm) return <Loading />;
    setTitle(`realm ${name}`);
    const user = api._user;
    return <>
        <HeadBar title={<div className="vcentered max_width_col">
            {realm.logo && <img alt="Logo" className="right_half_spaced" style={{ maxWidth: "40px"}} src={`data:image/png;base64, ${realm.logo}`} />}
            {name}
        </div>} shareLink={`realm/${name.toLowerCase()}`} shareTitle={`Realm ${name} on ${backendCache.name}`}
            content={<>
                {user && realm.controllers.includes(user.id) && 
                <button className="right_half_spaced" onClick={() => location.href = `/#/realm/${name}/edit`}><Edit /></button>}
                {user && !user.realms.includes(name) && <ButtonWithLoading
                    label="JOIN" classNameArg="active right_half_spaced"
                    onClick={async () => {
                        if (!confirm(`By joining the realm ${name} you confirm that you understand its description and agree with all terms and conditions mentioned there. Any rule violation can lead to moderation by stalwarts.`))
                            return false;
                        return api.call("toggle_realm_membership", name).then(api._reloadUser).then(loadRealm);
                    }} />}
                {user && user.realms.includes(name) && <ButtonWithLoading classNameArg="right_half_spaced" label="LEAVE"
                    onClick={async () => api.call("toggle_realm_membership", name).then(api._reloadUser).then(loadRealm)} />}
            </>} />
        <div className="spaced">
            <Content value={realm.description} />
            <p>Members: {showMembers ? userList(realm.members) : <a href="" onClick={e => {e.preventDefault(); setShowMembers(true)}}>{realm.members.length}</a>}</p>
        </div>
        <hr />
        <PostFeed title={<h2 className="spaced">Latest Posts</h2>}
            grid={true} feedLoader={async page => await api.query("realm_posts", name, page, false)} />
    </>;
}

export const Realms = () => {
    const [realms, setRealms] = React.useState([]);
    const [page, setPage] = React.useState(0);
    const [noMoreData, setNoMoreData] = React.useState(false);
    const loadRealms = async () => {
        let data = await api.query("realms", page);
        if (data.length == 0) {
            setNoMoreData(true);
        }
        setRealms(realms.concat(data))
    };
    React.useEffect(() => { loadRealms(); }, [page]);
    const user = api._user;
    return <>
        <HeadBar title="Realms" shareLink="realms"
            content={user && <button className="active" onClick={() => location.href = "/#/realm//new"}>CREATE</button>} />
        <div className={bigScreen() ? "two_column_grid" : null} style={{rowGap: 0, columnGap: "1em"}}>
            {realms.map(([name, realm]) =>{
                return <div key={name} className="stands_out" style={{position: "relative"}}>
                    <RealmRibbon name={name} />
                    <h3 className="vcentered">
                        {realm.logo && <img alt="Logo" className="right_spaced" style={{ maxWidth: "70px"}} src={`data:image/png;base64, ${realm.logo}`} />}
                        <span className="max_width_col">
                            <a href={`/#/realm/${name.toLowerCase()}`}>{name}</a>
                        </span>
                    </h3>
                    <Content value={realm.description.split("\n")[0]} classNameArg="bottom_spaced" />
                    <div>
                        <code>{realm.posts.length}</code> posts, <code>{realm.members.length}</code> members,
                        controlled by: {userList(realm.controllers)}
                    </div>
                </div>;})}
        </div>
        {!noMoreData && <div style={{display:"flex", justifyContent: "center"}}>
            <ButtonWithLoading classNameArg="active" onClick={() => setPage(page + 1)} label="MORE" />
        </div>}
    </>;
}
