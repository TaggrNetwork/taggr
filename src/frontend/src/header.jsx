import * as React from "react";
import {bigScreen, BurgerButton, ButtonWithLoading, Loading, ReactionToggleButton, realmColors, RealmSpan, ToggleButton} from "./common";
import {authMethods, LoginMasks} from "./logins";
import {Bell, CarretDown, Close, Cycles} from "./icons";

const logout = () => {
    location.href = "/";
    localStorage.clear();
    authMethods[api._method].logout();
}

export const Header = ({subtle, route}) => {
    let user = api._user;
    const [showLogins, setShowLogins] = React.useState(!user && location.href.includes("?join"));
    const [showButtonBar, toggleButtonBar] = React.useState(false);
    const [showRealms, toggleRealms] = React.useState(false);
    const [loading, setLoading] = React.useState(false);
    const [realmBg, realmFg] = realmColors(user?.current_realm);
    const inboxEmpty = !user || Object.keys(user.inbox).length == 0;
    const inRealm = user && user.current_realm;
    React.useEffect(() => { document.getElementById("logo").innerHTML = backendCache.config.logo; }, []);
    React.useEffect(() => { toggleButtonBar(false); toggleRealms(false) }, [route]);
    return <>
        <header className={`spaced top_half_spaced vcentered ${subtle ? "subtle" : ""}`}>
            <a href="#/home" id="logo"></a>
            {user && user.realms.length > 0 && !subtle && <ReactionToggleButton classNameArg="left_half_spaced"
                pressed={showRealms} onClick={() => { toggleRealms(!showRealms); toggleButtonBar(false) }}
                icon={<CarretDown classNameArg="large_text" />} />}
            {inRealm && <ButtonWithLoading classNameArg="left_half_spaced monospace"
                styleArg={{background: realmBg, padding: "0.2em"}}
                onClick={async () =>{
                    await api.call("enter_realm", "");
                    await api._reloadUser();
                    location.href = "/#/main";
                }}
                label={<div className="row_container vcentered">
                    <RealmSpan classNameArg="padded_rounded smaller_text" name={user.current_realm}/>
                    <Close styleArg={{fill: realmFg}} small={true} />
                </div>}
            />}
            <div className="row_container vcentered max_width_col flex_ended">
                {!subtle &&  <>
                    {user && !inboxEmpty && <span className="clickable" onClick={() => location.href = "#/inbox"}><Bell /><code className="left_half_spaced right_spaced">{`${Object.keys(user.inbox).length}`}</code></span>}
                    {user && inboxEmpty && <div className="vcentered"><Cycles /><code className="left_half_spaced right_spaced">{`${user.cycles}`}</code></div>}
                    {user && (bigScreen() || !user.current_realm) && <PostButton classNameArg="right_half_spaced" />}
                    {!api._principalId && <ToggleButton 
                        classNameArg={!showLogins && "active"}
                        toggler={() => setShowLogins(!showLogins)} currState={() => showLogins} onLabel="CLOSE" offLabel="ENTER" />}
                </>}
                {api._principalId && 
                    <BurgerButton onClick={() => { toggleButtonBar(!showButtonBar); toggleRealms(false) }} pressed={showButtonBar} />}
            </div>
        </header>
        {showLogins && <LoginMasks />}
        {showButtonBar && (!bigScreen() && user.current_realm) && <div className="spaced row_container"><PostButton classNameArg="max_width_col" /></div>}
        {showButtonBar && <div className="two_column_grid monospace top_spaced stands_out" style={{ rowGap: "1em" }}>
            {user && <a className="iconed" onClick={() => toggleButtonBar(!showButtonBar)} href={`/#/journal/${user.name}`}>📓 JOURNAL</a>}
            {user && <a className="iconed" onClick={() => toggleButtonBar(!showButtonBar)} href={`/#/user/${user.name}`}>👤 {api._user.name.toUpperCase()}</a>}
            {user && <a className="iconed" onClick={() => toggleButtonBar(!showButtonBar)} href={`/#/realms`}>🎭 REALMS</a>}
            {user && <a className="iconed" onClick={() => toggleButtonBar(!showButtonBar)} href={`/#/bookmarks`}>📑 BOOKMARKS</a>}
            {user && <a className="iconed" onClick={() => toggleButtonBar(!showButtonBar)} href="/#/wallets">💳 WALLETS</a>}
            {user && <a className="iconed" onClick={() => toggleButtonBar(!showButtonBar)} href="/#/invites">🎟 INVITES</a>}
            {user && <a className="iconed" onClick={() => toggleButtonBar(!showButtonBar)} href="/#/settings">⚙️ SETTINGS</a>}
            <a className="iconed" onClick={() => toggleButtonBar(!showButtonBar)} href="/#/dashboard">📊 DASHBOARD</a>
            <a className="iconed" onClick={() => toggleButtonBar(!showButtonBar)} href="/#/tokenomics">💎 TOKENOMICS</a>
            <a className="iconed" onClick={() => toggleButtonBar(!showButtonBar)} href="/#/proposals">🎈 PROPOSALS</a>
            <a className="iconed" onClick={() => toggleButtonBar(!showButtonBar)} href="/#/whitepaper">📄 WHITE PAPER</a>
            <a className="iconed" href="" onClick={logout}>🔌 LOGOUT</a>
        </div>}
        {showRealms && <div className={`${bigScreen() ? "four_column_grid" : "two_column_grid"} monospace top_spaced stands_out`}>
            {user.realms.map(realm => <RealmSpan key={realm}
                classNameArg="left_half_spaced right_half_spaced clickable padded_rounded text_centered"
                onClick={async () => {
                    toggleRealms(false);
                    setLoading(true);
                    await api.call("enter_realm", realm);
                    await api._reloadUser();
                    location.href = "/#/_";
                    setLoading(false);
                }} name={realm} />)}
        </div>}
        {loading && <Loading />}
    </>;
}

const PostButton = ({classNameArg}) =>
    <button className={`active ${classNameArg || ""}`} onClick={() => location.href = "#/new"}>POST</button>;
