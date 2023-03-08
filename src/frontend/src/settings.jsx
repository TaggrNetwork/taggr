import * as React from "react";
import {ButtonWithLoading, HeadBar} from "./common";

export const Settings = ({invite = null}) => {
    const user = api._user;
    const [principal, setPrincipal] = React.useState(api._principalId);
    const [name, setName] = React.useState("");
    const [about, setAbout] = React.useState("");
    const [settings, setSettings] = React.useState({});
    const [controllers, setControllers] = React.useState("");
    const [label, setLabel] = React.useState(null);
    const [timer, setTimer] = React.useState(null);
    const [uiRefresh, setUIRefresh] = React.useState(false);

    React.useEffect(() => {
        if (!user) return;
        setAbout(user.about);
        setControllers(user.controllers.join("\n"));
        setSettings(user.settings);
    }, [user])

    const setSetting = (key, e) => {
        const newSettings = {};
        Object.keys(settings).forEach(k => newSettings[k] = settings[k]);
        newSettings[key] = e.target.value;
        setSettings(newSettings);
        if (["theme", "columns"].includes(key)) setUIRefresh(true);
    };

    const submit = async () => { 
        if (!user) {
            let response = await api.call("create_user", name, invite);
            if ("Err" in response) {
                return alert(`Error: ${response.Err}`);
            }
        }
        const principal_ids = controllers.split("\n").map(v => v.trim()).filter(id => id.length > 0);
        const response = await api.call("update_user", about, principal_ids, JSON.stringify(settings));
        if ("Err" in response) {
            alert(`Error: ${response.Err}`);
            return;
        }
        if (!user) location.href = "/";
        else if (uiRefresh) location.reload();
    };

    return <>
        <HeadBar title="Settings" shareLink="setting" />
        <div className="spaced monospace column_container">
            {!user && <div className="column_container bottom_spaced">
                <div className="bottom_half_spaced">USER NAME <span className="accent">[required]</span></div>
                <input type="text" value={name} className="monospace" placeholder="alphanumeric"
                    onChange={event => {
                        clearTimeout(timer);
                        const name = event.target.value;
                        if (name) setTimer(setTimeout(() => api.query("validate_username", name).then(result => 
                            setLabel("Err" in result ? result.Err : "free!")), 300));
                        setName(name); }} />
                <code>{label && `Status: ${label}`}</code>
            </div>}
            <div className="column_container bottom_spaced">
                <div className="bottom_half_spaced">ABOUT YOU</div>
                <input placeholder="you can use markdown, URLs, hashtags, ..." className="monospace" type="text" value={about} onChange={event => setAbout(event.target.value)} />
            </div>
            <div className="column_container bottom_spaced">
                <div className="bottom_half_spaced">COLOR THEME</div>
                <select value={settings.theme} className="monospace" onChange={event => setSetting("theme", event)}>
                    <option value="auto">AUTO</option>
                    <option value="light">LIGHT</option>
                    <option value="dark">DARK</option>
                    <option value="classic">CLASSIC</option>
                    <option value="midnight">MIDNIGHT</option>
                </select>
            </div>
            <div className="column_container bottom_spaced">
                <div className="bottom_half_spaced">MULTI-COLUMN VIEW ON LANDING:</div>
                <select value={settings.columns} className="monospace" onChange={event => setSetting("columns", event)}>
                    <option value="on">ON</option>
                    <option value="off">OFF</option>
                </select>
            </div>
            <div className="column_container bottom_spaced">
                <div className="bottom_half_spaced">CONTROLLER PRINCIPALS (one per line)</div>
                <textarea className="monospace small_text" type="text" value={controllers} onChange={event => setControllers(event.target.value)} rows="4"></textarea>
            </div>
            <ButtonWithLoading classNameArg="active" onClick={submit} label="SAVE" />
            {api._user && <><hr />
            <div className="column_container top_spaced">
                <div className="bottom_half_spaced">PRINCIPAL</div>
                <input placeholder="Your principal" className="monospace small_text" type="text" value={principal} onChange={event => setPrincipal(event.target.value)} />
            </div>
            <div className="small_text vertically_spaced">⚠️ Please note that changing your principal will lead to the account loss if you do not control the new principal!</div>
            {<ButtonWithLoading classNameArg={principal != api._principalId ? "" : "inactive"} onClick={async () => {
                let response = await api.call("change_principal", principal);
                if ("Err" in response) {
                    alert(`Error: ${response.Err}`);
                    return;
                }
                localStorage.clear();
                location.reload();
            }} label="CHANGE PRINCIPAL" />}</>}
        </div>
    </>;
}

