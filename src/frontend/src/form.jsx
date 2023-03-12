import * as React from "react";
import { Content, CUT } from './content';
import { blobToUrl, ButtonWithLoading, getTokens, Loading, ReactionToggleButton } from './common';
import {Poll} from './poll';
import {Bars, Cycles, Paperclip} from "./icons";

const MAX_IMG_SIZE = 16777216;
const MAX_SUGGESTED_TAGS = 5;
export const MAX_POST_SIZE_BYTES = Math.ceil(1024 * 1024 * 1.9);

export const Form = ({postId = null, comment, realmArg = "", expanded, submitCallback, writingCallback = () => {}, blobs, content}) => {
    const draftKey = `draft_for_${comment? "comment" : "post"}_${postId}`;
    const [value, setValue] = React.useState("");
    const [realm, setRealm] = React.useState(realmArg);
    const [submitting, setSubmitting] = React.useState(false);
    const [lines, setLines] = React.useState(3);
    const [dragAndDropping, setDragAndDropping] = React.useState(false);
    const [tmpBlobs, setTmpBlobs] = React.useState([]);
    const [busy, setBusy] = React.useState(false);
    const [poll, setPoll] = React.useState(null);
    const [showTextField, setShowTextField] = React.useState(!!localStorage.getItem(draftKey) || expanded);
    const [suggestedTags, setSuggestedTags] = React.useState([]);
    const [suggestedUsers, setSuggestedUsers] = React.useState([]);
    const [choresTimer, setChoresTimer] = React.useState(null);
    const focus = React.useRef();
    const tags = window.backendCache.recent_tags;
    const users = Object.values(window.backendCache.users); 
    const { max_post_length, max_blob_size_bytes } = backendCache.config;

    const handleSubmit = async () => {
        if (ref.current?.clientHeight > window.innerHeight && !value.includes(CUT)) {
            alert("Your post does not fit on screen without scrolling.\n\nPlease add a cut line (three empty lines) after the introductory part.");
            return false;
        }
        if (value.length == 0 || value.length > max_post_length) {
            alert(`Post length should be larger than 0 and shorter than ${max_post_length} characters.`);
            return false;
        }
        setSubmitting(true);
        const blobArrays = Object.keys(tmpBlobs).reduce((acc, id) => {
            if (value.includes(`(/blob/${id})`)) {
                acc.push([id, [...tmpBlobs[id]]]);
            }
            return acc;
        }, []);
        const postSize = value.length + blobArrays.reduce((acc, [_, blob]) => acc + blob.length, 0);
        if(postSize > MAX_POST_SIZE_BYTES) {
            alert("Currently a single post cannot be larger than 2MB to be submitted.");
        } else if ((value.match(/!\[.*?\]\(\/blob\/.*?\)/g) || []).length != blobArrays.length) {
            alert("You're referencing pictures that are not attached anymore. Please re-upload.");
        } else {
            await submitCallback(value, blobArrays, poll, realm);
            setValue("");
            localStorage.removeItem(draftKey); 
        }
        setLines(3);
        setShowTextField(false);
        setSubmitting(false);
    };

    const dragOverHandler = ev => {
        setDragAndDropping(true);
        ev.preventDefault();
    };

    const dropHandler = async ev => {
        ev.preventDefault();
        setBusy(true);
        const files = (ev.dataTransfer || ev.target).files;
        let fileLinks = "";
        for (let i = 0; i < files.length; i++){
            let file = files[i];
            let content = await loadFile(file);
            let image = await loadImage(content);
            if (iOS() && image.height * image.width > MAX_IMG_SIZE) {
                alert("Image resolution should be under 16 megapixels.");
                setBusy(false);
                return;
            }
            let resized_content = content, low = 0, high = 100;
            if (content.byteLength > max_blob_size_bytes)
                while (true) {
                    const scale = (low + high) / 2;
                    resized_content = await resize(content, scale / 100);
                    const ratio = resized_content.byteLength / max_blob_size_bytes;
                    if (ratio < 1 && (0.92 < ratio || low > 99)) {
                        break;
                    } else if (ratio > 1) {
                        content = resized_content;
                        high = 100;
                    } else {
                        low = scale;
                    }
                }
            const size = Math.ceil(resized_content.byteLength / 1024);
            resized_content = new Uint8Array(resized_content);
            let key = await hash(resized_content);
            tmpBlobs[key] = resized_content;
            setTmpBlobs(tmpBlobs);
            image = await loadImage(resized_content);
            fileLinks += `![${image.width}x${image.height}, ${size}kb](/blob/${key})\n`;
            setDragAndDropping(false);
        }
        setValue(value + ((value == "" || value[value.length-1] == "\n") ? "" : "\n\n") + fileLinks);
        setBusy(false);
    };

    const onValueChange = value => {
        setValue(value);
        clearTimeout(choresTimer);
        const cursor = focus.current?.selectionStart-1;
        const suggestedTags = suggestTokens(cursor, value, tags, "#");
        setSuggestedTags(suggestedTags);
        const suggestedUsers = suggestTokens(cursor, value, users, "@");
        setSuggestedUsers(suggestedUsers);
        setChoresTimer(setTimeout(() => localStorage.setItem(draftKey, value), 1500));
        writingCallback(value);
    };

    const maybeInsertSuggestion = event => {
        if (event.charCode == 13) {
            const cursor = focus.current?.selectionStart-1;
            const suggestedTags = suggestTokens(cursor, value, tags, "#");
            const suggestedUsers = suggestTokens(cursor, value, users, "@");
            if (suggestedTags.length) {
                insertSuggestion(event, "#", suggestedTags[0]);
            } else if (suggestedUsers.length) {
                insertSuggestion(event, "@", suggestedUsers[0]);
            }
        }
    };

    const insertSuggestion = (event, trigger, token) => {
        event.preventDefault();
        const cursor = focus.current?.selectionStart;
        let i;
        for (i = cursor; value[i] != trigger; i--) {};
        setValue(value.slice(0, i+1) + token + value.slice(cursor) + " ");
        setSuggestedTags([]);
        setSuggestedUsers([]);
        setFocus();
    }

    const setFocus = () => {
        if(focus.current && !content) focus.current.focus(); 
    };

    const id = `form_${postId}_${lines}`;

    React.useEffect(() => setTmpBlobs(blobs || []), [blobs]);
    React.useEffect(() => setRealm(realmArg), [realmArg]);
    React.useEffect(() => {
        const effContent = content || localStorage.getItem(draftKey) || "";
        setValue(effContent);
        setLines(effContent.split('\n').length + 2);
    }, [content]);
    React.useEffect(() => setFocus(), [showTextField, focus]);
    const ref = React.useRef();

    const self = document.getElementById(id);
    if (self && self.clientHeight < self.scrollHeight) setLines(lines + 2);

    let trigger = "", completionList = [];
    if (suggestedTags.length) {
        trigger = "#";
        completionList = suggestedTags;
    } else {
        trigger = "@";
        completionList = suggestedUsers;
    }

    return <div onDrop={dropHandler} onDragOver={dragOverHandler} className="column_container">
        {!showTextField && <input type="text" className="bottom_half_spaced"
            placeholder="Reply here..."
            onFocus={() => setShowTextField(true)} /> }
        {showTextField && 
            <form className={`${submitting ? "inactive" : ""} column_container bottom_spaced`} autoFocus>
                <textarea id={id} ref={focus} rows={lines} disabled={submitting} value={value}
                    onKeyPress={maybeInsertSuggestion}
                    className={dragAndDropping ? "active_element" : null}
                    onChange={event => onValueChange(event.target.value)}></textarea>
                <div className="row_container spaced vcentered top_half_spaced">
                    {busy && <Loading spaced={false} />}
                    {!busy && completionList.length > 0 && <div className="monospace">
                        PICK: {completionList.map((token, i) => 
                        <button key={token} className={`right_spaced bottom_half_spaced ${i ? "" : "active"}`}
                            onClick={e =>  insertSuggestion(e, trigger, token)}>{`${trigger}${token}`}</button>)}
                    </div>}
                    <div className="row_container vcentered max_width_col flex_ended">
                        <div className="max_width_col"></div>
                        <Cycles /><code className="left_half_spaced">{`${costs(value, poll ? 1 : 0)}`}</code>
                        <label id="file_picker_label" htmlFor="file-picker" className="action left_spaced clickable"><Paperclip /></label>
                        <input id="file-picker" style={{display: "none"}} type="file" multiple accept="image/*" onChange={dropHandler} />
                        {postId == null && <ReactionToggleButton classNameArg="left_spaced" icon={<Bars />} pressed={!!poll}
                            onClick={() => setPoll(poll && confirm("Delete the poll?") 
                                ? null 
                                : (poll || { options: ["Option 1", "Option 2"], votes: {}, deadline: 24 }))} />}
                        {!comment && api._user.realms.length > 0 && <select value={realm || ""}
                            className="small_text left_spaced"
                            onChange={event => setRealm(event.target.value)}>
                            <option value="">{backendCache.config.name.toUpperCase()}</option>
                            {api._user.realms.map(name => <option key={name} value={name}>{name}</option>)}
                        </select>}
                        <ButtonWithLoading classNameArg="active left_spaced" label="SEND" onClick={handleSubmit} />
                    </div>
                </div>
            </form>}
        {poll && <div className="monospace column_container bottom_spaced">
            <h2>Poll</h2>
            VARIANTS (ONE PER LINE):
            <textarea rows={poll.options.length+2} className="monospace bottom_spaced" value={poll.options.join("\n")}
                onChange={e => setPoll({ ...poll, options: e.target.value.split("\n") })}></textarea>
            EXPIRATION:
            <select value={poll.deadline} onChange={e => setPoll({...poll, deadline: parseInt(e.target.value) })}>
                {[1,2,3,4,5,6,7].map(d => <option key={d} value={`${d * 24}`}>{`${d} DAY${d == 1 ? "" : "S"}`}</option>)}
            </select>
        </div>}
        {value && <article ref={ref} className={`bottom_spaced ${postId == null ? "prime" : ""}`}>
            <Content post={true} blobs={tmpBlobs} value={value} preview={true} primeMode={postId == null} />
            {poll && <Poll poll={poll} created={Number(new Date()) * 1000000} />}
        </article>}
    </div>;
}

const costs = (value, poll) => {
    const tags = getTokens("#$", value).length;
    const images = (value.match(/\(\/blob\/.+\)/g) || []).length;
    const paid_tags = Math.max(0, tags);
    const { post_cost, tag_cost, blob_cost, poll_cost } = backendCache.config;
    return Math.max(post_cost, paid_tags * tag_cost) + images * blob_cost + poll * poll_cost;
}

export const loadFile = file => {
    const reader = new FileReader();
    return new Promise((resolve, reject) => {
        reader.onerror = () => {
            reader.abort();
            reject(alert("Couldn't upload file!"));
        };
        reader.onload = () => resolve(reader.result);
        reader.readAsArrayBuffer(file);
    });
};

const loadImage = blob => {
    const image = new Image();
    return new Promise((resolve) => {
        image.onload = () => resolve(image);
        image.src = blobToUrl(blob);
    });
};

const canvasToBlob = canvas => new Promise(
    resolve => canvas.toBlob(
        blob => blob.arrayBuffer().then(resolve),'image/jpeg', 0.9
    )
);

const hash = async buffer => {
    const result = await crypto.subtle.digest('SHA-256', buffer);
    const hashArray = Array.from(new Uint8Array(result)).slice(0, 4);
    return hashArray
        .map(bytes => bytes.toString(16).padStart(2, '0'))
        .join('')
}

const resize = async (blob, scale) => {
    const image = await loadImage(blob);
    const canvas = downScaleImage(image, scale);
    return await canvasToBlob(canvas);
};

const suggestTokens = (cursor, value, tokens, trigger) => {
    let currentTag = ""
    let i;
    for (i = cursor; i >= 0 && value[i].match(/(\p{L}|-|\d)/gu); i--) {
        currentTag = value[i] + currentTag;
    }
    if (value[i] == trigger) {
        const result = tokens.filter(tag => tag.toLowerCase().startsWith(currentTag.toLowerCase()))
            .map(tag => currentTag + tag.slice(currentTag.length, tag.length));
        result.sort((a, b) => { if (a.length != b.length) { return a.length - b.length} else { return a < b } });
        return result.slice(0, MAX_SUGGESTED_TAGS);
    }
    return []
};

// scales the image by (float) scale < 1
// returns a canvas containing the scaled image.
function downScaleImage(img, scale) {
    var imgCV = document.createElement('canvas');
    imgCV.width = img.width;
    imgCV.height = img.height;
    var imgCtx = imgCV.getContext('2d');
    imgCtx.drawImage(img, 0, 0);
    return downScaleCanvas(imgCV, scale);
}

// scales the canvas by (float) scale < 1
// returns a new canvas containing the scaled image.
function downScaleCanvas(cv, scale) {
    if (!(scale < 1) || !(scale > 0)) throw ('scale must be a positive number <1 ');
    var sqScale = scale * scale; // square scale = area of source pixel within target
    var sw = cv.width; // source image width
    var sh = cv.height; // source image height
    var tw = Math.floor(sw * scale); // target image width
    var th = Math.floor(sh * scale); // target image height
    var sx = 0, sy = 0, sIndex = 0; // source x,y, index within source array
    var tx = 0, ty = 0, yIndex = 0, tIndex = 0; // target x,y, x,y index within target array
    var tX = 0, tY = 0; // rounded tx, ty
    var w = 0, nw = 0, wx = 0, nwx = 0, wy = 0, nwy = 0; // weight / next weight x / y
    // weight is weight of current source point within target.
    // next weight is weight of current source point within next target's point.
    var crossX = false; // does scaled px cross its current px right border ?
    var crossY = false; // does scaled px cross its current px bottom border ?
    var sBuffer = cv.getContext('2d').
        getImageData(0, 0, sw, sh).data; // source buffer 8 bit rgba
    var tBuffer = new Float32Array(3 * tw * th); // target buffer Float32 rgb
    var sR = 0, sG = 0,  sB = 0; // source's current point r,g,b
    /* untested !
    var sA = 0;  //source alpha  */    

    for (sy = 0; sy < sh; sy++) {
        ty = sy * scale; // y src position within target
        tY = 0 | ty;     // rounded : target pixel's y
        yIndex = 3 * tY * tw;  // line index within target array
        crossY = (tY != (0 | ty + scale)); 
        if (crossY) { // if pixel is crossing botton target pixel
            wy = (tY + 1 - ty); // weight of point within target pixel
            nwy = (ty + scale - tY - 1); // ... within y+1 target pixel
        }
        for (sx = 0; sx < sw; sx++, sIndex += 4) {
            tx = sx * scale; // x src position within target
            tX = 0 |  tx;    // rounded : target pixel's x
            tIndex = yIndex + tX * 3; // target pixel index within target array
            crossX = (tX != (0 | tx + scale));
            if (crossX) { // if pixel is crossing target pixel's right
                wx = (tX + 1 - tx); // weight of point within target pixel
                nwx = (tx + scale - tX - 1); // ... within x+1 target pixel
            }
            sR = sBuffer[sIndex    ];   // retrieving r,g,b for curr src px.
            sG = sBuffer[sIndex + 1];
            sB = sBuffer[sIndex + 2];

            if (!crossX && !crossY) { // pixel does not cross
                // just add components weighted by squared scale.
                tBuffer[tIndex    ] += sR * sqScale;
                tBuffer[tIndex + 1] += sG * sqScale;
                tBuffer[tIndex + 2] += sB * sqScale;
            } else if (crossX && !crossY) { // cross on X only
                w = wx * scale;
                // add weighted component for current px
                tBuffer[tIndex    ] += sR * w;
                tBuffer[tIndex + 1] += sG * w;
                tBuffer[tIndex + 2] += sB * w;
                // add weighted component for next (tX+1) px                
                nw = nwx * scale
                tBuffer[tIndex + 3] += sR * nw;
                tBuffer[tIndex + 4] += sG * nw;
                tBuffer[tIndex + 5] += sB * nw;
            } else if (crossY && !crossX) { // cross on Y only
                w = wy * scale;
                // add weighted component for current px
                tBuffer[tIndex    ] += sR * w;
                tBuffer[tIndex + 1] += sG * w;
                tBuffer[tIndex + 2] += sB * w;
                // add weighted component for next (tY+1) px                
                nw = nwy * scale
                tBuffer[tIndex + 3 * tw    ] += sR * nw;
                tBuffer[tIndex + 3 * tw + 1] += sG * nw;
                tBuffer[tIndex + 3 * tw + 2] += sB * nw;
            } else { // crosses both x and y : four target points involved
                // add weighted component for current px
                w = wx * wy;
                tBuffer[tIndex    ] += sR * w;
                tBuffer[tIndex + 1] += sG * w;
                tBuffer[tIndex + 2] += sB * w;
                // for tX + 1; tY px
                nw = nwx * wy;
                tBuffer[tIndex + 3] += sR * nw;
                tBuffer[tIndex + 4] += sG * nw;
                tBuffer[tIndex + 5] += sB * nw;
                // for tX ; tY + 1 px
                nw = wx * nwy;
                tBuffer[tIndex + 3 * tw    ] += sR * nw;
                tBuffer[tIndex + 3 * tw + 1] += sG * nw;
                tBuffer[tIndex + 3 * tw + 2] += sB * nw;
                // for tX + 1 ; tY +1 px
                nw = nwx * nwy;
                tBuffer[tIndex + 3 * tw + 3] += sR * nw;
                tBuffer[tIndex + 3 * tw + 4] += sG * nw;
                tBuffer[tIndex + 3 * tw + 5] += sB * nw;
            }
        } // end for sx 
    } // end for sy

    // create result canvas
    var resCV = document.createElement('canvas');
    resCV.width = tw;
    resCV.height = th;
    var resCtx = resCV.getContext('2d');
    var imgRes = resCtx.getImageData(0, 0, tw, th);
    var tByteBuffer = imgRes.data;
    // convert float32 array into a UInt8Clamped Array
    var pxIndex = 0; //  
    for (sIndex = 0, tIndex = 0; pxIndex < tw * th; sIndex += 3, tIndex += 4, pxIndex++) {
        tByteBuffer[tIndex] = Math.ceil(tBuffer[sIndex]);
        tByteBuffer[tIndex + 1] = Math.ceil(tBuffer[sIndex + 1]);
        tByteBuffer[tIndex + 2] = Math.ceil(tBuffer[sIndex + 2]);
        tByteBuffer[tIndex + 3] = 255;
    }
    // writing result to canvas.
    resCtx.putImageData(imgRes, 0, 0);
    return resCV;
}

const iOS = () => [ 'iPad Simulator', 'iPhone Simulator', 'iPod Simulator', 'iPad', 'iPhone', 'iPod' ].includes(navigator.platform);
