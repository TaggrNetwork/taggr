import * as React from "react";
import { Form } from './form';
import { getPatch, loadPostBlobs, loadPost, isRoot } from './common';

export const PostSubmissionForm = ({id, repost}) => {
    const [post, setPost] = React.useState({});
    const [blobs, setBlobs] = React.useState({});

    const load = async () => {
        if (!id) return;
        const post = await loadPost(api, id);
        setPost(post);
        setBlobs(await loadPostBlobs(post.files));
    };

    React.useEffect(() => { load(); }, []);

    const callback = async (text, blobs, poll, realm) => {
        let postId;
        const optionalRealm = realm ? [realm] : [];
        if (post.id) {
            const patch = getPatch(text, post.body);
            let response = await api.edit_post(id, text, blobs, patch, optionalRealm);
            if ("Err" in response) {
                alert(`Error: ${response.Err}`);
                return
            }
            postId = post.id;
        } else {
            const result = await api.add_post(text, blobs, [], optionalRealm, encodePoll(poll));
            if ("Err" in result) {
                return alert(`Error: ${result.Err}`);
            }
            postId = result.Ok;
        }
        location.href = `#/post/${postId}`;
    };

    const content = repost ? `\n\n[repost:${repost}](#/post/${repost})` : "";

    return <div className="spaced">
        <ul>
            <li>Use <a target="_blank" href="https://commonmark.org/help/">Markdown</a> for formatting.</li>
            <li>Use <code>#hashtags</code> if you want your post to appear in the corresponding tag-feed.</li>
            <li>Use three empty lines to create a cut line for long posts.</li>
            <li>You can drag and drop images into the text area.</li>
            <li>Group images together and separate from the rest by new lines to create galleries.</li>
        </ul>
        <Form submitCallback={callback} postId={id} content={post.body || content} blobs={blobs} expanded={true}
            comment={!isRoot(post)} realmArg={post.realm || api._user.current_realm}/>
    </div>;
}

const encodePoll = poll => {
    if (poll) {
        poll.votes = {};
        return [(new TextEncoder()).encode(JSON.stringify({ Poll: poll }))];
    }
    return []
}
