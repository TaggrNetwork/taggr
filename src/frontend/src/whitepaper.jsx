import * as React from "react";
import {HeadBar} from "./common";
import {Content} from "./content";
import template from '../../../WHITEPAPER.md';


export const Whitepaper = () => {
    let value = template.match(/\$([a-zA-Z_]+)/g).reduce((acc, e) => {
        const key = e.slice(1);
        let value = backendCache.config[key];
        // Remove decimals
        if (key == "total_supply")
                value = (value / Math.pow(10, backendCache.config.token_decimals)).toLocaleString();
        return acc.replace(e, value);
    }, template);
    return <>
        <HeadBar title="White Paper" shareLink="whitepaper" />
        <Content classNameArg="spaced" value={value} />
    </>;
}
