import template from '../assets/style.css';

const themes = {
    "classic": {
        "text": "#cbcbbc",
        "focus": "#c8c6a1",
        "background_light": "#23383f",
        "background_dark": "#0f242b",
        "background": "#1c3239",
        "code": "white",
        "clickable": "#30cec5",
        "clicked": "#009e95",
        "accent": "#FFc700"
    },
    "light": {
        "text": "#23383F",
        "focus": "darkslategray",
        "background_light": "#cbcbbc",
        "background_dark": "#a9a99a",
        "background": "#c3c3b4",
        "code": "black",
        "clickable": "teal",
        "clicked": "#006060",
        "accent": "orangered"
    },
    "dark": {
        "text": "#cccccc",
        "focus": "#c8c6a1",
        "background_light": "#24242a",
        "background_dark": "#101014",
        "background": "#1e1e23",
        "code": "white",
        "clickable": "#00b0b0",
        "clicked": "teal",
        "accent": "gold"
    },
    "midnight": {
        "text": "#cccccc",
        "focus": "#c8c6a1",
        "background_light": "#192636",
        "background_dark": "#091523",
        "background": "#111d2b",
        "code": "white",
        "clickable": "#00b0b0",
        "clicked": "teal",
        "accent": "#FFd700"
    }
};

export const applyTheme = () => {
    let theme = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? "dark" : "light";
    if (api._user) {
        const preferredTheme = api._user.settings.theme;
        if (preferredTheme && preferredTheme != "auto") theme = preferredTheme;
    }
    const palette = themes[theme || "classic"];
    const styleNode = document.getElementById("style");
    styleNode.innerText = Object.keys(palette).reduce((acc, color) =>
        acc.replaceAll(`$${color}`, palette[color]), template);
}
