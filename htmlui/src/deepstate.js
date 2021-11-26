// functions in deep state manipulate nested component state by using path expressions.

// getDeepStateProperty returns the provided deep state property or a default value
// For example: { "a": { "b": { "c": true } } }
// getDeepStateProperty("a") returns {b":{"c":true}}
// getDeepStateProperty("a.b") returns {"c":true}
// getDeepStateProperty("a.b.c") returns true

export function setDeepStateProperty(component, name, value) {
    let newState = { ...component.state };
    let st = newState;

    const parts = name.split(/\./);

    for (let i = 0; i < parts.length - 1; i++) {
        const part = parts[i];

        if (st[part] === undefined) {
            st[part] = {}
        } else {
            st[part] = { ...st[part] }
        }

        st = st[part]
    }

    const part = parts[parts.length - 1]
    st[part] = value;

    component.setState(newState);
}

// getDeepStateProperty returns the provided deep state property or a default value
// For example: { "a": { "b": { "c": true } } }
// getDeepStateProperty("a") returns {b":{"c":true}}
// getDeepStateProperty("a.b") returns {"c":true}
// getDeepStateProperty("a.b.c") returns true
export function getDeepStateProperty(component, name, defaultValue = "") {
    let st = component.state;
    const parts = name.split(/\./);

    for (let i = 0; i < parts.length; i++) {
        const part = parts[i];

        if (part in st) {
            st = st[part];
        } else {
            return defaultValue;
        }
    }

    return st;
}