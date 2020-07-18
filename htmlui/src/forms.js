import React from 'react';
import Form from 'react-bootstrap/Form';
import Col from 'react-bootstrap/Col';

export function validateRequiredFields(component, fields) {
    let updateState = {};
    let failed = false;

    for (let i = 0; i < fields.length; i++) {
        const field = fields[i];

        if (!component.state[field]) {
            // explicitly set field to empty string, component triggers validation error UI.
            updateState[field] = '';
            failed = true;
        }
    }

    if (failed) {
        component.setState(updateState);
        return false;
    }

    return true;
}

export function handleChange(event, valueGetter=x=>x.value) {
    let newState = { ...this.state };
    let st = newState;

    const parts = event.target.name.split(/\./);

    for (let i = 0; i < parts.length - 1; i++) {
        const part = parts[i];

        if (st[part] === undefined) {
            st[part] = {}
        }

        st = st[part]
    }

    const part = parts[parts.length - 1]
    const v = valueGetter(event.target);
    st[part] = v;

    this.setState(newState);
}

export function stateProperty(component, name, defaultValue = "") {
    let st = component.state;
    const parts = name.split(/\./);

    for (let i = 0; i < parts.length; i++) {
        const part = parts[i];
        if (st === undefined) {
            return undefined;
        }

        if (part in st) {
            st = st[part];
        } else {
            return defaultValue;
        }
    }

    return st;
}

export function RequiredField(component, label, name, props = {}, helpText = null) {
    return <Form.Group as={Col}>
        <Form.Label className="required">{label}</Form.Label>
        <Form.Control
            isInvalid={stateProperty(component, name, null) === ''}
            name={name}
            value={stateProperty(component, name)}
            data-testid={'control-'+name}
            onChange={component.handleChange}
            {...props} />
        {helpText && <Form.Text className="text-muted">{helpText}</Form.Text>}
        <Form.Control.Feedback type="invalid">{label} Is Required</Form.Control.Feedback>
    </Form.Group>
}

export function OptionalField(component, label, name, props = {}, helpText = null, invalidFeedback = null) {
    return <Form.Group as={Col}>
        <Form.Label>{label}</Form.Label>
        <Form.Control
            name={name}
            value={stateProperty(component, name)}
            data-testid={'control-'+name}
            onChange={component.handleChange}
            {...props} />
        {helpText && <Form.Text className="text-muted">{helpText}</Form.Text>}
        {invalidFeedback && <Form.Control.Feedback type="invalid">{invalidFeedback}</Form.Control.Feedback>}
    </Form.Group>
}

function valueToNumber(t) {
    if (t.value === "") {
        return undefined;
    }

    const v = Number.parseInt(t.value);
    if (isNaN(v)) {
        return t.value + '';
    }

    return v;
}

function isInvalidNumber(v) {
    if (v === undefined || v === '') {
        return false
    }

    if (isNaN(Number.parseInt(v))) {
        return true;
    }

    return false;
}

export function OptionalNumberField(component, label, name, props = {}) {
    return <Form.Group as={Col}>
        <Form.Label>{label}</Form.Label>
        <Form.Control
            name={name}
            isInvalid={isInvalidNumber(stateProperty(component, name))}
            value={stateProperty(component, name)}
            onChange={e => component.handleChange(e, valueToNumber)}
            data-testid={'control-'+name}
            {...props} />
        <Form.Control.Feedback type="invalid">Must be a valid number or empty</Form.Control.Feedback>
    </Form.Group>
}

export function hasExactlyOneOf(component, names) {
    let count = 0;

    for (let i = 0; i < names.length; i++) {
        if (stateProperty(component, names[i])) {
            count++
        }
    }

    return count === 1;
}

function checkedToBool(t) {
    if (t.checked) {
        return true;
    }

    return false;
}

export function RequiredBoolean(component, label, name, defaultLabel) {
    return <Form.Group as={Col}>
        <Form.Check
            label={label}
            name={name}
            checked={stateProperty(component, name)}
            onChange={e => component.handleChange(e, checkedToBool)}
            data-testid={'control-'+name}
            type="checkbox" />
    </Form.Group>
}

function optionalBooleanValue(target) {
    if (target.value === "true") {
        return true;
    }
    if (target.value === "false") {
        return false;
    }

    return undefined;
}

export function OptionalBoolean(component, label, name, defaultLabel) {
    return <Form.Group as={Col}>
        <Form.Label>{label}</Form.Label>
        <Form.Control
            name={name}
            value={stateProperty(component, name)}
            onChange={e => component.handleChange(e, optionalBooleanValue)}
            as="select">
            <option value="">{defaultLabel}</option>
            <option value="true">yes</option>
            <option value="false">no</option>
        </Form.Control>
    </Form.Group>
}

function listToMultilineString(v) {
    if (v) {
        return v.join("\n");
    }

    return "";
}

function multilineStringToList(target) {
    const v = target.value;
    if (v === "") {
        return undefined;
    }

    return v.split(/\n/);
}

export function StringList(component, label, name, helpText) {
    return <Form.Group as={Col}>
        <Form.Label>{label}</Form.Label>
        <Form.Control
            name={name}
            value={listToMultilineString(stateProperty(component, name))}
            onChange={e => component.handleChange(e, multilineStringToList)}
            as="textarea"
            rows="5">
        </Form.Control>
        <Form.Text className="text-muted">{helpText}</Form.Text>
    </Form.Group>
}