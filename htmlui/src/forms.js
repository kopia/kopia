import React from 'react';
import Form from 'react-bootstrap/Form';
import Col from 'react-bootstrap/Col';
import FormGroup from 'react-bootstrap/FormGroup';

import { getDeepStateProperty, setDeepStateProperty } from './deepstate.js';

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

export function handleChange(event, valueGetter = x => x.value) {
    setDeepStateProperty(this, event.target.name, valueGetter(event.target));
}

export function stateProperty(component, name, defaultValue = "") {
    return getDeepStateProperty(component, name);
}

export function RequiredField(component, label, name, props = {}, helpText = null) {
    return <Form.Group as={Col}>
        <Form.Label className="required">{label}</Form.Label>
        <Form.Control
            size="sm"
            isInvalid={stateProperty(component, name, null) === ''}
            name={name}
            value={stateProperty(component, name)}
            data-testid={'control-' + name}
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
            size="sm"
            name={name}
            value={stateProperty(component, name)}
            data-testid={'control-' + name}
            onChange={component.handleChange}
            {...props} />
        {helpText && <Form.Text className="text-muted">{helpText}</Form.Text>}
        {invalidFeedback && <Form.Control.Feedback type="invalid">{invalidFeedback}</Form.Control.Feedback>}
    </Form.Group>
}

export function valueToNumber(t) {
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
        {label && <Form.Label>{label}</Form.Label>}
        <Form.Control
            size="sm"
            name={name}
            isInvalid={isInvalidNumber(stateProperty(component, name))}
            value={stateProperty(component, name)}
            onChange={e => component.handleChange(e, valueToNumber)}
            data-testid={'control-' + name}
            {...props} />
        <Form.Control.Feedback type="invalid">Must be a valid number or empty</Form.Control.Feedback>
    </Form.Group>
}


export function RequiredNumberField(component, label, name, props = {}) {
    return <Form.Group as={Col}>
        <Form.Label>{label}</Form.Label>
        <Form.Control
            size="sm"
            name={name}
            isInvalid={stateProperty(component, name, null) === '' || isInvalidNumber(stateProperty(component, name))}
            value={stateProperty(component, name)}
            onChange={e => component.handleChange(e, valueToNumber)}
            data-testid={'control-' + name}
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

export function RequiredBoolean(component, label, name, helpText) {
    return <Form.Group as={Col}>
        <Form.Check
            label={label}
            name={name}
            className="required"
            checked={stateProperty(component, name)}
            onChange={e => component.handleChange(e, checkedToBool)}
            data-testid={'control-' + name}
            type="checkbox" />
        {helpText && <Form.Text className="text-muted">{helpText}</Form.Text>}
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
        {label && <Form.Label>{label}</Form.Label>}
        <Form.Control
            size="sm"
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

export function StringList(component, name) {
    return <Form.Group as={Col}>
        <Form.Control
            size="sm"
            name={name}
            value={listToMultilineString(stateProperty(component, name))}
            onChange={e => component.handleChange(e, multilineStringToList)}
            as="textarea"
            rows="5">
        </Form.Control>
    </Form.Group>
}

export function TimesOfDayList(component, name) {
    function parseTimeOfDay(v) {
        var re = /(\d+):(\d+)/;

        const match = re.exec(v);
        if (match) {
            const h = parseInt(match[1]);
            const m = parseInt(match[2]);
            let valid = (h < 24 && m < 60);

            if (m < 10 && match[2].length === 1) {
                valid = false;
            }

            if (valid) {
                return {hour: h, min: m}
            }
        }

        return v;
    }

    function toMultilineString(v) {
        if (v) {
            let tmp = [];

            for (const tod of v) {
                if (tod.hour) {
                    tmp.push(tod.hour + ":" + (tod.min < 10 ? "0": "") + tod.min);
                } else {
                    tmp.push(tod);
                }
            }

            return tmp.join("\n");
        }
    
        return "";
    }
    
    function fromMultilineString(target) {
        const v = target.value;
        if (v === "") {
            return undefined;
        }

        let result = [];
    
        for (const line of v.split(/\n/)) {
            result.push(parseTimeOfDay(line));
        };

        return result;
    }
    
    return <FormGroup>
        <Form.Control
            size="sm"
            name={name}
            value={toMultilineString(stateProperty(component, name))}
            onChange={e => component.handleChange(e, fromMultilineString)}
            as="textarea"
            rows="5">
        </Form.Control>
        <Form.Control.Feedback type="invalid">Invalid Times of Day</Form.Control.Feedback>
   </FormGroup>;
}

export function LogDetailSelector(component, name) {
    return <Form.Control as="select" size="sm"
    name={name}
    onChange={e => component.handleChange(e, valueToNumber)}
    value={stateProperty(component, name)}>
    <option value="">(inherit from parent)</option>
    <option value="0">0 - no output</option>
    <option value="1">1 - minimal details</option>
    <option value="2">2</option>
    <option value="3">3</option>
    <option value="4">4</option>
    <option value="5">5 - normal details</option>
    <option value="6">6</option>
    <option value="7">7</option>
    <option value="8">8</option>
    <option value="9">9</option>
    <option value="10">10 - maximum details</option>
</Form.Control>
}