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

export function handleChange(event) {
    this.setState({
        [event.target.name]: event.target.value,
    });
}

export function RequiredField(component, label, name, props={}) {
    return <Form.Group as={Col}>
        <Form.Label className="required">{label}</Form.Label>
        <Form.Control
            isInvalid={component.state[name] === ''}
            name={name}
            value={component.state[name]}
            onChange={component.handleChange} 
            {...props} />
        <Form.Control.Feedback type="invalid">{label} Is Required</Form.Control.Feedback>
    </Form.Group>
}

export function OptionalField(component, label, name, props={}) {
    return <Form.Group as={Col}>
        <Form.Label>{label}</Form.Label>
        <Form.Control
            name={name}
            value={component.state[name]}
            onChange={component.handleChange} 
            {...props} />
    </Form.Group>
}