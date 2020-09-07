import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { validateRequiredFields, handleChange, RequiredField } from './forms';

export class SetupToken extends Component {
    constructor(props) {
        super();

        this.state = {
            ...props.initial
        };
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["token"])
    }

    render() {
        return <>
            <Form.Row>
                {RequiredField(this, "Token", "token", { autoFocus: true, type: "password", placeholder: "paste connection token" })}
            </Form.Row>
        </>;
    }
}
