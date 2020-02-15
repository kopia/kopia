import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { validateRequiredFields, handleChange, RequiredField } from './forms';

export class SetupToken extends Component {
    constructor() {
        super();

        this.state = {};
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["token"])
    }

    render() {
        return <>
            <Form.Row>
                {RequiredField(this, "Token", "token", { type: "password", placeholder: "paste connection token generated using `kopia repo status -st`" })}
            </Form.Row>
        </>;
    }
}
