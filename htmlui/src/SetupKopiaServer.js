import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { handleChange, OptionalField, RequiredField, validateRequiredFields } from './forms';

export class SetupKopiaServer extends Component {
    constructor() {
        super();

        this.state = {};
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["url"])
    }

    render() {
        return <>
            <Form.Row>
                {RequiredField(this, "Server address", "url", { placeholder: "enter server URL (https://<host>:port)" })}
            </Form.Row>
            <Form.Row>
                {OptionalField(this, "Trusted server certificate finterprint (SHA256)", "serverCertFingerprint", { placeholder: "enter trusted server certificate fingerprint printed at server startup" })}
            </Form.Row>
        </>;
    }
}
