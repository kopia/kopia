import React, { Component } from 'react';
import Row from 'react-bootstrap/Row';
import { handleChange, OptionalField, RequiredField, validateRequiredFields } from './forms';

export class SetupKopiaServer extends Component {
    constructor(props) {
        super();

        this.state = {
            ...props.initial
        };
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["url"])
    }

    render() {
        return <>
            <Row>
                {RequiredField(this, "Server address", "url", { autoFocus: true, placeholder: "enter server URL (https://<host>:port)" })}
            </Row>
            <Row>
                {OptionalField(this, "Trusted server certificate fingerprint (SHA256)", "serverCertFingerprint", { placeholder: "enter trusted server certificate fingerprint printed at server startup" })}
            </Row>
        </>;
    }
}
