import axios from 'axios';
import React, { Component } from 'react';
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { Link } from "react-router-dom";
import { handleChange, RequiredBoolean, RequiredField, RequiredNumberField, validateRequiredFields } from './forms';
import { errorAlert, GoBackButton } from './uiutil';

export class BeginRestore extends Component {
    constructor(props) {
        super();

        this.state = {
            incremental: true,
            continueOnErrors: false,
            restoreOwnership: true,
            restorePermissions: true,
            restoreModTimes: true,
            uncompressedZip: true,
            overwriteFiles: false,
            overwriteDirectories: false,
            overwriteSymlinks: false,
            ignorePermissionErrors: true,
            writeFilesAtomically: false,
            restoreDirEntryAtDepth: 1000,
            minSizeForPlaceholder: 0,
            restoreTask: "",
        };

        this.handleChange = handleChange.bind(this);
        this.start = this.start.bind(this);
    }

    start(e) {
        e.preventDefault();

        if (!validateRequiredFields(this, ["destination"])) {
            return;
        }

        const dst = (this.state.destination + "");

        let req = {
            root: this.props.match.params.oid,
            options: {
                incremental: this.state.incremental,
                ignoreErrors: this.state.continueOnErrors,
                restoreDirEntryAtDepth: this.state.restoreDirEntryAtDepth,
                minSizeForPlaceholder: this.state.minSizeForPlaceholder,
            },
        }

        if (dst.endsWith(".zip")) {
            req.zipFile = dst;
            req.uncompressedZip = this.state.uncompressedZip;
        } else if (dst.endsWith(".tar")) {
            req.tarFile = dst;
        } else {
            req.fsOutput = {
                targetPath: dst,
                skipOwners: !this.state.restoreOwnership,
                skipPermissions: !this.state.restorePermissions,
                skipTimes: !this.state.restoreModTimes,

                ignorePermissionErrors: this.state.ignorePermissionErrors,
                overwriteFiles: this.state.overwriteFiles,
                overwriteDirectories: this.state.overwriteDirectories,
                overwriteSymlinks: this.state.overwriteSymlinks,
                writeFilesAtomically: this.state.writeFilesAtomically
            }
        }

        axios.post('/api/v1/restore', req).then(result => {
            this.setState({
                restoreTask: result.data.id,
            })
            this.props.history.replace("/tasks/" + result.data.id);
        }).catch(error => {
            errorAlert(error);
        });
    }

    render() {
        if (this.state.restoreTask) {
            return <p>
                <GoBackButton onClick={this.props.history.goBack} />
                <Link replace="true" to={"/tasks/" + this.state.restoreTask}>Go To Restore Task</Link>.
            </p>;
        }

        return <div className="padded-top">
            <GoBackButton onClick={this.props.history.goBack} />&nbsp;<span className="page-title">Restore</span>
            <hr/>
            <Form onSubmit={this.start}>
                <Row>
                    {RequiredField(this, "Destination", "destination", {
                        autoFocus: true,
                        placeholder: "enter destination path",
                    },
                        "You can also restore to a .zip or .tar file by providing the appropriate extension.")}
                </Row>
                <Row>
                    {RequiredBoolean(this, "Skip previously restored files and symlinks", "incremental")}
                </Row>
                <Row>
                    {RequiredBoolean(this, "Continue on Errors", "continueOnErrors", "When a restore error occurs, attempt to continue instead of failing fast.")}
                </Row>
                <Row>
                    {RequiredBoolean(this, "Restore File Ownership", "restoreOwnership")}
                </Row>
                <Row>
                    {RequiredBoolean(this, "Restore File Permissions", "restorePermissions")}
                </Row>
                <Row>
                    {RequiredBoolean(this, "Restore File Modification Time", "restoreModTimes")}
                </Row>
                <Row>
                    {RequiredBoolean(this, "Overwrite Files", "overwriteFiles")}
                </Row>
                <Row>
                    {RequiredBoolean(this, "Overwrite Directories", "overwriteDirectories")}
                </Row>
                <Row>
                    {RequiredBoolean(this, "Overwrite Symbolic Links", "overwriteSymlinks")}
                </Row>
                <Row>
                    {RequiredBoolean(this, "Write files atomically", "writeFilesAtomically")}
                </Row>
                <Row>
                    <Col><hr/></Col>
                </Row>
                <Row>
                    {RequiredNumberField(this, "Shallow Restore At Depth", "restoreDirEntryAtDepth")}
                    {RequiredNumberField(this, "Minimal File Size For Shallow Restore", "minSizeForPlaceholder")}
                </Row>
                <Row>
                    <Col><hr/></Col>
                </Row>
                <Row>
                    {RequiredBoolean(this, "Disable ZIP compression", "uncompressedZip", "Do not compress when restoring to a ZIP file (faster).")}
                </Row>
                <Row>
                    <Col><hr/></Col>
                </Row>
                <Row>
                    <Col>
                        <Button variant="primary" type="submit" data-testid="submit-button">Begin Restore</Button>
                    </Col>
                </Row>
            </Form>
        </div>;
    }
}
