import 'bootstrap/dist/css/bootstrap.min.css';
import React from 'react';
import Tab from 'react-bootstrap/Tab';
import Tabs from 'react-bootstrap/Tabs';

import ServerConfig from './ServerConfig';
import ServerLogs from './ServerLogs';
import ServerStatus from './ServerStatus';

export default function Repo(props) {
    const repoID = props.repoID;

    return (
        <div>
            <Tabs defaultActiveKey="config" transition={false}>
                <Tab eventKey="config" title="Configuration">
                    <div className="tab-body">
                        <ServerConfig repoID={repoID} />
                    </div>
                </Tab>
                <Tab eventKey="logs" title="Logs">
                    <div className="tab-body">
                        <ServerLogs repoID={repoID} />
                    </div>
                </Tab>
            </Tabs>
            <hr />
            <ServerStatus repoID={repoID} />
        </div>
    );
}
