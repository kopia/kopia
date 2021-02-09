
import axios from 'axios';
import React, { Component } from 'react';
import Table from 'react-bootstrap/Table';
import { handleChange } from './forms';
import { redirectIfNotConnected } from './uiutil';

export class TaskLogs extends Component {
    constructor() {
        super();
        this.state = {
            items: [],
            isLoading: false,
            error: null,
        };

        this.handleChange = handleChange.bind(this);
        this.fetchLog = this.fetchLog.bind(this);
        this.interval = window.setInterval(this.fetchLog, 3000);
        this.messagesEndRef = React.createRef();
        this.scrollToBottom = this.scrollToBottom.bind(this);
    }

    componentDidMount() {
        this.setState({
            isLoading: true,
        });

        this.fetchLog();
        this.scrollToBottom();
    }

    componentWillUnmount() {
        window.clearInterval(this.interval);
    }

    lastMessage(l) {
        if (!l || !l.length) {
            return "";
        }

        return l[l.length - 1].msg;
    }

    fetchLog() {
        axios.get('/api/v1/tasks/' + this.props.taskID + "/logs").then(result => {
            let oldLogs = this.state.logs;
            this.setState({
                logs: result.data.logs,
                isLoading: false,
            });
            
            if (this.lastMessage(oldLogs) !== this.lastMessage(result.data.logs)) {
                this.scrollToBottom();
            } 
        }).catch(error => {
            redirectIfNotConnected(error);
            this.setState({
                error,
                isLoading: false
            });
        });
    }

    fullLogTime(x) {
        return new Date(x * 1000).toLocaleString();
    }

    formatLogTime(x) {
        const d = new Date(x * 1000);
        let result = "";

        result += ("0" + d.getHours()).substr(-2);
        result += ":";
        result += ("0" + d.getMinutes()).substr(-2);
        result += ":";
        result += ("0" + d.getSeconds()).substr(-2);
        result += ".";
        result += ("00" + d.getMilliseconds()).substr(-3)

        return result;
    }

    scrollToBottom() {
        const c = this.messagesEndRef.current;
        if (c) {
            c.scrollIntoView({ behavior: 'smooth' })
        }
    }

    render() {
        const { logs, isLoading, error } = this.state;
        if (error) {
            return <p>{error.message}</p>;
        }
        if (isLoading) {
            return <p>Loading ...</p>;
        }

        if (logs) {
            return <div class="logs-table"><Table size="sm">
                <tbody>
                    {logs.map(v => <tr className={'loglevel-' + v.level}>
                        <td title={this.fullLogTime(v.ts)}>{this.formatLogTime(v.ts)}</td>
                        <td class="elide">{v.msg}</td></tr>)}
                </tbody>
            </Table>
            <div ref={this.messagesEndRef} />
            </div>;
        }

        return null;
    }
}
