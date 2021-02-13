
import { faStopCircle } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import React, { Component } from 'react';
import Alert from 'react-bootstrap/Alert';
import Badge from 'react-bootstrap/Badge';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Spinner from 'react-bootstrap/Spinner';
import { sizeDisplayName } from './uiutil';
import { TaskLogs } from './TaskLogs';
import { cancelTask, formatDuration, GoBackButton, redirectIfNotConnected } from './uiutil';

export class TaskDetails extends Component {
    constructor() {
        super();
        this.state = {
            items: [],
            isLoading: true,
            error: null,
            showLog: false,
        };

        this.fetchTask = this.fetchTask.bind(this);
        
        // poll frequently, we will stop as soon as the task ends.
        this.interval = window.setInterval(() => this.fetchTask(this.props), 500);
    }

    componentDidMount() {
        this.setState({
            isLoading: true,
        });

        this.fetchTask(this.props);
    }

    componentWillUnmount() {
        if (this.interval) {
            window.clearInterval(this.interval);
        }
    }

    fetchTask(props) {
        let tid = props.match.params.tid;

        axios.get('/api/v1/tasks/' + tid).then(result => {
            this.setState({
                task: result.data,
                isLoading: false,
            });

            if (result.data.endTime) {
                window.clearInterval(this.interval);
                this.interval = null;
            }
        }).catch(error => {
            redirectIfNotConnected(error);
            this.setState({
                error,
                isLoading: false
            });
        });
    }

    componentWillReceiveProps(props) {
        this.fetchTask(props);
    }

    summaryControl(task) {
        const dur = formatDuration(task.startTime, task.endTime)

        switch (task.status) {

        case "SUCCESS":
            return <Alert variant="success">Task succeeded after {dur}.</Alert>;

        case "FAILED":
            return <Alert variant="danger"><b>Error:</b> {task.errorMessage}.</Alert>;

        case "CANCELING":
            return <Alert variant="warning">Cancelation requested...</Alert>;

        case "CANCELED":
            return <Alert variant="warning">Task canceled.</Alert>;

        default:
            return <Alert variant="primary"> <Spinner animation="border" variant="primary" size="sm" /> Task in progress ({dur}).</Alert>;
        }
    }

    counterBadge(label, c) {
        if (!c.value) {
            return "";
        }

        let formatted = c.value.toLocaleString();
        if (c.units === "bytes") {
            formatted = sizeDisplayName(c.value);
        }

        let variant = "secondary";
        switch (c.level) {
            case "warning":
                variant = "warning";
                break;
            case "error":
                variant = "danger";
                break;
            case "notice":
                variant = "info";
                break;
            default:
                variant = "secondary";
                break;
        }

        return <Badge key={label} className="counter-badge" variant={variant}>{label}: {formatted}</Badge>
    }

    counterLevelToSortOrder(l) {
        switch (l) {
            case "error":
                return 30
            case "notice":
                return 10;
            case "warning":
                return 5;
            default:
                return 0;
        }
    }

    sortedBadges(counters) {
        let keys = Object.keys(counters);

        // sort keys by their level and the name alphabetically.
        keys.sort((a, b) => {
            if (counters[a].level !== counters[b].level) {
                return this.counterLevelToSortOrder(counters[b].level) - this.counterLevelToSortOrder(counters[a].level);
            }

            if (a < b) {
                return -1;
            }

            if (a > b) {
                return 1;
            }

            return 0;
        });

        return keys.map(c => (counters[c].value > 0) && this.counterBadge(c, counters[c]));
    }

    render() {
        const { task, isLoading, error } = this.state;
        if (error) {
            return <p>{error.message}</p>;
        }

        if (isLoading) {
            return <p>Loading ...</p>;
        }

        return <Form>
            <Form.Row>
                <Form.Group>
                    <GoBackButton onClick={this.props.history.goBack} />
                    {task.status === "RUNNING" && <>
                        &nbsp;<Button size="sm" variant="danger" onClick={() => cancelTask(task.id)} ><FontAwesomeIcon icon={faStopCircle} /> Stop </Button>
                    </>}
                </Form.Group>
            </Form.Row>
            <Form.Row>
                <Col xs={3} >
                    <Form.Group>
                        <Form.Control type="text" readOnly={true} value={task.kind} />
                    </Form.Group>
                </Col>
                <Col xs={9} >
                    <Form.Group>
                        <Form.Control type="text" readOnly={true} value={task.description} />
                    </Form.Group>
                </Col>
            </Form.Row>
            <Form.Row>
                <Col xs={9}>
                    {this.summaryControl(task)}
                </Col>
                <Col xs={3}>
                    <Form.Group>
                        <Form.Control type="text" readOnly={true} value={"Started: " + new Date(task.startTime).toLocaleString()} />
                    </Form.Group>
                </Col>
            </Form.Row>
            {task.counters && <Form.Row>
                <Col>
                {this.sortedBadges(task.counters)}
                </Col>
            </Form.Row>}
            <hr/>
            <Form.Row>
                <Col>
                {this.state.showLog ? <TaskLogs taskID={this.props.match.params.tid} /> : <Button onClick={() => this.setState({showLog:true})}>Show Log</Button>}
                </Col>
            </Form.Row>
        </Form>
            ;
    }
}
