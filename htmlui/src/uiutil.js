import { faBan, faCheck, faChevronLeft, faCopy, faExclamationCircle, faExclamationTriangle, faFolderOpen, faTerminal, faWindowClose } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import React, { useState } from 'react';
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import FormControl from 'react-bootstrap/FormControl';
import InputGroup from 'react-bootstrap/InputGroup';
import Spinner from 'react-bootstrap/Spinner';
import { Link } from 'react-router-dom';

const base10UnitPrefixes = ["", "K", "M", "G", "T"];

function niceNumber(f) {
    return (Math.round(f * 10) / 10.0) + '';
}

function toDecimalUnitString(f, thousand, prefixes, suffix) {
    for (var i = 0; i < prefixes.length; i++) {
        if (f < 0.9 * thousand) {
            return niceNumber(f) + ' ' + prefixes[i] + suffix;
        }
        f /= thousand
    }

    return niceNumber(f) + ' ' + prefixes[prefixes.length - 1] + suffix;
}

export function sizeWithFailures(size, summ) {
    if (size === undefined) {
        return "";
    }

    if (!summ || !summ.errors || !summ.numFailed) {
        return <span>{sizeDisplayName(size)}</span>
    }

    let caption = "Encountered " + summ.numFailed + " errors:\n\n";
    let prefix = "- "
    if (summ.numFailed === 1) {
        caption = "Error: ";
        prefix = "";
    }

    caption += summ.errors.map(x => prefix + x.path + ": " + x.error).join("\n");

    return <span>
        {sizeDisplayName(size)}&nbsp;
        <FontAwesomeIcon color="red" icon={faExclamationTriangle} title={caption} />
    </span>;
}

export function sizeDisplayName(s) {
    if (s === undefined) {
        return "";
    }
    return toDecimalUnitString(s, 1000, base10UnitPrefixes, "B");
}

export function intervalDisplayName(v) {
    return "-";
}

export function timesOfDayDisplayName(v) {
    if (!v) {
        return "(none)";
    }
    return v.length + " times";
}

export function parseQuery(queryString) {
    var query = {};
    var pairs = (queryString[0] === '?' ? queryString.substr(1) : queryString).split('&');
    for (var i = 0; i < pairs.length; i++) {
        var pair = pairs[i].split('=');
        query[decodeURIComponent(pair[0])] = decodeURIComponent(pair[1] || '');
    }
    return query;
}

export function rfc3339TimestampForDisplay(n) {
    if (!n) {
        return "";
    }

    let t = new Date(n);
    return t.toLocaleString();
}

export function objectLink(n) {
    if (n.startsWith("k")) {
        return "/snapshots/dir/" + n;
    }
    return "/api/v1/objects/" + n;
}

export function ownerName(s) {
    return s.userName + "@" + s.host;
}

export function compare(a, b) {
    return (a < b ? -1 : (a > b ? 1 : 0));
}

export function redirectIfNotConnected(e) {
    if (e && e.response && e.response.data && e.response.data.code === "NOT_CONNECTED") {
        window.location.replace("/repo");
        return;
    }
}

export function formatMilliseconds(ms) {
    return Math.round(ms / 100.0) / 10.0 + "s"
}

export function formatDuration(from, to) {
    if (!from) {
        return "";
    }

    if (!to) {
        const ms = new Date().valueOf() - new Date(from).valueOf();
        if (ms < 0) {
            return ""
        }

        return formatMilliseconds(ms)
    }

    return formatMilliseconds(new Date(to).valueOf() - new Date(from).valueOf());
}

export function taskStatusSymbol(task) {
    const st = task.status;
    const dur = formatDuration(task.startTime, task.endTime);

    switch (st) {
        case "RUNNING":
            return <>
                <Spinner animation="border" variant="primary" size="sm" /> Running for {dur}
                &nbsp;
                <FontAwesomeIcon size="sm" color="red" icon={faWindowClose} title="Cancel task" onClick={() => cancelTask(task.id)} />
            </>;

        case "SUCCESS":
            return <p><FontAwesomeIcon icon={faCheck} color="green" /> Finished in {dur}</p>;

        case "FAILED":
            return <p><FontAwesomeIcon icon={faExclamationCircle} color="red" /> Failed after {dur}</p>;

        case "CANCELED":
            return <p><FontAwesomeIcon icon={faBan} /> Canceled after {dur}</p>;

        default:
            return st;
    }
}

export function cancelTask(tid) {
    axios.post('/api/v1/tasks/' + tid + '/cancel', {}).then(result => {
    }).catch(error => {
    });
}

export function GoBackButton(props) {
    return <Button size="sm" variant="outline-secondary" {...props}><FontAwesomeIcon icon={faChevronLeft} /> Return </Button>;
}

export function PolicyTypeName(s) {
    if (!s.host && !s.userName) {
        return "Global Policy"
    }

    if (!s.userName) {
        return "Host: " + s.host;
    }

    if (!s.path) {
        return "User: " + s.userName + "@" + s.host;
    }

    return "Directory: " + s.userName + "@" + s.host + ":" + s.path;
}

export function policyEditorURL(s) {
    return '/policies/edit?' + sourceQueryStringParams(s);
}

export function PolicyEditorLink(s) {
    return <Link to={policyEditorURL(s)}>{PolicyTypeName(s)}</Link>;
}

export function sourceQueryStringParams(src) {
    return 'userName=' + encodeURIComponent(src.userName) + '&host=' + encodeURIComponent(src.host) + '&path=' + encodeURIComponent(src.path);
}

export function isAbsolutePath(p) {
    // Unix-style path.
    if (p.startsWith("/")) {
        return true;
    }

    // Windows-style X:\... path.
    if (p.length >= 3 && p.substring(1, 3) === ":\\") {
        const letter = p.substring(0, 1).toUpperCase();

        return letter >= "A" && letter <= "Z";
    }

    // Windows UNC path.
    if (p.startsWith("\\\\")) {
        return true;
    }

    return false;
}

export function errorAlert(err, prefix) {
    if (!prefix) {
        prefix = "Error"
    }

    prefix += ": ";

    if (err.response && err.response.data && err.response.data.error) {
        alert(prefix + err.response.data.error);
    } else if (err instanceof Error) {
        alert(err);
    } else {
        alert(prefix + JSON.stringify(err));
    }
}

export function DirectorySelector(props) {
    let { onDirectorySelected, ...inputProps } = props;

    if (!window.kopiaUI) {
        return <Form.Control size="sm" {...inputProps} />
    }

    return <InputGroup>
        <FormControl size="sm" {...inputProps} />
        <Button size="sm" onClick={() => window.kopiaUI.selectDirectory(onDirectorySelected)}>
            <FontAwesomeIcon icon={faFolderOpen} />
        </Button>
    </InputGroup>;
}

export function CLIEquivalent(props) {
    let [visible, setVisible] = useState(false);
    let [cliInfo, setCLIInfo] = useState({});

    if (visible && !cliInfo.executable) {
        axios.get('/api/v1/cli').then(result => {
            setCLIInfo(result.data);
        }).catch(error => { });
    }

    const ref = React.createRef()

    function copyToClibopard() {
        const el = ref.current;
        if (!el) {
            return
        }

        el.select();
        el.setSelectionRange(0, 99999);

        document.execCommand("copy");
    }


    return <>
        <InputGroup size="sm" >
            <Button size="sm" title="Click to show CLI equivalent" variant="warning" onClick={() => setVisible(!visible)}><FontAwesomeIcon size="sm" icon={faTerminal} /></Button>
            {visible && <Button class="sm" variant="outline-dark" title="Copy to clipboard" onClick={copyToClibopard} ><FontAwesomeIcon size="sm" icon={faCopy} /></Button>}
            {visible && <FormControl size="sm" ref={ref} className="cli-equivalent" value={`${cliInfo.executable} ${props.command}`} />}
        </InputGroup>
    </>;
}

