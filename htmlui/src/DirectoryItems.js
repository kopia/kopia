import React, { Component } from 'react';
import { Link } from "react-router-dom";
import MyTable from './Table';
import { objectLink, rfc3339TimestampForDisplay, sizeWithFailures } from './uiutil';

function objectName(name, typeID) {
    if (typeID === "d") {
        return name + "/";
    }

    return name
}

function sizeInfo(item) {
    if (item.size) {
        return item.size;
    }

    if (item.summ && item.summ.size) {
        return item.summ.size;
    }

    return 0;
}

function directoryLinkOrDownload(x) {
    if (x.obj.startsWith("k")) {
        return <Link to={objectLink(x.obj)}>{objectName(x.name, x.type)}</Link>;
    }

    return <a href={"/api/v1/objects/" + x.obj + "?fname=" + x.name}>{x.name}</a>;
}

export class DirectoryItems extends Component {
    render() {
        const columns = [{
            id: "name",
            Header: 'Name',
            width: "",
            accessor: x => directoryLinkOrDownload(x),
        }, {
            id: "mtime",
            accessor: "mtime",
            Header: "Last Mod",
            width: 200,
            Cell: x => rfc3339TimestampForDisplay(x.cell.value),
        }, {
            id: "size",
            accessor: x => sizeInfo(x),
            Header: "Size",
            width: 100,
            Cell: x => sizeWithFailures(x.cell.value, x.row.original.summ),
        }, {
            id: "files",
            accessor: "summ.files",
            Header: "Files",
            width: 100,
        }, {
            id: "dirs",
            accessor: "summ.dirs",
            Header: "Dirs",
            width: 100,
        }]

        return <MyTable data={this.props.items} columns={columns} />;
    }
}
