import React from 'react';
import Pagination from 'react-bootstrap/Pagination';
import Table from 'react-bootstrap/Table';
import { usePagination, useSortBy, useTable } from 'react-table';

function paginationItems(count, active, gotoPage) {
  let items = [];

  function pageWithNumber(number) {
    return <Pagination.Item key={number} active={number === active} onClick={() => gotoPage(number-1)}>
      {number}
    </Pagination.Item>;
  }

  function dotDotDot() {
    return <Pagination.Ellipsis />;
  }

  let minPageNumber = active - 10;
  if (minPageNumber < 1) {
    minPageNumber = 1;
  }

  let maxPageNumber = active + 9;
  if (minPageNumber + 19 >= maxPageNumber) {
    maxPageNumber = minPageNumber + 19;
  }
  if (maxPageNumber > count) {
    maxPageNumber = count;
  }

  if (minPageNumber > 1) {
    items.push(dotDotDot());
  }

  for (let number = minPageNumber; number <= maxPageNumber; number++) {
    items.push(pageWithNumber(number));
  }

  if (maxPageNumber < count) {
    items.push(dotDotDot());
  }

  return items;
}

export default function MyTable({ columns, data }) {
  // Use the state and functions returned from useTable to build your UI
  const {
    getTableProps,
    getTableBodyProps,
    headerGroups,
    page,
    prepareRow,
    canPreviousPage,
    canNextPage,
    pageOptions,
    pageCount,
    gotoPage,
    nextPage,
    previousPage,
    state: { pageIndex },
  } = useTable({
    columns,
    data,
    initialState: { pageSize: 15 },
  },
    useSortBy,
    usePagination,
  )

  const paginationUI = pageOptions.length > 1 && 
  <>
    <Pagination size="sm" variant="dark">
      <Pagination.First onClick={() => gotoPage(0)} disabled={!canPreviousPage} />
      <Pagination.Prev onClick={() => previousPage()} disabled={!canPreviousPage} />
      <Pagination.Next onClick={() => nextPage()} disabled={!canNextPage} />
      <Pagination.Last onClick={() => gotoPage(pageCount - 1)} disabled={!canNextPage} />
    </Pagination>

    &nbsp;
    <Pagination size="sm" variant="dark">
      {paginationItems(pageOptions.length, pageIndex+1, gotoPage)}
    </Pagination>
    </>;

  return (
    <>
      <Table size="sm" striped bordered hover {...getTableProps()}>
        <thead className="thead-dark">
          {headerGroups.map(headerGroup => (
            <tr {...headerGroup.getHeaderGroupProps()}>
              {headerGroup.headers.map(column => (
                <th {...column.getHeaderProps({...column.getSortByToggleProps(), style: {
                  width: column.width,
                }})}>{column.render('Header')}
                 {column.isSorted ? (column.isSortedDesc ? '🔽' : '🔼') : ''}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody {...getTableBodyProps()}>
          {page.map(
            (row, i) => {
              prepareRow(row);
              return (
                <tr {...row.getRowProps()}>
                  {row.cells.map(cell => {
                    return <td {...cell.getCellProps()}>{cell.render('Cell')}</td>
                  })}
                </tr>
              )
            }
          )}
        </tbody>
      </Table>
      { paginationUI }
    </>
  )
}
