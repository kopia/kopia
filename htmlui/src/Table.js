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
    setPageSize,
    state: { pageIndex, pageSize },
  } = useTable({
    columns,
    data,
    initialState: { pageSize: 10 },
    autoResetPage: false,
    autoResetSortBy: false,
  },
    useSortBy,
    usePagination,
  )

  if (pageIndex >= pageCount && pageIndex !== 0 && pageCount > 0) {
    gotoPage(pageCount - 1);
  }

  const paginationUI = pageOptions.length > 1 && 
  <>
    <Pagination size="sm" variant="dark">
      <Pagination.First onClick={() => gotoPage(0)} disabled={!canPreviousPage} />
      <Pagination.Prev onClick={() => previousPage()} disabled={!canPreviousPage} />
      {paginationItems(pageOptions.length, pageIndex+1, gotoPage)}
      <Pagination.Next onClick={() => nextPage()} disabled={!canNextPage} />
      <Pagination.Last onClick={() => gotoPage(pageCount - 1)} disabled={!canNextPage} />
    </Pagination>
    <Pagination size="sm" variant="dark">
    <select value={pageSize} onChange={e => {setPageSize(Number(e.target.value))}}>
      {[10, 20, 30, 40, 50, 100].map(pageSize => (<option key={pageSize} value={pageSize}>Show {pageSize}</option>))}
    </select>
    </Pagination>
    </>;

  return (
    <>
      <Table size="sm" striped bordered hover {...getTableProps()}>
        <thead className="table-dark">
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
