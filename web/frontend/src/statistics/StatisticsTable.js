import React from 'react'
import styled from '@emotion/styled'

import { AutoSizer, Table, Column } from 'react-virtualized'

const StatisticsTableContainer = styled.div`
  .ReactVirtualized__Table.empty {
    background-color: #f5f5f5;
    .ReactVirtualized__Grid {
      background-color: #f5f5f5;
    }
  }
`

function StatisticsTable({ rows, sortBy, sortDirection, setSortSettings }) {
  return (
    <StatisticsTableContainer>
      <AutoSizer disableHeight>
        {({ width }) => (
          <Table
            className={rows.length === 0 ? 'empty' : ''}
            headerHeight={20}
            height={750}
            rowHeight={20}
            rowCount={rows.length}
            width={width}
            sortBy={sortBy}
            sortDirection={sortDirection}
            rowGetter={({ index }) => rows[index]}
            noRowsRenderer={() => (
              <div style={{ padding: '1rem' }}>
                No window for current selection.
              </div>
            )}
            sort={sortSettings => setSortSettings(sortSettings)}
          >
            <Column dataKey="postId" width={100} label="Post ID" />
            <Column dataKey="value" width={100} label="Value" />
          </Table>
        )}
      </AutoSizer>
    </StatisticsTableContainer>
  )
}

export default StatisticsTable
