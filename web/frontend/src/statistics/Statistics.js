import React, { useState } from 'react'
import gql from 'graphql-tag'
import _has from 'lodash/has'
import styled from '@emotion/styled'

import { useQuery } from 'react-apollo-hooks'
import {
  AutoSizer,
  Table,
  Column,
  SortDirection,
  List,
} from 'react-virtualized'
import { Row, Col, Divider } from 'antd'
import { Skeleton, Typography } from 'antd'

import PollIntervalControl from '../common/PollIntervalControl'
import { sortItems, formatTimestamp } from '../common/util'
import { SingleSlider } from '../common/Slider'

const STATISTICS_QUERY = gql`
  query {
    statisticsOutputs {
      timestamp
      postId
      value
      outputType
    }
  }
`

const StatisticsTableContainer = styled.div`
  .ReactVirtualized__Table.empty {
    background-color: lightgrey;
    .ReactVirtualized__Grid {
      background-color: lightgrey;
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
                No results in current window.
              </div>
            )}
            sort={sortSettings => setSortSettings(sortSettings)}
          >
            <Column dataKey="timestamp" width={150} label="Timestamp" />
            <Column dataKey="postId" width={100} label="Post ID" />
            <Column dataKey="value" width={100} label="Value" />
          </Table>
        )}
      </AutoSizer>
    </StatisticsTableContainer>
  )
}

function Statistics() {
  const [pollInterval, setPollInterval] = useState(5000)
  const [currentTs, setCurrentTs] = useState()
  const [{ sortBy, sortDirection }, setSortSettings] = useState({
    sortBy: 'value',
    sortDirection: SortDirection.DESC,
  })
  const { data, error, loading } = useQuery(STATISTICS_QUERY, {
    pollInterval,
  })

  if (loading || !_has(data, 'statisticsOutputs.0')) {
    return <Skeleton active />
  }

  if (error) {
    console.error(error)
    return null
  }

  const postIdSet = new Set()
  let commentCounts = []
  let replyCounts = []
  let uniquePersonCounts = []

  let maxTs = null
  let minTs = null
  if (data.statisticsOutputs.length > 0) {
    maxTs = data.statisticsOutputs[0].timestamp
    const lastIndex = data.statisticsOutputs.length - 1
    minTs = data.statisticsOutputs[lastIndex].timestamp
  }

  // split the statistics outputs into the three output types
  data.statisticsOutputs.forEach(output => {
    const updatedOutput = {
      ...output,
      timestamp: formatTimestamp(output.timestamp),
    }

    if (
      currentTs ? output.timestamp === currentTs : output.timestamp === minTs
    ) {
      postIdSet.add(output.postId)
      if (output.outputType === 'REPLY_COUNT') {
        replyCounts.push(updatedOutput)
        return
      }
      if (output.outputType === 'COMMENT_COUNT') {
        commentCounts.push(updatedOutput)
        return
      }
    }

    if (
      currentTs ? output.timestamp === currentTs : output.timestamp === minTs
    ) {
      postIdSet.add(output.postId)
      uniquePersonCounts.push(updatedOutput)
    } else if (
      currentTs
        ? output.timestamp === currentTs - 1800000
        : output.Timestamp === minTs - 1800000
    ) {
      postIdSet.add(output.postId)
    }
  })

  // sort all output types according to the specified settings
  commentCounts = sortItems(commentCounts, sortBy, sortDirection)
  replyCounts = sortItems(replyCounts, sortBy, sortDirection)
  uniquePersonCounts = sortItems(uniquePersonCounts, sortBy, sortDirection)

  const activePostIds = Array.from(postIdSet.values()).sort((a, b) => a > b)

  return (
    <>
      <Row>
        <Col span={4}>
          <PollIntervalControl
            pollInterval={pollInterval}
            setPollInterval={setPollInterval}
          />
        </Col>

        <Col span={19}>
          <SingleSlider
            maxTs={maxTs}
            minTs={minTs}
            currentTs={currentTs}
            setCurrentTs={setCurrentTs}
          />
        </Col>
      </Row>

      <Divider />

      <Row gutter={16}>
        <Col span={6}>
          <Typography.Title level={2}>Comment Count</Typography.Title>
          <StatisticsTable
            sortBy={sortBy}
            sortDirection={sortDirection}
            rows={commentCounts}
            setSortSettings={setSortSettings}
          />
        </Col>

        <Col span={6}>
          <Typography.Title level={2}>Reply Count</Typography.Title>
          <StatisticsTable
            sortBy={sortBy}
            sortDirection={sortDirection}
            rows={replyCounts}
            setSortSettings={setSortSettings}
          />
        </Col>

        <Col span={6}>
          <Typography.Title level={2}>Unique Person Count</Typography.Title>
          <StatisticsTable
            sortBy={sortBy}
            sortDirection={sortDirection}
            rows={uniquePersonCounts}
            setSortSettings={setSortSettings}
          />
        </Col>

        <Col span={6}>
          <Typography.Title level={2}>Active Posts</Typography.Title>
          <AutoSizer disableHeight>
            {({ width }) => (
              <List
                headerHeight={20}
                height={750}
                rowHeight={20}
                rowCount={activePostIds.length}
                width={width}
                rowRenderer={({ index, style, key }) => (
                  <div key={key} style={style}>
                    {activePostIds[index]}
                  </div>
                )}
              />
            )}
          </AutoSizer>
        </Col>
      </Row>
    </>
  )
}

export default Statistics
