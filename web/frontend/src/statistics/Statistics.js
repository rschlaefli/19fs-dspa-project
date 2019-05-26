import React, { useState } from 'react'
import gql from 'graphql-tag'
import _has from 'lodash/has'
import dayjs from 'dayjs'

import { useQuery } from 'react-apollo-hooks'
import { SortDirection } from 'react-virtualized'
import { Row, Col } from 'antd'
import { Skeleton, Typography } from 'antd'

import StatisticsTable from './StatisticsTable'
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
    maxTs = data.statisticsOutputs[1].timestamp
    const lastIndex = data.statisticsOutputs.length - 1
    minTs = data.statisticsOutputs[lastIndex - 1].timestamp
  }

  const resultTs = currentTs && currentTs >= minTs ? currentTs : minTs

  // split the statistics outputs into the three output types
  data.statisticsOutputs.forEach(output => {
    const updatedOutput = {
      ...output,
      timestamp: formatTimestamp(output.timestamp + 1),
    }

    if (output.timestamp === resultTs) {
      postIdSet.add(output.postId)
      if (output.outputType === 'REPLY_COUNT') {
        replyCounts.push(updatedOutput)
        return
      }

      if (output.outputType === 'COMMENT_COUNT') {
        commentCounts.push(updatedOutput)
        return
      }

      uniquePersonCounts.push(updatedOutput)
    }
  })

  // sort all output types according to the specified settings
  commentCounts = sortItems(commentCounts, sortBy, sortDirection)
  replyCounts = sortItems(replyCounts, sortBy, sortDirection)
  uniquePersonCounts = sortItems(uniquePersonCounts, sortBy, sortDirection)

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

      <Row>
        <Col>
          <Typography.Title level={2}>
            {dayjs(resultTs).format('YYYY-MM-DD HH:mm:ss')}
          </Typography.Title>
        </Col>
      </Row>

      <Row gutter={32}>
        <Col span={4}>
          <Typography.Title level={3}>Comment Count</Typography.Title>
          <StatisticsTable
            sortBy={sortBy}
            sortDirection={sortDirection}
            rows={commentCounts}
            setSortSettings={setSortSettings}
          />
        </Col>

        <Col span={4}>
          <Typography.Title level={3}>Reply Count</Typography.Title>
          <StatisticsTable
            sortBy={sortBy}
            sortDirection={sortDirection}
            rows={replyCounts}
            setSortSettings={setSortSettings}
          />
        </Col>

        <Col span={4}>
          <Typography.Title level={3}>Unique Person Count</Typography.Title>
          <StatisticsTable
            sortBy={sortBy}
            sortDirection={sortDirection}
            rows={uniquePersonCounts}
            setSortSettings={setSortSettings}
          />
        </Col>

        <Col span={4}>
          <Typography.Title level={3}>General Information</Typography.Title>
          <p>
            <strong>Active Posts:</strong> {commentCounts.length}
          </p>
          <p>
            <strong>Selected Window:</strong>{' '}
            {dayjs(resultTs).format('YYYY-MM-DD HH:mm:ss')}
          </p>
        </Col>
      </Row>
    </>
  )
}

export default Statistics
