import React, { useState } from 'react'
import gql from 'graphql-tag'

import { useQuery } from 'react-apollo-hooks'
import { AutoSizer, Table, Column } from 'react-virtualized'
import { Row, Col } from 'antd'
import { Slider, Skeleton, Typography } from 'antd'

import PollIntervalControl from '../common/PollIntervalControl'

const STATISTICS_QUERY = gql`
  query StatisticsOutputs {
    statisticsOutputs {
      timestamp
      postId
      value
      outputType
    }
  }
`

function Statistics() {
  const [sliderValue, setSliderValue] = useState()
  const [pollInterval, setPollInterval] = useState(4000)

  const { data, error, loading } = useQuery(STATISTICS_QUERY, {
    pollInterval,
  })

  if (
    loading ||
    !data ||
    !data.statisticsOutputs ||
    data.statisticsOutputs.length === 0
  ) {
    return <Skeleton active />
  }

  if (error) {
    console.error(error)
    return null
  }

  const commentCounts = []
  const replyCounts = []
  const uniquePersonCounts = []

  let maxTs =
    data.statisticsOutputs.length > 0 && data.statisticsOutputs[0].timestamp
  let minTs =
    data.statisticsOutputs.length > 0 &&
    data.statisticsOutputs[data.statisticsOutputs.length - 1].timestamp

  data.statisticsOutputs.forEach(output => {
    if (
      sliderValue &&
      (output.timestamp > sliderValue[1] || output.timestamp < sliderValue[0])
    ) {
      return
    }

    if (output.timestamp > maxTs) {
      maxTs = output.timestamp
    } else if (output.timestamp < minTs) {
      minTs = output.timestamp
    }

    if (output.outputType === 'REPLY_COUNT') {
      replyCounts.push(output)
      return
    }

    if (output.outputType === 'COMMENT_COUNT') {
      commentCounts.push(output)
      return
    }

    uniquePersonCounts.push(output)
  })

  return (
    <>
      <Row>
        <Col span={4}>
          <PollIntervalControl
            pollInterval={pollInterval}
            setPollInterval={setPollInterval}
          />
        </Col>
        <Col span={18}>
          <Slider
            range
            max={maxTs}
            min={minTs}
            defaultValue={[minTs, maxTs]}
            value={sliderValue}
            onChange={setSliderValue}
          />
        </Col>
      </Row>
      <Row>
        <Col span={8}>
          <Typography.Title level={2}>Comment Count</Typography.Title>
          <AutoSizer>
            {({ height, width }) => (
              <Table
                headerHeight={20}
                height={height}
                rowCount={commentCounts.length}
                rowHeight={20}
                rowGetter={({ index }) => commentCounts[index]}
                width={width}
              >
                <Column dataKey="timestamp" width={150} label="Timestamp" />
                <Column dataKey="postId" width={100} label="Post ID" />
                <Column dataKey="value" width={100} label="Value" />
              </Table>
            )}
          </AutoSizer>
        </Col>

        <Col span={8}>
          <Typography.Title level={2}>Reply Count</Typography.Title>
          <AutoSizer>
            {({ height, width }) => (
              <Table
                headerHeight={20}
                height={height}
                rowCount={commentCounts.length}
                rowHeight={20}
                rowGetter={({ index }) => commentCounts[index]}
                width={width}
              >
                <Column dataKey="timestamp" width={150} label="Timestamp" />
                <Column dataKey="postId" width={100} label="Post ID" />
                <Column dataKey="value" width={100} label="Value" />
              </Table>
            )}
          </AutoSizer>
        </Col>

        <Col span={8}>
          <Typography.Title level={2}>Unique Person Count</Typography.Title>
          <AutoSizer>
            {({ height, width }) => (
              <Table
                headerHeight={20}
                height={height}
                rowCount={commentCounts.length}
                rowHeight={20}
                rowGetter={({ index }) => commentCounts[index]}
                width={width}
              >
                <Column dataKey="timestamp" width={150} label="Timestamp" />
                <Column dataKey="postId" width={100} label="Post ID" />
                <Column dataKey="value" width={100} label="Value" />
              </Table>
            )}
          </AutoSizer>
        </Col>
      </Row>
    </>
  )
}

export default Statistics
