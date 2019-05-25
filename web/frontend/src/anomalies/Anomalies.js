import React, { useState } from 'react'
import gql from 'graphql-tag'
import _has from 'lodash/has'
import dayjs from 'dayjs'
import _sortBy from 'lodash/sortBy'

import { useQuery } from 'react-apollo-hooks'
import { Table, Column, AutoSizer } from 'react-virtualized'
import { Skeleton, Row, Col } from 'antd'
import { ResponsiveContainer, LineChart, CartesianGrid } from 'recharts'

import PollIntervalControl from '../common/PollIntervalControl'

const ANOMALIES_QUERY = gql`
  query AnomaliesOutputs {
    anomaliesOutputs {
      personId
      anomalousEvents
      voteCounts {
        TIMESPAN
        INTERACTIONS_RATIO
        NEW_USER_LIKES
        CONTENTS_SHORT
        CONTENTS_MEDIUM
        CONTENTS_LONG
      }
      timestamp
    }
  }
`

function Anomalies() {
  const [sortBy, setSortBy] = useState('personId')
  const [pollInterval, setPollInterval] = useState(1000)

  const { data, error, loading } = useQuery(ANOMALIES_QUERY, {
    pollInterval,
  })

  if (loading || !_has(data, 'anomaliesOutputs.0')) {
    return <Skeleton active />
  }

  if (error) {
    console.error(error)
    return null
  }

  const rows = _sortBy(
    data.anomaliesOutputs.map(output => ({
      ...output,
      timestamp: dayjs(output.timestamp).format('YYYY-MM-DD HH:mm:ss'),
      voteCounts: Object.entries(output.voteCounts)
        .filter(([key, value]) => key !== '__typename' && value != null)
        .map(([key, value]) => `${key}=${value}`)
        .join(', '),
    })),
    output => output[sortBy]
  )

  return (
    <>
      <Row>
        <Col>
          <PollIntervalControl
            pollInterval={pollInterval}
            setPollInterval={setPollInterval}
          />
        </Col>
      </Row>
      <Row gutter={16}>
        <Col span={16}>
          <AutoSizer disableHeight>
            {({ width }) => (
              <Table
                headerHeight={20}
                height={750}
                rowCount={rows.length}
                rowHeight={20}
                rowGetter={({ index }) => rows[index]}
                width={width}
                sort={({ sortBy }) => setSortBy(sortBy)}
                sortBy={sortBy}
              >
                <Column dataKey="personId" width={100} label="Person ID" />
                <Column dataKey="timestamp" width={150} label="Timestamp" />
                <Column
                  dataKey="anomalousEvents"
                  width={300}
                  label="Anomalous Events"
                />
                <Column dataKey="voteCounts" width={500} label="Vote Counts" />
              </Table>
            )}
          </AutoSizer>
        </Col>
        <Col span={8}>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart>
              <CartesianGrid strokeDasharray="3 3" />
            </LineChart>
          </ResponsiveContainer>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart>
              <CartesianGrid strokeDasharray="3 3" />
            </LineChart>
          </ResponsiveContainer>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart>
              <CartesianGrid strokeDasharray="3 3" />
            </LineChart>
          </ResponsiveContainer>
        </Col>
      </Row>
    </>
  )
}

export default Anomalies
