import React, { useState } from 'react'
import gql from 'graphql-tag'
import _has from 'lodash/has'

import { useQuery } from 'react-apollo-hooks'
import { Table, Column, AutoSizer, SortDirection } from 'react-virtualized'
import { Skeleton, Row, Col, Divider } from 'antd'
import { ResponsiveContainer, LineChart, CartesianGrid } from 'recharts'

import PollIntervalControl from '../common/PollIntervalControl'
import {
  sortItems,
  formatTimestamp,
  formatObjectAsString,
} from '../common/util'

const ANOMALIES_QUERY = gql`
  query {
    anomaliesOutputs {
      personId
      anomalousEvents
      timestamp
      voteCounts {
        TIMESPAN
        INTERACTIONS_RATIO
        NEW_USER_LIKES
        CONTENTS_SHORT
        CONTENTS_MEDIUM
        CONTENTS_LONG
        CONTENTS_EMPTY
        TAG_COUNT
      }
    }
  }
`

function Anomalies() {
  const [pollInterval, setPollInterval] = useState(5000)
  const [{ sortBy, sortDirection }, setSortSettings] = useState({
    sortBy: 'timestamp',
    sortDirection: SortDirection.DESC,
  })
  const { data, error, loading } = useQuery(ANOMALIES_QUERY, {
    pollInterval,
  })

  if (loading || !_has(data, 'anomaliesOutputs')) {
    return <Skeleton active />
  }

  if (error) {
    console.error(error)
    return null
  }

  // sort the outputs according to the specified settings
  const rows = sortItems(
    data.anomaliesOutputs.map(output => ({
      ...output,
      timestamp: formatTimestamp(output.timestamp),
      voteCounts: formatObjectAsString(output.voteCounts),
    })),
    sortBy,
    sortDirection
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

      <Divider />

      <Row gutter={16}>
        <Col span={16}>
          <AutoSizer disableHeight>
            {({ width }) => (
              <Table
                headerHeight={20}
                height={750}
                rowHeight={20}
                rowCount={rows.length}
                width={width}
                sortBy={sortBy}
                sortDirection={sortDirection}
                rowGetter={({ index }) => rows[index]}
                sort={sortSettings => setSortSettings(sortSettings)}
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
