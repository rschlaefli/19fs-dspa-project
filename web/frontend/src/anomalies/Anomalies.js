import React, { useState } from 'react'
import gql from 'graphql-tag'
import _has from 'lodash/has'

import { useQuery } from 'react-apollo-hooks'
import { Table, Column, AutoSizer, SortDirection } from 'react-virtualized'
import { Skeleton, Row, Col, Divider } from 'antd'

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
        TAG_COUNT
      }
    }
  }
`

function splitEventTypes(anomalousEvents) {
  const anomalousComments = []
  const anomalousLikes = []
  const anomalousPosts = []

  anomalousEvents.forEach(event => {
    const [eventType, ...rest] = event.split('_')
    if (eventType === 'POST') {
      anomalousPosts.push(rest[0])
    } else if (eventType === 'COMMENT') {
      anomalousComments.push(rest[0])
    } else if (eventType === 'LIKE') {
      anomalousLikes.push(rest[1])
    }
  })

  return {
    anomalousComments,
    anomalousLikes,
    anomalousPosts,
  }
}

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
      ...splitEventTypes(output.anomalousEvents),
      timestamp: formatTimestamp(output.timestamp),
      ...output.voteCounts,
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

      <Row>
        <Col>
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
                  dataKey="anomalousComments"
                  width={200}
                  label="Anomalous Comments"
                />
                <Column
                  dataKey="anomalousLikes"
                  width={200}
                  label="Anomalous Likes"
                />
                <Column
                  dataKey="anomalousPosts"
                  width={200}
                  label="Anomalous Posts"
                />
                <Column dataKey="TIMESPAN" width={100} label="TIMESPAN" />
                <Column dataKey="TAG_COUNT" width={100} label="TAG_COUNT" />
                <Column
                  dataKey="INTERACTIONS_RATIO"
                  width={150}
                  label="INTERACTIONS_RATIO"
                />
                <Column
                  dataKey="NEW_USER_LIKES"
                  width={150}
                  label="NEW_USER_LIKES"
                />
                <Column
                  dataKey="CONTENTS_SHORT"
                  width={150}
                  label="CONTENTS_SHORT"
                />
                <Column
                  dataKey="CONTENTS_MEDIUM"
                  width={150}
                  label="CONTENTS_MEDIUM"
                />
                <Column
                  dataKey="CONTENTS_LONG"
                  width={150}
                  label="CONTENTS_LONG"
                />
              </Table>
            )}
          </AutoSizer>
        </Col>
      </Row>
    </>
  )
}

export default Anomalies
