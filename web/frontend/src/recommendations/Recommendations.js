import React, { useState } from 'react'
import gql from 'graphql-tag'
import _groupBy from 'lodash/groupBy'
import _has from 'lodash/has'
import dayjs from 'dayjs'

import { useQuery } from 'react-apollo-hooks'
import { Row, Col, Skeleton, Typography } from 'antd'

import WindowSlider from '../common/WindowSlider'
import PollIntervalControl from '../common/PollIntervalControl'
import UserRecommendationsCard from './UserRecommendationsCard'

const RECOMMENDATIONS_QUERY = gql`
  query RecommendationsOutputs {
    recommendationsOutputs {
      timestamp
      personId
      personName
      inactive
      similarities {
        personId
        personName
        similarity
      }
    }
  }
`

function Recommendations() {
  const [currentTs, setCurrentTs] = useState()
  const [pollInterval, setPollInterval] = useState(2000)

  const { data, error, loading } = useQuery(RECOMMENDATIONS_QUERY, {
    pollInterval,
  })

  if (loading || !_has(data, 'recommendationsOutputs.0')) {
    return <Skeleton active />
  }

  if (error) {
    console.error(error)
    return null
  }

  // extract the maximum and minimum timestamp from the available data
  let maxTs = null
  let minTs = null
  if (data.recommendationsOutputs.length > 0) {
    maxTs = data.recommendationsOutputs[0].timestamp
    const lastIndex = data.recommendationsOutputs.length - 1
    minTs = data.recommendationsOutputs[lastIndex].timestamp
  }

  // compute the timestamp of the window to show
  // if the selected window has moved past the current period of persistence
  // simply display the window with the smallest available timestamp
  const resultTs = currentTs && currentTs >= minTs ? currentTs : minTs

  const recommendations = _groupBy(
    data.recommendationsOutputs
      .filter(output => output.timestamp === resultTs)
      .flatMap(output =>
        output.similarities.map(similarity => ({
          personId: output.personId,
          personName: output.personName,
          friendPersonId: similarity.personId,
          friendPersonName: similarity.personName,
          similarity: similarity.similarity,
        }))
      ),
    output => output.personId
  )

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
          <WindowSlider
            maxTs={maxTs}
            minTs={minTs}
            currentTs={currentTs}
            setCurrentTs={setCurrentTs}
            windowLength={4}
          />
        </Col>
      </Row>
      <Row>
        <Col>
          <Typography.Title level={2}>
            {dayjs(resultTs).format('YYYY-MM-DD')}{' '}
            {dayjs(resultTs)
              .subtract(4, 'hours')
              .add(1, 'seconds')
              .format('HH:mm:ss')}
            -{dayjs(resultTs).format('HH:mm:ss')}
          </Typography.Title>
        </Col>
      </Row>
      <Row gutter={16}>
        {Object.keys(recommendations).map(personId => (
          <Col span={6}>
            <UserRecommendationsCard
              personId={personId}
              recommendations={recommendations[personId]}
            />
          </Col>
        ))}
      </Row>
    </>
  )
}

export default Recommendations
