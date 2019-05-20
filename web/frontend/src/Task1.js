import React from 'react'
import gql from 'graphql-tag'
import { useSubscription } from 'react-apollo-hooks'

const NEW_STATISTICS_OUTPUT_SUBSCRIPTION = gql`
  subscription NewStatisticsOutput {
    newStatisticsOutput {
      timestamp
    }
  }
`

function Task1() {
  const { data, error, loading } = useSubscription(
    NEW_STATISTICS_OUTPUT_SUBSCRIPTION
  )

  if (error) {
    return error
  }

  if (loading) {
    return 'loading...'
  }

  return (
    <div>
      <h1>StatisticsOutput</h1>
      {data.newStatisticsOutput && data.newStatisticsOutput.timestamp}
    </div>
  )
}

export default Task1
