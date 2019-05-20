const { gql } = require('apollo-server')

module.exports = gql`
  enum OutputType {
    REPLY_COUNT
    COMMENT_COUNT
    UNIQUE_PERSON_COUNT
  }

  type StatisticsOutput {
    timestamp: Float!
    postId: Int!
    value: Int!
    outputType: OutputType!
  }

  type PersonSimilarity {
    personId: Int!
    similarity: Float!
  }

  type RecommendationsOutput {
    personId: Int!
    similarities: [PersonSimilarity!]!
  }

  type VoteCounts {
    TIMESPAN: Int
    CONTENTS_SHORT: Int
    CONTENTS_MEDIUM: Int
    CONTENTS_LONG: Int
  }

  type AnomaliesOutput {
    personId: Int!
    anomalousEvents: [String!]!
    voteCounts: VoteCounts!
  }

  type Query {
    statisticsOutputs: [StatisticsOutput!]
    recommendationsOutputs: [RecommendationsOutput!]
    anomaliesOutputs: [AnomaliesOutput!]
  }

  type Subscription {
    newStatisticsOutput: StatisticsOutput!
    newRecommendations: RecommendationsOutput!
    newAnomalies: AnomaliesOutput!
  }
`
