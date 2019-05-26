const { gql } = require('apollo-server-express')

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
    personName: String
    similarity: Float!
  }

  type RecommendationsOutput {
    timestamp: Float!
    personId: Int!
    personName: String
    inactive: Boolean!
    similarities: [PersonSimilarity!]!
  }

  type VoteCounts {
    TIMESPAN: Int
    CONTENTS_SHORT: Int
    CONTENTS_MEDIUM: Int
    CONTENTS_LONG: Int
    INTERACTIONS_RATIO: Int
    NEW_USER_LIKES: Int
    CONTENTS_EMPTY: Int
    TAG_COUNT: Int
  }

  type AnomaliesOutput {
    timestamp: Float!
    personId: Int!
    anomalousEvents: [String!]!
    voteCounts: VoteCounts!
  }

  type Query {
    statisticsOutputs: [StatisticsOutput!]
    recommendationsOutputs: [RecommendationsOutput!]
    anomaliesOutputs: [AnomaliesOutput!]
  }
`
