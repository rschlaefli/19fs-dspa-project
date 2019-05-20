const { ApolloServer } = require('apollo-server')
const { RedisPubSub } = require('graphql-redis-subscriptions')

const typeDefs = require('./types')
const { setupKafka, subscribeTo, messageToJson } = require('./kafka')

const pubsub = new RedisPubSub()

const NEW_STATISTICS_OUTPUT = 'NEW_STATISTICS_OUTPUT'
const NEW_RECOMMENDATIONS = 'NEW_RECOMMENDATIONS'
const NEW_ANOMALIES = 'NEW_ANOMALIES'

const resolvers = {
  Subscription: {
    newStatisticsOutput: {
      subscribe: () => pubsub.asyncIterator([NEW_STATISTICS_OUTPUT]),
    },
    newRecommendations: {
      subscribe: () => pubsub.asyncIterator([NEW_RECOMMENDATIONS]),
    },
    newAnomalies: {
      subscribe: () => pubsub.asyncIterator([NEW_ANOMALIES]),
    },
  },
}

const server = new ApolloServer({ typeDefs, resolvers })

async function main() {
  const START_FROM_BEGINNING = true
  const kafkaConsumer = await setupKafka()

  subscribeTo(kafkaConsumer, 'active-posts-out', START_FROM_BEGINNING)
  subscribeTo(kafkaConsumer, 'recommendations-out', START_FROM_BEGINNING)
  subscribeTo(kafkaConsumer, 'anomalies-out', START_FROM_BEGINNING)

  await kafkaConsumer.run({
    autoCommitInterval: 5000,
    autoCommitThreshold: 500,
    eachMessage: async ({ topic, message }) => {
      const messageAsJson = messageToJson(message)
      if (topic === 'active-posts-out') {
        pubsub.publish(NEW_STATISTICS_OUTPUT, {
          newStatisticsOutput: messageAsJson,
        })
      } else if (topic === 'recommendations-out') {
        pubsub.publish(NEW_RECOMMENDATIONS, {
          newRecommendations: messageAsJson,
        })
      } else if (topic === 'anomalies-out') {
        pubsub.publish(NEW_ANOMALIES, {
          newAnomalies: messageAsJson,
        })
      }
    },
  })

  return kafkaConsumer
}

server.listen().then(({ url }) => {
  console.log(`Server ready at ${url}`)

  main()
    .then(() => {
      console.log('success')
    })
    .catch(err => {
      console.error(err)
    })
})

process.on('SIGINT', () => {
  pubsub.close()
  process.exit(0)
})
