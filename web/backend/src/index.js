const express = require('express')
const { ApolloServer } = require('apollo-server-express')

const { PROCESSORS, ERROR_TYPES, SIGNAL_TRAPS } = require('./constants')
const Kafka = require('./kafka')
const QueueFactory = require('./queue')
const typeDefs = require('./types')
const resolvers = require('./resolvers')
const Redis = require('./redis')

// prepare a kafka consumer and outputs queue instance
let kafkaConsumer
const outputsQueue = QueueFactory.initializeQueue({ name: 'outputs' })

// setup apollo server with static file serving using express
const server = new ApolloServer({ typeDefs, resolvers })
const app = express()
app.use(express.static('public'))
server.applyMiddleware({ app })

async function main() {
  console.log(
    `Kafka: ${process.env.KAFKA_URL}, Redis: ${process.env.REDIS_HOST}`
  )

  try {
    kafkaConsumer = await Kafka.setupKafka({
      brokers: [process.env.KAFKA_URL || 'localhost:29092'],
    })
  } catch (err) {
    console.error(err)
    return cleanup({ exitCode: 1 })
  }

  outputsQueue.process('active-posts-out', 10, PROCESSORS.Statistics)
  outputsQueue.process('recommendations-out', PROCESSORS.Recommendations)
  outputsQueue.process('anomalies-out', PROCESSORS.Anomalies)

  try {
    await Kafka.runKafkaConsumer({
      kafkaConsumer,
      outputsQueue,
    })
  } catch (err) {
    console.error(err)
    return cleanup({ exitCode: 1 })
  }

  return Promise.resolve()
}

app.listen(4000, err => {
  if (err) throw err

  console.log(`> Server booting up...`)

  main()
    .then(() => {
      console.log(
        `> Server ready at http://localhost:4000${server.graphqlPath}`
      )
    })
    .catch(err => {
      console.error(err)
      cleanup()
    })
})

async function cleanup({ exitCode }) {
  try {
    await outputsQueue.empty()
    await outputsQueue.close()
  } catch (err) {
    console.error(err)
    process.exit(1)
  }

  if (redis) {
    try {
      await Redis.closeInstances()
    } catch (err) {
      console.error(err)
      process.exit(1)
    }
  }

  if (kafkaConsumer) {
    try {
      await kafkaConsumer.disconnect()
    } catch (err) {
      console.error(err)
      process.exit(1)
    }
  }

  if (exitCode) {
    process.exit(exitCode)
  }
}

ERROR_TYPES.map(type => {
  process.on(type, async e => {
    await cleanup({ exitCode: 0 })
  })
})

SIGNAL_TRAPS.map(type => {
  process.once(type, async () => {
    await cleanup()
    process.kill(process.pid, type)
  })
})
