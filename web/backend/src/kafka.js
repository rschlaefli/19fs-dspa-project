const { Kafka } = require('kafkajs')

function messageToJson(message) {
  const messageAsString = message.value.toString('utf-8').trim()
  try {
    return JSON.parse(messageAsString)
  } catch (err) {
    console.log(messageAsString)
    console.error(err)
  }
}

async function setupKafka({
  brokers = ['localhost:29092'],
  clientId = 'visualization',
  fromBeginning = false,
  groupId = 'nodejs',
  topics = ['active-posts-out', 'recommendations-out', 'anomalies-out'],
} = {}) {
  const kafka = new Kafka({ clientId, brokers })

  const consumer = kafka.consumer({ groupId })
  await consumer.connect()

  await Promise.all(
    topics.map(async topic => {
      return consumer.subscribe({ topic, fromBeginning })
    })
  )

  return consumer
}

async function runKafkaConsumer({ kafkaConsumer, outputsQueue }) {
  return kafkaConsumer.run({
    autoCommitInterval: 5000,
    autoCommitThreshold: 50000,
    eachMessage: async ({ topic, message }) => {
      const messageAsJson = messageToJson(message)
      outputsQueue.add(topic, messageAsJson, {
        removeOnComplete: true,
        removeOnFail: true,
      })
    },
  })
}

module.exports = {
  setupKafka,
  runKafkaConsumer,
}
