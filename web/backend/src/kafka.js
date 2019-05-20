const { Kafka } = require('kafkajs')

async function setupKafka(groupId = 'nodejs') {
  try {
    const kafka = new Kafka({
      clientId: 'visualization',
      brokers: ['localhost:29092'],
    })

    const consumer = kafka.consumer({
      groupId,
    })

    await consumer.connect()

    return consumer
  } catch (err) {
    console.error(err)
  }
}

async function subscribeTo(consumer, topic, fromBeginning = false) {
  try {
    await consumer.subscribe({ topic, fromBeginning })
  } catch (err) {
    console.error(err)
  }
}

function messageToJson(message) {
  try {
    const messageAsString = message.value.toString('utf-8', 1).trim()
    console.log(messageAsString)
    return JSON.parse(messageAsString)
  } catch (err) {
    console.error(err)
  }
}

module.exports = {
  setupKafka,
  subscribeTo,
  messageToJson,
}
