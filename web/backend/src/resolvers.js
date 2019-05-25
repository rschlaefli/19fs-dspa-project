const { getRedisSingleton } = require('./redis')
const { ANOMALIES } = require('./constants')

const redis = getRedisSingleton(1)

async function resolveStatisticsOutputs() {
  // get all items that are in the sorted set
  const outputs = await redis.zrevrangebyscore('statistics', '+inf', '-inf')

  // map all items to an object representation
  return outputs.map(output => JSON.parse(output))
}

async function resolveRecommendationsOutputs() {
  // get all items that are in the sorted set
  const outputs = await redis.zrevrangebyscore(
    'recommendations',
    '+inf',
    '-inf'
  )

  // map all items to an object representation
  return outputs.map(output => JSON.parse(output))
}

async function resolveAnomaliesOutputs() {
  // get all items that are in the sorted set
  const outputs = await redis.zrevrangebyscore('anomalies', '+inf', '-inf')

  // map all items to an object representation
  return outputs.map(output => JSON.parse(output))
}

module.exports = {
  Query: {
    anomaliesOutputs: resolveAnomaliesOutputs,
    recommendationsOutputs: resolveRecommendationsOutputs,
    statisticsOutputs: resolveStatisticsOutputs,
  },
}
