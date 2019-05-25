const { getRedisSingleton } = require('../redis')
const { RECOMMENDATIONS } = require('../constants')

const redis = getRedisSingleton(1)

module.exports = async function processRecommendations(job) {
  // stringify the data for storage in redis
  const dataAsString = JSON.stringify(job.data)

  // compute the first timestamp that is in the window-to-keep
  const keepFromTimestamp =
    job.data.timestamp - RECOMMENDATIONS.DAYS_TO_KEEP * 86400000

  return Promise.all([
    // add the current output to the sorted set and score with timestamp
    redis.zadd('recommendations', job.data.timestamp, dataAsString),
    // delete all existing outputs that are before the window to keep
    redis.zremrangebyscore('recommendations', '-inf', keepFromTimestamp),
  ])
}
