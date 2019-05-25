const Redis = require('ioredis')

let instances = []

function getRedisSingleton(db = 1) {
  redis = new Redis({
    db,
    host: process.env.REDIS_HOST || 'localhost',
  })
  instances.push(redis)
  return redis
}

async function closeInstances() {
  return Promise.all(instances.map(redis => redis.disconnect()))
}

module.exports = {
  getRedisSingleton,
  closeInstances,
}
