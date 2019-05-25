const Queue = require('bull')

function initializeQueue({ name, maxEventsPerSecond }) {
  if (maxEventsPerSecond) {
    return new Queue(name, {
      redis: {
        host: process.env.REDIS_HOST || 'localhost',
      },
      limiter: {
        max: maxEventsPerSecond,
        duration: 1000,
      },
    })
  }

  return new Queue(name, {
    redis: {
      host: process.env.REDIS_HOST || 'localhost',
    },
  })
}

module.exports = {
  initializeQueue,
}
