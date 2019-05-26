module.exports = {
  ANOMALIES: {
    DAYS_TO_KEEP: 7,
  },
  RECOMMENDATIONS: {
    DAYS_TO_KEEP: 1,
  },
  STATISTICS: {
    DAYS_TO_KEEP: 0.35,
  },
  ERROR_TYPES: ['unhandledRejection', 'uncaughtException'],
  SIGNAL_TRAPS: ['SIGTERM', 'SIGINT', 'SIGUSR2'],
  START_FROM_BEGINNING: true,
  PROCESSORS: {
    Statistics: __dirname + '/processors/processStatistics.js',
    Recommendations: __dirname + '/processors/processRecommendations.js',
    Anomalies: __dirname + '/processors/processAnomalies.js',
  },
}
