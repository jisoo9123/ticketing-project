require('dotenv').config();

module.exports = {
  port: parseInt(process.env.PORT || '3000', 10),

  redisUrl: process.env.REDIS_URL || 'redis://127.0.0.1:6379',

  pg: {
    host: process.env.PGHOST || '127.0.0.1',
    port: parseInt(process.env.PGPORT || '5432', 10),
    database: process.env.PGDATABASE || 'ticketing',
    user: process.env.PGUSER || 'ticketing_app',
    password: process.env.PGPASSWORD || '',
  },

  kafka: {
    brokers: (process.env.KAFKA_BROKERS || '127.0.0.1:9092').split(','),
    clientId: process.env.KAFKA_CLIENT_ID || 'app-namespace',
    consumerGroup: process.env.KAFKA_CONSUMER_GROUP || 'app-namespace-consumers',
  },

  topics: {
    bookingCreated: 'booking.created',
    paymentRequested: 'payment.requested',
    paymentCompleted: 'payment.completed',
    notificationRequested: 'notification.requested',
  },

  idempotencyTtlSeconds: parseInt(process.env.IDEMPOTENCY_TTL_SECONDS || '86400', 10),
  seatHoldTtlSeconds: parseInt(process.env.SEAT_HOLD_TTL_SECONDS || '300', 10),
};
