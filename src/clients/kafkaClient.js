const { Kafka, logLevel } = require('kafkajs');
const config = require('../config');

const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers,
  logLevel: logLevel.NOTHING,
  connectionTimeout: 3000,
  retry: { retries: 2 },
});

const producer = kafka.producer();
let producerConnected = false;

async function getProducer() {
  if (!producerConnected) {
    await producer.connect();
    producerConnected = true;
    console.log('[kafka] producer connected:', config.kafka.brokers.join(','));
  }
  return producer;
}

async function publish(topic, payload) {
  const p = await getProducer();
  await p.send({
    topic,
    messages: [{
      key: payload.booking_id || payload.ticket_id || undefined,
      value: JSON.stringify(payload),
    }],
  });
}

function createConsumer(groupIdSuffix) {
  return kafka.consumer({ groupId: `${config.kafka.consumerGroup}-${groupIdSuffix}` });
}

module.exports = { kafka, getProducer, publish, createConsumer };
