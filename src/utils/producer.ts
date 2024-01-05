import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['0.0.0.0:9092', '0.0.0.0:9092'],
});

const producer = kafka.producer();

export const run = async () => {
  await producer.connect();
  console.log('Producer connected');
};
