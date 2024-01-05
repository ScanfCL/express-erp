import { Kafka } from 'kafkajs';
import { CustomerService } from '../services/customer';
import { DBFileService } from '../services/dbfile';
import { SalesInvoiceService } from '../services/sales-invoice';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['0.0.0.0:9092', '0.0.0.0:9092'],
});

enum TOPICS {
  CUSTOMER = 'CUSTOMER',
  SALES_INVOICE = 'SALES_INVOICE',
}

const consumer = kafka.consumer({ groupId: 'kafka' });

const dbFileService = new DBFileService();
const customerService = new CustomerService(dbFileService, consumer);
const salesInvoiceService = new SalesInvoiceService(dbFileService, consumer);

export const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.CUSTOMER, fromBeginning: true });
  await consumer.subscribe({
    topic: TOPICS.SALES_INVOICE,
    fromBeginning: true,
  });

  console.log('Consumer connected !');

  await consumer.run({
    eachMessage: async (eachMessage) => {
      const { topic } = eachMessage;

      console.log('eachMessage', eachMessage);

      switch (topic) {
        case TOPICS.CUSTOMER:
          await customerService.processMessage(eachMessage);
          break;
        case TOPICS.SALES_INVOICE:
          await salesInvoiceService.processMessage(eachMessage);
      }
    },
  });
};
