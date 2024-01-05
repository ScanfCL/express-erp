import express, { Express, Request, Response } from 'express';
import { Kafka } from 'kafkajs';
import { DBFileService } from './services/dbfile';
import { run as runConsumer } from './utils/consumer';

const app: Express = express();

const port: number = 3000;

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['0.0.0.0:9092', '0.0.0.0:9092'],
});

const producer = kafka.producer();
const dbfile = new DBFileService();

export const runProducer = async () => {
  await producer.connect();
  console.log('Producer connected');
};

app.get('/', (req: Request, res: Response) => {
  res.json({
    message: 'Hello Express + TypeScirpt!! son2',
  });
});

app.get('/read/:name', async (req: Request, res: Response) => {
  const records = await dbfile.readDBFile(req.params.name);

  res.json({
    message: JSON.stringify(records),
  });
});

app.get('/create/customer', async (req: Request, res: Response) => {
  const payload = {
    action: 'Create',
    customer: {
      CUSCOD: '12205',
      CUSTYP: 'HO',
      PRENAM: 'นาย',
      CUSNAM: 'ณัชพนธ์  แก้วโสภา',
      ADDR01: '298/1266 XT ห้วยขวาง เขตห้วยขวาง แขวงห้วยขวาง',
      ADDR02: '',
      ADDR03: 'กรุงเทพมหานคร',
      ZIPCOD: '10310',
      TELNUM: '0986688018',
      CONTACT: 'ณัชพนธ์  แก้วโสภา',
      CUSNAM2: 'nutchapon@zimpligital.com',
      TAXID: '1199900570089',
      ORGNUM: -1,
      TAXTYP: '',
      TAXRAT: null,
      TAXGRP: '',
      TAXCOND: '',
      SHIPTO: '',
      SLMCOD: 'HH001',
      AREACOD: 'HO',
      PAYTRM: 99,
      PAYCOND: 'condition',
      PAYER: '',
      TABPR: '0',
      DISC: '    199.00',
      BALANCE: 0,
      CHQRCV: 0,
      CRLINE: 100000,
      LASIVC: '2023-12-26T00:00:00.000Z',
      ACCNUM: '1111-10',
      REMARK: 'note',
      DLVBY: 'A3',
      TRACKSAL: '',
      CREBY: 'ERP00',
      CREDAT: '2023-12-28T00:00:00.000Z',
      USERID: 'ERP00',
      CHGDAT: '2023-12-28T00:00:00.000Z',
      STATUS: '',
      INACTDAT: null,
    },
  };
  await producer.send({
    topic: 'CUSTOMER',
    messages: [{ value: JSON.stringify(payload) }],
  });

  console.log('send message success');

  res.json({
    message: 'Send message queue create customer successfully',
  });
});

app.listen(port, () => console.log(`Application is running on port ${port}`));

runConsumer().catch(console.error);
runProducer().catch(console.error);
