import { Consumer, KafkaMessage } from 'kafkajs';
import { DBFileService } from './dbfile';

const DB_FILE_PATH = 'ARMAS';

export type CustomerCreateInput = {
  CUSCOD: string;
  CUSTYP: string;
  PRENAM: string;
  CUSNAM: string;
  ADDR01: string;
  ADDR02: string;
  ADDR03: string;
  ZIPCOD: string;
  TELNUM: string;
  CONTACT: string;
  CUSNAM2: string;
  TAXID: string;
  ORGNUM: number;
  TAXTYP: string;
  TAXRAT: number | null;
  TAXGRP: string;
  TAXCOND: string;
  SHIPTO: string;
  SLMCOD: string;
  AREACOD: string;
  PAYTRM: number;
  PAYCOND: string;
  PAYER: string;
  TABPR: string;
  DISC: string;
  BALANCE: number;
  CHQRCV: number;
  CRLINE: number;
  LASIVC: string;
  ACCNUM: string;
  REMARK: string;
  DLVBY: string;
  TRACKSAL: string;
  CREBY: string;
  CREDAT: string;
  USERID: string;
  CHGDAT: string;
  STATUS: string;
  INACTDAT: string | null;
};

export enum CustomerActionType {
  Create = 'Create',
  Update = 'Update',
}

export type CustomerPayloadMessage = {
  action: CustomerActionType;
  customer: CustomerCreateInput;
};

export class CustomerService {
  constructor(
    private dbFileService: DBFileService,
    private consumer: Consumer,
  ) {}

  async processMessage({
    topic,
    message,
    partition,
  }: {
    topic: string;
    message: KafkaMessage;
    partition: number;
  }) {
    const payload = JSON.parse(
      message.value?.toString() || '',
    ) as CustomerPayloadMessage;

    console.log('process Message', payload);

    switch (payload.action) {
      case CustomerActionType.Create:
        await this.createCustomer(payload.customer);
      // case CustomerActionType.Update:
      //   throw new Error('Not implemented');
    }

    await this.consumer.commitOffsets([
      {
        topic,
        partition,
        offset: (parseInt(message.offset, 10) + 1).toString(),
      },
    ]);
  }

  async createCustomer(payload: CustomerCreateInput) {
    try {
      console.log('processing Create customer');
      const records =
        await this.dbFileService.readDBFile<CustomerCreateInput>(DB_FILE_PATH);

      // const newData = { ...payload, CUSCOD: 'TEST1' };
      // records.push(newData);

      // console.log('newData', newData);

      const encodedRecords = this.dbFileService.encodeDBFile(records);

      console.log('encode done!');

      await this.dbFileService.writeDBFile({
        name: DB_FILE_PATH,
        records: encodedRecords,
      });

      console.log('write file done!');
    } catch (error) {
      console.log('error', error);
    }
  }
}
