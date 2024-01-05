import { Consumer, KafkaMessage } from 'kafkajs';
import { DBFileService } from './dbfile';

const SALES_INVOICE_PATH = 'ARTRN';
const SALES_INVOICE_ITEM_PATH = 'STCRD';

enum SalesInvoiceActionType {
  Create,
  Update,
  AddServiceItems,
}

type SalesInvoiceCreateInput = {
  RECTYP: string;
  DOCNUM: string;
  DOCDAT: string;
  POSTGL: string;
  SONUM: string;
  CNTYP: string;
  DEPCOD: string;
  FLGVAT: string;
  SLMCOD: string;
  CUSCOD: string;
  SHIPTO: string;
  YOUREF: string;
  AREACOD: string;
  PAYTRM: number;
  DUEDAT: string;
  BILNUM: string;
  NXTSEQ: string;
  AMOUNT: number;
  DISC: string;
  DISCAMT: number;
  AFTDISC: number;
  ADVNUM: string;
  ADVAMT: number;
  TOTAL: number;
  AMTRAT0: number;
  VATRAT: number;
  VATAMT: number;
  NETAMT: number;
  NETVAL: number;
  RCVAMT: number;
  REMAMT: number;
  COMAMT: number;
  CMPLAPP: string;
  CMPLDAT: string;
  DOCSTAT: string;
  CSHRCV: number;
  CHQRCV: number;
  INTRCV: number;
  BEFTAX: number;
  TAXRAT: number | null;
  TAXCOND: string;
  TAX: number;
  IVCAMT: number;
  CHQPAS: number;
  VATDAT: string | null;
  VATPRD: string | null;
  VATLATE: string;
  SRV_VATTYP: string;
  DLVBY: string;
  RESERVE: string | null;
  USERID: string;
  CHGDAT: string;
  USERPRN: string;
  PRNDAT: string | null;
  PRNCNT: string | null;
  PRNTIM: string;
  AUTHID: string;
  APPROVE: string | null;
  BILLTO: string;
  ORGNUM: number;
};

type SalesInvoiceItemCreateInput = {
  STKCOD: string;
  LOCCOD: string;
  DOCNUM: string;
  SEQNUM: string;
  DOCDAT: string;
  RDOCNUM: string;
  REFNUM: string;
  DEPCOD: string;
  POSOPR: string;
  FREE: string;
  VATCOD: string;
  PEOPLE: string;
  SLMCOD: string;
  FLAG: string;
  TRNQTY: number;
  TQUCOD: string;
  TFACTOR: number;
  UNITPR: number;
  DISC: string;
  DISCAMT: number;
  TRNVAL: number;
  PHYBAL: number;
  RETSTK: string;
  XTRNQTY: number;
  XUNITPR: number;
  XTRNVAL: number;
  XSALVAL: number;
  NETVAL: number;
  MLOTNUM: string;
  MREMBAL: number;
  MREMVAL: number;
  BALCHG: number;
  VALCHG: number;
  LOTBAL: number;
  LOTVAL: number;
  LUNITPR: number;
  PSTKCOD: string;
  ACCNUMDR: string;
  ACCNUMCR: string;
  STKDES: string;
  PACKING: string;
  JOBCOD: string;
  PHASE: string;
  COSCOD: string;
  REIMBURSE: string;
};

type SalesInvoicePayloadMessage = {
  action: SalesInvoiceActionType;
  data: unknown;
};

export class SalesInvoiceService {
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
    ) as SalesInvoicePayloadMessage;
    switch (payload.action) {
      case SalesInvoiceActionType.Create:
        await this.createSalesInvoice(payload.data as SalesInvoiceCreateInput);
      case SalesInvoiceActionType.AddServiceItems:
        await this.addServiceItems(
          payload.data as SalesInvoiceItemCreateInput[],
        );
    }

    await this.consumer.commitOffsets([
      {
        topic,
        partition,
        offset: (parseInt(message.offset, 10) + 1).toString(),
      },
    ]);
  }

  async createSalesInvoice(payload: SalesInvoiceCreateInput) {
    const records = await this.dbFileService.readDBFile(SALES_INVOICE_PATH);
    records.push(payload);
  }

  async addServiceItems(items: SalesInvoiceItemCreateInput[]) {
    const records = await this.dbFileService.readDBFile(
      SALES_INVOICE_ITEM_PATH,
    );

    for (const item of items) {
      records.push(item);
    }

    await this.dbFileService.writeDBFile({
      name: SALES_INVOICE_ITEM_PATH,
      records,
    });
  }
}
