import { DBFFile } from 'dbffile';
import { FileVersion } from 'dbffile/dist/file-version';
import iconv from 'iconv-lite';

export class DBFileService {
  constructor() {}

  async readDBFile<T = {}>(name: string) {
    const dbf = await DBFFile.open(name + '.dbf');
    const res = [];
    for await (const record of dbf) {
      const newData = Object.keys(record).reduce(
        (prev, key) => ({
          ...prev,
          [key]:
            typeof record[key] === 'string'
              ? iconv.decode(
                  Buffer.from(record[key] as string, 'binary'),
                  'tis-620',
                )
              : record[key],
        }),
        {},
      );
      res.push(newData);
    }
    return res as T[];
  }

  async writeDBFile({
    name,
    records,
  }: {
    name: string;
    records: Record<string, unknown>[];
  }) {
    try {
      const dbf = await DBFFile.open(name + '.dbf');

      console.log('dbf', dbf);

      if (!dbf._version) {
        throw Error('');
      }

      const newDbf = await DBFFile.create('SON.DBF', dbf.fields, {
        fileVersion: dbf._version as FileVersion,
      });

      console.log('newDbf', newDbf);

      await newDbf.appendRecords(records);
      console.log('append record done');
    } catch (error) {
      console.error(error);
      throw new Error('Something went wrong');
    }
  }

  encodeDBFile(records: Record<string, unknown>[]) {
    const encodedRecords = [];

    for (const item of records) {
      encodedRecords.push(
        Object.keys(item).reduce(
          (prev, key) => ({
            ...prev,
            [key]:
              typeof item[key] === 'string'
                ? Buffer.from(
                    iconv.encode(item[key] as string, 'tis-620'),
                  ).toString('binary')
                : item[key],
          }),
          {},
        ),
      );
    }

    return encodedRecords;
  }
}
