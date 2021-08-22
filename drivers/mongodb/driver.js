import debug from 'debug';
import { MongoClient, ObjectId } from 'mongodb';
const logger = debug('app:tpcc:driver:mongodb');

export async function connect(config) {
  const client = await MongoClient.connect(config.url, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    minPoolSize: config.minPoolSize,
    maxPoolSize: config.maxPoolSize,
  });
  const db = client.db(config.name);
  return db;
}

export async function loadData(config, connecttion, tableName, data) {
}
