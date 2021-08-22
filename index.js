import dotenv from 'dotenv';
dotenv.config();

// import
import debug from 'debug';
import config from 'config';
import lodash from 'lodash';
import fs from 'fs';
import {
  randId,
  randAlphaString,
  randNumString,
  randItemData,
  randZipCode,
  randLastName,
  randInt,
  randFloat,
  randNumber,
} from './util/rand.js';
import { exit } from 'process';
const logger = debug('app:tpcc');

// read driver name from command line
const envName = process.env.NODE_ENV;
const driverName = process.argv[2];
logger(`env = "${envName}"`);
logger(`driver = "${driverName}"`);

// dynamically load driver module
const driver = await import(`./drivers/${driverName}/driver.js`);
logger('driver loaded.');

// load driver config
const driverConfig = JSON.parse(
  fs.readFileSync(`./drivers/${driverName}/config.json`, 'utf8')
);
logger('driver config loaded.');

// connect to the database
const driverConnection = await driver.connect(driverConfig);
logger('driver connected.');

// load data
const dataPath = config.get('data.path');
let data;
if (!config.get('data.rebuild')) {
  try {
    data = JSON.parse(
      fs.readFileSync(dataPath, 'utf8')
    );
    logger('data read from cache.');
  } catch (err) {
    debug(err);
  }
}
if (!data) {
  const cardinality = config.get('cardinality');
  data = { items: [], warehouses: [] };
  for (let i = 0; i < cardinality.item; ++i) {
    const item = {
      id: randId(),
      imageId: randId(),
      name: randAlphaString(14, 24),
      price: randNumber(1, 100, 0.01),
      data: randItemData(),
    };
    data.items.push(item);
  }
  for (let w = 0; w < cardinality.warehouse; ++w) {
    const warehouse = {
      id: randId(),
      name: randAlphaString(6, 10),
      street1: randAlphaString(10, 20),
      street2: randAlphaString(10, 20),
      city: randAlphaString(10, 20),
      state: randAlphaString(2, 2),
      zip: randZipCode(),
      tax: randNumber(0.0, 0.2, 0.0001),
      ytd: 300_000,
      districts: [],
      stock: [],
    };
    data.warehouses.push(warehouse);
    for (let d = 0; d < 10; ++d) {
      const district = {
        id: randId(),
        name: randAlphaString(6, 10),
        street1: randAlphaString(10, 20),
        street2: randAlphaString(10, 20),
        city: randAlphaString(10, 20),
        state: randAlphaString(2, 2),
        zip: randZipCode(),
        tax: randNumber(0.0, 0.2, 0.0001),
        ytd: 30_000,
        customers: [],
        //nextOrderId: 3001 // page 66
        //newOrders: [],
      };
      warehouse.districts.push(district);
      for (let c = 0; c < 3000; ++c) {
        const customer = {
          id: randId(),
          first: randAlphaString(8, 16),
          middle: 'OE',
          last: randLastName(c),
          street1: randAlphaString(10, 20),
          street2: randAlphaString(10, 20),
          city: randAlphaString(10, 20),
          state: randAlphaString(2, 2),
          zip: randZipCode(),
          phone: randNumString(16, 16),
          since: new Date(),
          credit: randInt,
          creditLim: 50_000,
          discount: randNumber(0.0, 0.5, 0.0001),
          balance: -10,
          ytd: 10,
          paymentCnt: 1,
          deliveryCnt: 0,
          data: randAlphaString(300, 500),
          history: [
            {
              date: new Date(),
              amount: 10,
              data: randAlphaString(12, 24),
            },
          ],
          orders: [],
        };
        district.customers.push(customer);
      }
      const shuffledCustomers = lodash.shuffle(district.customers);
      for (let o = 0; o < 3000; ++o) {
        const customer = shuffledCustomers[o];
        const order = {
          id: randId(),
          entryDate: new Date(),
          carrier: o < 2101 ? randInt(1, 10) : null,
          allLocal: true,
          lines: [],
        };
        customer.orders.push(order);
        const lineCount = randInt(5, 15);
        for (let i = 0; i < lineCount; ++i) {
          const item = data.items[randInt(0, cardinality.item - 1)];
          order.lines.push({
            itemId: item.id,
            quantity: 5,
            supplyWarehouseId: warehouse.id,
            deliveryDate: o < 2101 ? order.entryDate : null,
            amount: o < 2101 ? 0 : randNumber(0.01, 9999.99, 0.01),
            districtInfo: randAlphaString(24, 24),
          });
        }
      }
      // for (let o = 2100; o < 3000; ++o) {
      //   const order = districtOrders[o];
      //   district.newOrders.push(order);
      // }
    }
    for (const item of data.items) {
      const stock = {
        itemId: item.id,
        quantity: randInt(10, 100),
        ytd: 0,
        orderCnt: 0,
        remoteOrderCnt: 0,
        data: randItemData(),
        districtInfo: warehouse.districts.map(() => randAlphaString(24, 24)),
      };
      warehouse.stock.push(stock);
    }
  }
  if (config.get('data.save')) {
    fs.mkdirSync('./tmp', { recursive: true });
    fs.writeFileSync(dataPath, JSON.stringify(data, null, 2));
  }
  logger('data generated.');
}
await driver.loadData(driverConfig, driverConnection, data);
logger('data loaded.');

// run benchmarks
logger('TODO: benchmarks finished.');

// exit
logger('done.');
process.exit(0);
