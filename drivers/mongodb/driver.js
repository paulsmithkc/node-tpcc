import debug from 'debug';
import _ from 'lodash';
import { MongoClient } from 'mongodb';
import { randId } from '../../util/rand.js';
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

export async function init(config, db) {
  await db.collection('item').deleteMany({});
  await db.collection('warehouse').deleteMany({});
  await db.collection('district').deleteMany({});
  await db.collection('customer').deleteMany({});
  await db.collection('stock').deleteMany({});
  await db.collection('order').deleteMany({});
  await db.collection('newOrder').deleteMany({});
}

export async function loadData(config, db, data) {
  const items = data.items;
  await db.collection('item').insertMany(items);

  const warehouses = [];
  const customers = [];
  const stocks = [];
  for (const w of data.warehouses) {
    const warehouse = _.pick(w, [
      '_id',
      'name',
      'street1',
      'street2',
      'city',
      'state',
      'zip',
      'tax',
      'ytd',
    ]);
    warehouse.districts = [];
    warehouses.push(warehouse);

    for (const d of w.districts) {
      warehouse.districts.push(
        _.pick(d, [
          '_id',
          'name',
          'street1',
          'street2',
          'city',
          'state',
          'zip',
          'tax',
          'ytd',
        ])
      );

      for (const c of d.customers) {
        const customer = c;
        // denormalize data needed for transactions
        customer.warehouse = _.pick(w, ['_id', 'name', 'tax']);
        customer.district = _.pick(d, ['_id', 'name', 'tax']);
        customers.push(customer);
      }
    }

    for (const s of w.stocks) {
      stocks.push(s);
    }
  }

  await db.collection('warehouse').insertMany(warehouses);
  await db.collection('customer').insertMany(customers);
  await db.collection('stock').insertMany(stocks);

  // TODO: populate other collections
  // TODO: create indexes
}

export async function doNewOrder(
  config,
  db,
  { warehouseId, districtId, customerId, lines }
) {
  logger(`doNewOrder(${customerId})`);

  const [customer, items, stocks] = await Promise.all([
    db.collection('customer').findOne({ _id: { $eq: customerId } }),
    db
      .collection('item')
      .find({ _id: { $in: _.map(lines, (x) => x.itemId) } })
      .toArray(),
    db
      .collection('stock')
      .find({
        _id: { $in: _.map(lines, (x) => `${x.supplyWarehouseId}-${x.itemId}`) },
      })
      .toArray(),
  ]);
  if (!customer) {
    throw new Error(`customer ${customerId} not found.`);
  }
  // TODO: handle orders from another district (2.4.1.2)
  if (customer.warehouse._id != warehouseId) {
    throw new Error('customer is not in this warehouse.');
  }
  if (customer.district._id != districtId) {
    throw new Error('customer is not in this district.');
  }

  // logger('customerId', customerId);
  // logger('items', items.length);
  // logger('items[0]', items[0]);
  // logger('stocks', stocks.length);
  // logger('stocks[0]', stocks[0]);

  const order = {
    _id: randId(),
    entryDate: new Date(),
    allLocal: true,
    lines: [],
  };
  const stockUpdates = [];
  let totalAmount = 0;

  for (const line of lines) {
    const itemId = line.itemId;
    const item = _.find(items, (x) => x._id == itemId);
    if (!item) {
      throw new Error(`item ${itemId} not found.`);
    }

    const stockId = `${line.supplyWarehouseId}-${line.itemId}`;
    const stock = _.find(stocks, (x) => x._id == stockId);
    if (!stock) {
      throw new Error(`stock ${stockId} not found.`);
    }

    const amount = line.quantity * item.price;
    totalAmount += amount;

    order.lines.push({
      itemId: line.itemId,
      quantity: line.quantity,
      supplyWarehouseId: line.supplyWarehouseId,
      amount,
      districtInfo: _.find(stock.districts, (x) => x._id == districtId).info,
    });
    if (line.supplyWarehouseId != warehouseId) {
      order.allLocal = false;
    }

    const prevStockQuantity = stock.quantity;
    stock.quantity -= line.quantity;
    if (stock.quantity < 10) {
      stock.quantity += 91;
    }

    stockUpdates.push({
      updateOne: {
        filter: { _id: stockId },
        update: {
          $inc: {
            quantity: stock.quantity - prevStockQuantity,
            ytd: line.quantity,
            orderCnt: 1,
            remoteOrderCnt: line.supplyWarehouseId != warehouseId ? 1 : 0,
          },
        },
      },
    });
  }

  totalAmount *= 1 - customer.discount;
  totalAmount *= 1 + customer.warehouse.tax + customer.district.tax;
  logger('totalAmount', totalAmount);

  await Promise.all([
    db.collection('customer').updateOne(
      {
        _id: { $eq: customerId },
      },
      {
        $push: { orders: order },
      }
    ),
    db.collection('stock').bulkWrite(stockUpdates),
  ]);

  logger('doNewOrder.');
  return { totalAmount };
}

export async function doPayment(
  config,
  db,
  { warehouseId, districtId, customer, amount }
) {
  logger('doPayment()');
  logger('customer', customer);

  // TODO: implement payment

  logger('doPayment.');
}
