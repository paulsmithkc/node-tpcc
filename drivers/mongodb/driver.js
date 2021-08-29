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
  const districts = [];
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
    warehouses.push(warehouse);

    for (const d of w.districts) {
      const district = _.pick(d, [
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
      // denormalize data needed for transactions
      district.warehouse = _.pick(w, ['_id', 'name', 'tax']);
      district.newOrders = d.newOrders;
      districts.push(district);

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
  await db.collection('district').insertMany(districts);
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
  const now = new Date();

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
    throw new Error(`customer ${customerId} not found!`);
  }

  // Handle remote orders (2.4.1.2)
  let warehouse = customer.warehouse;
  let district = customer.district;
  if (warehouseId != warehouse._id || districtId != district._id) {
    [warehouse, district] = await Promise.all([
      db
        .collection('warehouse')
        .findOne(
          { _id: { $eq: warehouseId } },
          { projection: { name: 1, tax: 1 } }
        ),
      db
        .collection('district')
        .findOne(
          { _id: { $eq: districtId } },
          { projection: { name: 1, tax: 1 } }
        ),
    ]);
  }

  const orderId = randId();
  const order = {
    _id: orderId,
    entryDate: now,
    allLocal: true,
    lines: [],
  };
  const stockUpdates = [];
  let totalAmount = 0;

  for (const line of lines) {
    const itemId = line.itemId;
    const item = _.find(items, (x) => x._id == itemId);
    if (!item) {
      throw new Error(`item ${itemId} not found!`);
    }

    const stockId = `${line.supplyWarehouseId}-${line.itemId}`;
    const stock = _.find(stocks, (x) => x._id == stockId);
    if (!stock) {
      throw new Error(`stock ${stockId} not found!`);
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
  totalAmount *= 1 + warehouse.tax + district.tax;
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
    db.collection('district').updateOne(
      {
        _id: { $eq: districtId },
      },
      {
        $push: { newOrders: { customerId, orderId } },
      }
    ),
    db.collection('stock').bulkWrite(stockUpdates),
  ]);

  logger('doNewOrder.');
  return { orderId, totalAmount };
}

export async function doPayment(
  config,
  db,
  { warehouseId, districtId, customerFilter, amount }
) {
  logger('doPayment()');
  logger('customerFilter', customerFilter);
  const now = new Date();

  // Get the customer by ID or last name
  // See 2.5.2.2
  let customerQuery;
  if (customerFilter._id) {
    customerQuery = db
      .collection('customer')
      .findOne({ _id: { $eq: customerFilter._id } });
  } else if (customerFilter.last) {
    customerQuery = db
      .collection('customer')
      .find({ last: { $eq: customerFilter.last } })
      .sort({ first: 1, _id: 1 })
      .toArray()
      .then((arr) => arr[arr.length >> 1]); // pick the middle entry
  } else {
    throw new Error('customer not selected!');
  }

  // Did we find a customer?
  const customer = await customerQuery;
  if (!customer) {
    throw new Error(`customer not found!`);
  }

  // Handle remote payments (2.5.1.2)
  let warehouse = customer.warehouse;
  let district = customer.district;
  if (warehouseId != warehouse._id || districtId != district._id) {
    [warehouse, district] = await Promise.all([
      db
        .collection('warehouse')
        .findOne(
          { _id: { $eq: warehouseId } },
          { projection: { name: 1, tax: 1 } }
        ),
      db
        .collection('district')
        .findOne(
          { _id: { $eq: districtId } },
          { projection: { name: 1, tax: 1 } }
        ),
    ]);
  }

  const customerId = customer._id;
  let customerData = customer.data;

  if (customer.credit == 'BC') {
    customerData = `${customerId} ${customer.district._id} ${customer.warehouse._id} ${districtId} ${warehouseId} ${amount} | ${customer.data}`;
    customerData = customerData.substring(0, 500);
  }

  await Promise.all([
    db.collection('customer').updateOne(
      { _id: { $eq: customerId } },
      {
        $set: {
          data: customerData,
        },
        $inc: {
          balance: -amount,
          paymentYtd: amount,
          paymentCnt: 1,
        },
        $push: {
          history: {
            warehouseId,
            districtId,
            amount,
            date: now,
            data: `${warehouse.name}    ${district.name}`,
          },
        },
      }
    ),
    db.collection('warehouse').updateOne(
      { _id: warehouseId },
      {
        $inc: {
          ytd: amount,
        },
      }
    ),
    db.collection('district').updateOne(
      { _id: districtId },
      {
        $inc: {
          ytd: amount,
        },
      }
    ),
  ]);

  logger('doPayment.');
  return { customerId };
}

export async function doDelivery(config, db, { warehouseId, carrier }) {
  logger('doDelivery()');
  const now = new Date();

  const districts = await db
    .collection('district')
    .find({ 'warehouse._id': { $eq: warehouseId } })
    .toArray();

  for (const district of districts) {
    // TODO: deliver the oldest order from each district
  }

  logger('doDelivery.');
  return {};
}
