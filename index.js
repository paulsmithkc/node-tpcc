import dotenv from 'dotenv';
import debug from 'debug';
import fs from 'fs';

// initialize
dotenv.config();
const logger = debug('app:tpcc');

// read driver name from command line
const driverName = process.argv[2];
logger(`driver = "${driverName}"`);

// load driver and config
const driver = await import(`./drivers/${driverName}/driver.js`);
logger('driver loaded.');
const config = JSON.parse(fs.readFileSync(`./drivers/${driverName}/config.json`, 'utf-8'));
logger('driver config loaded.');

// connect to the database
const connection = await driver.connect(config);
logger('driver connected.');

// load data
await driver.loadData(config, connection, 'warehouse', []);
logger('TODO: data loaded.');

// run benchmarks
logger('TODO: benchmarks finished.');

// exit
logger('done.');
process.exit(0);
