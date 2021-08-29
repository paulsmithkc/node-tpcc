import random from 'random';
import { nanoid } from 'nanoid';
import config from 'config';

const syllables = [
  'BAR',
  'OUGHT',
  'ABLE',
  'PRI',
  'PRES',
  'ESE',
  'ANTI',
  'CALLY',
  'ATION',
  'EING',
];

const c_customerLastName = randInt(0, 255);
const c_customerId = randInt(0, 1023);
const c_orderLineItemId = randInt(0, 8191);

/**
 * Generates a random ID using nanoid.
 * This was chosen to reflect real world security practices.
 *
 * @returns {string}
 */
export function randId() {
  return nanoid();
}

/**
 * Generates a random alphanumeric string.
 * See TPC-C 4.3.2.2
 * @returns {string}
 */
export function randAlphaString(x, y) {
  const len = x === y ? x : randInt(x, y);
  let chars = [];
  for (let i = 0; i < len; ++i) {
    chars.push(randAlphaChar());
  }
  return String.fromCharCode(...chars);
}

/**
 * Generates a random numeric string.
 * See TPC-C 4.3.2.2
 * @returns {string}
 */
export function randNumString(x, y) {
  const len = x === y ? x : randInt(x, y);
  let chars = [];
  for (let i = 0; i < len; ++i) {
    chars.push(randInt(0, 9) + 0x30);
  }
  return String.fromCharCode(...chars);
}

/**
 * Generates a random alphanumeric character code.
 * See TPC-C 4.3.2.2
 * @returns {number}
 */
export function randAlphaChar() {
  const n = randInt(0, 61);
  if (n < 26) {
    return n + 0x41;
  } else if (n < 52) {
    return n - 26 + 0x61;
  } else {
    return n - 52 + 0x30;
  }
}

/**
 * Generates a random item data string.
 * See TPC-C 4.3.3
 * @returns {string}
 */
export function randItemData() {
  let itemData = randAlphaString(26, 50);
  if (randFloat(0, 1) < 0.1) {
    const x = randInt(0, itemData.length - 8);
    itemData = itemData.slice(0, x) + 'ORIGINAL' + itemData.slice(x + 8);
  }
  return itemData;
}

/**
 * Generates a random zip code.
 * See TPC-C 4.3.2.7
 * @returns {string}
 */
export function randZipCode() {
  return randNumString(4, 4) + '11111';
}

/**
 * Generates a random customer last name.
 * See TPC-C 4.3.2.3 (page 64) and 4.3.3.1 (page 67)
 * @returns {string}
 */
export function randLastName(index) {
  const num = index < 1000 ? index : nurand(c_customerLastName, 255, 0, 999);
  const a = syllables[(num / 100) | 0];
  const b = syllables[(num / 10) % 10 | 0];
  const c = syllables[num % 10 | 0];
  return `${a}${b}${c}`;
}

/**
 * Pick a uniform random integer between x and y inclusive.
 * See TPC-C 4.3.2.5
 *
 * @param {number} x lower bound (inclusive)
 * @param {number} y upper bound (inclusive)
 * @returns {number}
 */
export function randInt(x, y) {
  return random.int(x, y);
}

/**
 * Pick a uniform random number between x and y.
 * Used for probabilistic distributions.
 *
 * @param {number} x lower bound (inclusive)
 * @param {number} y upper bound (exclusive)
 * @returns {number}
 */
export function randFloat(x, y) {
  return random.float(x, y);
}

/**
 * Pick a uniform random number between x and y inclusive.
 * See TPC-C 4.3.2.5
 *
 * @param {number} x lower bound (inclusive)
 * @param {number} y upper bound (inclusive)
 * @param {number} step the difference between adjacent numbers
 * @returns {number}
 */
export function randNumber(x, y, step) {
  return random.int(x / step, y / step) * step;
}

/**
 * Pick a non-uniform random integer between x and y inclusive.
 *
 * See TPC-C 2.1.6 for more details.
 *
 * @param {number} c runtime-constant randomly chosen within [0 .. a]
 * @param {number} a runtime-constant chosen according to the size of the range [x .. y]
 * @param {number} x lower bound (inclusive)
 * @param {number} y upper bound (inclusive)
 * @returns {number}
 */
function nurand(c, a, x, y) {
  const r1 = random.int(0, a);
  const r2 = random.int(x, y);
  return (((r1 | r2) + c) % (y - x + 1)) + x;
}
