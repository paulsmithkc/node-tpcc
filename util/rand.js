import random from 'random';

/**
 * Pick a uniform random integer between x and y inclusive.
 * 
 * @param {number} x lower bound (inclusive)
 * @param {number} y upper bound (inclusive)
 * @returns {number}
 */
export function rand(x, y) {
  return random.int((min = x), (max = y));
}

/**
 * Pick a non-uniform random integer between x and y inclusive.
 * 
 * See TPC-C 2.1.6 for more details.
 * 
 * @param {number} a runtime-constant chosen according to the size of the range [x .. y]
 * @param {number} c runtime-constant randomly chosen within [0 .. a]
 * @param {number} x lower bound (inclusive)
 * @param {number} y upper bound (inclusive)
 * @returns {number}
 */
export function nurand(a, c, x, y) {
  const r1 = random.int((min = 0), (max = a));
  const r2 = random.int((min = x), (max = y));
  return ((r1 | r2) + c) % (y - x + 1) + x;
}
