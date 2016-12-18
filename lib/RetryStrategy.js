// No-op retry strategy
class RetryStrategy {
  execute(fn, callback) { fn(callback); }
}

module.exports = RetryStrategy;
