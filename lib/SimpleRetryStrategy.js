const RetryStrategy = require('./RetryStrategy');

// Retries an async function a fixed number of times with no special
// back-off or error checking logic.
class SimpleRetryStrategy extends RetryStrategy {
  constructor(times) {
    super();
    this.times = times;
    this.attempts = 0;
  }

  execute(fn, callback) {
    var self = this;
    self.attempts++;
    fn((err, res) => {
      if (err && self.attempts == self.times)
        callback(err);
      else if (err)
        setTimeout(() => execute(fn, callback), 0);
      else
        callback(null, res);
    });
  }
}

module.exports = SimpleRetryStrategy;
