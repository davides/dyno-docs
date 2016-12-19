const AWS = require('aws-sdk');
const SimpleRetryStrategy = require('./SimpleRetryStrategy');
const ETag = '$$etag';

class DynoDocsClient {
  constructor(client, table, pk, rk) {
    this.table = table;
    this.pk = pk;
    this.rk = rk;
    this.client = client;
  }

  put(item, retryStrategy, callback) {
    if (!item) throw new Error(`item' is required.`);
    if (!retryStrategy && !callback) throw new Error(`'callback' is required.`);
    if (typeof retryStrategy == 'function') {
      callback = retryStrategy;
      retryStrategy = new SimpleRetryStrategy(1);
    }

    var self = this;

    retryStrategy.execute((cb) => {
      var params = {
        TableName: self.table,
        Item: Object.assign({}, item)
      };
      self._ensureSafeUpdate(params);
      self.client.put(params, cb);
    }, callback);
  }

  get(pk, rk, callback) {
    var params = {
      TableName: this.table,
      Key: this._key(pk, rk)
    };

    this.client.get(params, this._respond(callback, x => x.Item));
  }

  _respond(callback, project) {
    return (err, res) => callback(err, err ? null : project(res));
  }

  _key(pk, rk) {
    var key = {};
    key[this.pk] = pk;
    key[this.rk] = rk;
    return key;
  }

  _ensureSafeUpdate(params) {
    var requestVersion = params.Item[ETag];

    if (requestVersion == undefined || requestVersion == '*') {
      params.ConditionExpression = undefined;
    } else {
      params.ConditionExpression = '#etag = :etag';
      params.ExpressionAttributeNames = { '#etag': ETag };
      params.ExpressionAttributeValues = { ':etag': requestVersion };
    }

    params.Item[ETag] = this._version();
  }

  _version() { return (new Date()).toISOString(); }
}

module.exports = DynoDocsClient;
