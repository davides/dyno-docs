const async_ = require('async');
const assert = require('assert');
const RetryStrategy = require('../lib/RetryStrategy');
const DynoDocsClient = require('../lib/DynoDocsClient');
const AWS = require('aws-sdk');

AWS.config.update({ region: 'us-east-1' });
var dynamo = new AWS.DynamoDB();

describe('DynoDocsClient', function() {
  var table = 'dynodocstest';
  var pk = 'hashkey';
  var rk = 'rangekey';
  var client = new DynoDocsClient(new AWS.DynamoDB.DocumentClient(), table, pk, rk);

  function getKey(client, item) {
    var key = {};
    key[pk] = item[pk];
    key[rk] = item[rk];
    return key;
  }

  function makeItem(pkval, rkval, val) {
    var item = {};
    item[pk] = pkval;
    item[rk] = rkval;
    item[val] = val;
    return item;
  }

  before(function(done) {
    dynamo.scan({ TableName: table },
      (err, res) => {
        if (err) return done(err);
        async_.each(res.items,
          (item, cb) => { dynamo.deleteItem({ Key: getKey(item) }, cb); },
          (err) => { done(err); }
        );
      }
    );
  });

  describe('#put, #get', function() {
    it('returns a stored item', function(done) {
      var item = makeItem('h1', 'r1', 1);
      client.put(item, (err, res) => {
        if (err) return done(err);
        client.get('h1', 'r1', (err, res) => {
          if (err) return done(err);
          assert.equal(res.val, item.val);
          done();
        });
      });
    });
  });

  describe('#put', function() {
    it('preserves consistency when incorrect etag is provided', function(done) {
      var item = makeItem('h2', 'r2', 2);
      client.put(item, (err, res) => {
        // trying to put again with an incorrect version should not be allowed
        item.etag = 'incorrect-etag';
        client.put(item, (err, res) => {
          if (err) {
            assert.equal(err.code, 'ConditionalCheckFailedException', 'Unexpected error: ' + err);
            done();
          } else {
            done(new Error('Update should not have been allowed with wrong etag.'));
          }
        });
      });
    });

    it('forces update when etag is asterisk', function(done) {
      var item = makeItem('h3', 'r3', 3);
      client.put(item, (err, res) => {
        // it can be overwritten with a wildcard etag
        item.etag = '*';
        client.put(item, done);
      });
    });

    it('allows updates when correct etag is provided', function(done) {
      var item = makeItem('h4', 'r4', 4);
      client.put(item, (err, res) => {
        // using the correct etag should work
        item['$$etag'] = res['$$etag'];
        client.put(item, done);
      });
    });
  });

  describe('#put (with retry)', function() {
    class TestStrategy extends RetryStrategy {
      constructor(seam) { super(); this.seam = seam; }
      execute(fn, callback) { this.seam(fn, callback); }
    }

    it('uses provided retry strategy', function(done) {
      var item = makeItem('h5', 'r5', 5);
      var retryStrategy = new TestStrategy((fn, cb) => done());
      client.put(item, retryStrategy, (err, res) => {});
    });
  });
});
