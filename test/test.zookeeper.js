'use strict';

var libPath = process.env['kafka-cov'] ? '../lib-cov/' : '../lib/',
    Zookeeper = require(libPath + 'zookeeper'),
    config = require('./config');

var zk;

/*
 *  To run the test, you should ensure:
 *  1. at least 2 broker running
 *  2. zookeeper host is set in config file
 *  3. create path kafka0.8 in zookeeper
 */

before(function () {
    zk = new Zookeeper(config.zoo);
});

describe('Zookeeper', function () {
    describe('when init success', function () {
        it('should emit init event', function (done) {
            var zk = new Zookeeper(config.zoo);
            zk.on('brokersChanged', function (brokers) {
                Object.keys(brokers).length.should.above(1);
                done();
            });
        });
    });

    describe('#topicExists', function () {
        it('should return false when topic not exist', function (done) {
            zk.topicExists('_not_exist_topic_test', function (existed, topic) {
                existed.should.not.be.ok;
                topic.should.equal('_not_exist_topic_test');
                done();
            });
        });
    });
});
