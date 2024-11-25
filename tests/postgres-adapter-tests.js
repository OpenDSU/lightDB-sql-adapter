const assert = require('assert');
const {Pool} = require('pg');
const PostgreSQLDb = require('../sqlAdapter');

describe('PostgreSQL Adapter Tests', () => {
    let db;
    const connectionString = 'postgresql://postgres:password@localhost:5432/postgres';

    before(async function () {
        this.timeout(10000);
        db = new PostgreSQLDb(connectionString);
    });

    after(async function () {
        if (db) {
            await db.close();
        }
    });

    beforeEach(async () => {
        // Clean up test collections
        await new Promise((resolve) => {
            db.getCollections((err, collections) => {
                if (err) return resolve();
                let completed = 0;
                if (collections.length === 0) return resolve();

                collections.forEach(collection => {
                    db.removeCollection(collection, () => {
                        completed++;
                        if (completed === collections.length) {
                            resolve();
                        }
                    });
                });
            });
        });
    });

    describe('Collection Management', () => {
        it('should create a new collection', (done) => {
            db.createCollection('test_collection', ['field1', 'field2'], (err) => {
                assert.strictEqual(err, undefined);
                db.getCollections((err, collections) => {
                    assert.strictEqual(err, undefined);
                    assert(collections.includes('test_collection'));
                    done();
                });
            });
        });

        it('should remove a collection', (done) => {
            db.createCollection('test_collection', [], (err) => {
                assert.strictEqual(err, undefined);
                db.removeCollection('test_collection', (err) => {
                    assert.strictEqual(err, undefined);
                    db.getCollections((err, collections) => {
                        assert.strictEqual(err, undefined);
                        assert(!collections.includes('test_collection'));
                        done();
                    });
                });
            });
        });
    });

    describe('Record Operations', () => {
        beforeEach((done) => {
            db.createCollection('test_records', [], done);
        });

        it('should insert a record', (done) => {
            const record = {name: 'Test', value: 123};
            db.insertRecord('test_records', 'key1', record, (err, result) => {
                assert.strictEqual(err, null);
                assert.strictEqual(result.name, 'Test');
                assert.strictEqual(result.value, 123);
                done();
            });
        });

        it('should not allow duplicate primary keys', (done) => {
            const record = {name: 'Test', value: 123};
            db.insertRecord('test_records', 'key1', record, (err) => {
                assert.strictEqual(err, null);
                db.insertRecord('test_records', 'key1', record, (err) => {
                    assert(err instanceof Error);
                    done();
                });
            });
        });

        it('should update a record', (done) => {
            const record = {name: 'Test', value: 123};
            const updatedRecord = {name: 'Updated', value: 456};

            db.insertRecord('test_records', 'key1', record, (err) => {
                assert.strictEqual(err, null);
                db.updateRecord('test_records', 'key1', updatedRecord, (err, result) => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(result.name, 'Updated');
                    assert.strictEqual(result.value, 456);
                    done();
                });
            });
        });

        it('should delete a record', (done) => {
            const record = {name: 'Test', value: 123};
            db.insertRecord('test_records', 'key1', record, (err) => {
                assert.strictEqual(err, null);
                db.deleteRecord('test_records', 'key1', (err, result) => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(result.name, 'Test');
                    db.getRecord('test_records', 'key1', (err, result) => {
                        assert.strictEqual(err, null);
                        assert.strictEqual(result, null);
                        done();
                    });
                });
            });
        });
    });

    describe('Filter Operations', () => {
        beforeEach((done) => {
            db.createCollection('test_filters', [], (err) => {
                if (err) return done(err);

                const records = [
                    {score: 10, name: 'Alice'},
                    {score: 20, name: 'Bob'},
                    {score: 30, name: 'Charlie'}
                ];

                let completed = 0;
                records.forEach((record, index) => {
                    db.insertRecord('test_filters', `key${index}`, record, () => {
                        completed++;
                        if (completed === records.length) {
                            done();
                        }
                    });
                });
            });
        });

        it('should filter records with conditions', (done) => {
            db.filter('test_filters', ['score >= 20'], 'asc', null, (err, results) => {
                assert.strictEqual(err, null);
                assert.strictEqual(results.length, 2);
                assert(results.every(r => r.score >= 20));
                done();
            });
        });

        it('should sort filtered results', (done) => {
            db.filter('test_filters', [], 'desc', null, (err, results) => {
                assert.strictEqual(err, null);
                assert.strictEqual(results.length, 3);
                for (let i = 1; i < results.length; i++) {
                    assert(results[i - 1].__timestamp >= results[i].__timestamp);
                }
                done();
            });
        });

        it('should limit filtered results', (done) => {
            db.filter('test_filters', [], 'asc', 2, (err, results) => {
                assert.strictEqual(err, null);
                assert.strictEqual(results.length, 2);
                done();
            });
        });
    });

    describe('Queue Operations', () => {
        const queueName = 'test_queue';

        beforeEach((done) => {
            db.createCollection(queueName, [], done);
        });

        it('should add items to queue', (done) => {
            const item = {data: 'test data'};
            db.addInQueue(queueName, item, true, (err, pk) => {
                assert.strictEqual(err, null);
                assert(pk);
                done();
            });
        });

        it('should get queue size', (done) => {
            const items = [
                {data: 'item1'},
                {data: 'item2'}
            ];

            let completed = 0;
            items.forEach(item => {
                db.addInQueue(queueName, item, true, () => {
                    completed++;
                    if (completed === items.length) {
                        db.queueSize(queueName, (err, size) => {
                            assert.strictEqual(err, null);
                            assert.strictEqual(size, 2);
                            done();
                        });
                    }
                });
            });
        });

        it('should list queue items', (done) => {
            const items = [
                {data: 'item1'},
                {data: 'item2'}
            ];

            let completed = 0;
            items.forEach(item => {
                db.addInQueue(queueName, item, true, () => {
                    completed++;
                    if (completed === items.length) {
                        db.listQueue(queueName, 'asc', null, (err, list) => {
                            assert.strictEqual(err, null);
                            assert.strictEqual(list.length, 2);
                            done();
                        });
                    }
                });
            });
        });
    });

    describe('Key-Value Operations', function () {
        // Increase timeout for these specific tests
        this.timeout(20000);

        beforeEach(async function () {
            // Wait for initialization to complete
            await db.initPromise;
        });

        it('should write and read string value', function (done) {
            db.writeKey('testKey', 'testValue', function (err) {
                assert.strictEqual(err, null);
                db.readKey('testKey', function (err, result) {
                    assert.strictEqual(err, null);
                    assert.strictEqual(result.type, 'string');
                    assert.strictEqual(result.value, 'testValue');
                    done();
                });
            });
        });

        it('should write and read object value', function (done) {
            const testObj = {foo: 'bar'};
            db.writeKey('testKey', testObj, function (err) {
                assert.strictEqual(err, null);
                db.readKey('testKey', function (err, result) {
                    assert.strictEqual(err, null);
                    assert.strictEqual(result.type, 'object');
                    assert.deepStrictEqual(JSON.parse(result.value), testObj);
                    done();
                });
            });
        });

        it('should write and read buffer value', function (done) {
            const testBuffer = Buffer.from('test');
            db.writeKey('testKey', testBuffer, function (err) {
                assert.strictEqual(err, null);
                db.readKey('testKey', function (err, result) {
                    assert.strictEqual(err, null);
                    assert.strictEqual(result.type, 'buffer');
                    assert.strictEqual(result.value, testBuffer.toString());
                    done();
                });
            });
        });
    });
});