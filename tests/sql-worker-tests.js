// sql-worker-tests.js
const assert = require('assert');
const SQLAdapter = require('../sqlAdapter');
const ConnectionRegistry = require('../connectionRegistry');

describe('SQL Worker Adapter Tests', () => {
    [
        ConnectionRegistry.POSTGRESQL,
        ConnectionRegistry.MYSQL,
        ConnectionRegistry.SQLSERVER
    ].forEach(dbType => {
        describe(`${dbType} Worker Tests`, () => {
            let db;

            before(async function () {
                this.timeout(10000);
                try {
                    const isAvailable = await ConnectionRegistry.testConnection(dbType);
                    if (!isAvailable) {
                        console.warn(`Warning: Could not connect to ${dbType}. Skipping tests.`);
                        this.skip();
                        return;
                    }

                    db = new SQLAdapter(dbType);
                } catch (err) {
                    console.error(`Failed to initialize ${dbType}:`, err);
                    this.skip();
                }
            });

            after(async function () {
                if (db) {
                    await db.close();
                }
            });

            beforeEach(async function () {
                this.timeout(10000);
                if (!db) return;

                try {
                    const collections = await db.getCollections();
                    for (const collection of collections) {
                        await db.removeCollection(collection);
                    }
                } catch (err) {
                    console.error('Error in beforeEach:', err);
                }
            });

            describe('Collection Management', () => {
                it('should create a new collection', async () => {
                    await db.createCollection('test_collection', ['field1', 'field2']);
                    const collections = await db.getCollections();
                    assert(collections.includes('test_collection'));
                });

                it('should handle collection creation with no indices', async () => {
                    await db.createCollection('test_collection', []);
                    const collections = await db.getCollections();
                    assert(collections.includes('test_collection'));
                });

                it('should remove a collection', async () => {
                    await db.createCollection('test_collection', []);
                    await db.removeCollection('test_collection');
                    const collections = await db.getCollections();
                    assert(!collections.includes('test_collection'));
                });
            });

            describe('Record Operations', () => {
                beforeEach(async () => {
                    await db.createCollection('test_records', []);
                });

                it('should insert a record', async () => {
                    const record = {name: 'Test', value: 123};
                    const result = await db.insertRecord('test_records', 'key1', record);
                    assert.strictEqual(result.name, 'Test');
                    assert.strictEqual(result.value, 123);
                });

                it('should handle insert of complex objects', async () => {
                    const record = {
                        name: 'Test',
                        nested: {key: 'value'},
                        array: [1, 2, 3]
                    };
                    const result = await db.insertRecord('test_records', 'key1', record);
                    assert.deepStrictEqual(result.nested, {key: 'value'});
                    assert.deepStrictEqual(result.array, [1, 2, 3]);
                });

                it('should update a record', async () => {
                    const record = {name: 'Test', value: 123};
                    const updatedRecord = {name: 'Updated', value: 456};

                    await db.insertRecord('test_records', 'key1', record);
                    const result = await db.updateRecord('test_records', 'key1', updatedRecord);

                    assert.strictEqual(result.name, 'Updated');
                    assert.strictEqual(result.value, 456);
                });

                it('should delete a record', async () => {
                    const record = {name: 'Test', value: 123};
                    await db.insertRecord('test_records', 'key1', record);
                    await db.deleteRecord('test_records', 'key1');
                    const result = await db.getRecord('test_records', 'key1');
                    assert.strictEqual(result, null);
                });

                it('should get one record', async () => {
                    const record = {name: 'Test', value: 123};
                    await db.insertRecord('test_records', 'key1', record);
                    const result = await db.getOneRecord('test_records');
                    assert.strictEqual(result.name, 'Test');
                    assert.strictEqual(result.value, 123);
                });

                it('should get all records', async () => {
                    const records = [
                        {name: 'Test1', value: 123},
                        {name: 'Test2', value: 456}
                    ];

                    await db.insertRecord('test_records', 'key1', records[0]);
                    await db.insertRecord('test_records', 'key2', records[1]);

                    const results = await db.getAllRecords('test_records');
                    assert.strictEqual(results.length, 2);
                    assert(results.some(r => r.name === 'Test1' && r.value === 123));
                    assert(results.some(r => r.name === 'Test2' && r.value === 456));
                });
            });

            describe('Filter Operations', () => {
                beforeEach(async () => {
                    await db.createCollection('test_filters', ['score']);
                    const records = [
                        {score: 10, name: 'Alice'},
                        {score: 20, name: 'Bob'},
                        {score: 30, name: 'Charlie'}
                    ];

                    for (let i = 0; i < records.length; i++) {
                        await db.insertRecord('test_filters', `key${i}`, records[i]);
                    }
                });

                it('should filter records with conditions', async function () {
                    const results = await db.filter('test_filters', ['score >= 20']);
                    assert.strictEqual(results.length, 2);
                    assert(results.every(r => r.score >= 20));
                });

                it('should sort filtered results', async () => {
                    const results = await db.filter('test_filters', [], 'desc');
                    assert.strictEqual(results.length, 3);
                    for (let i = 1; i < results.length; i++) {
                        assert(results[i - 1].__timestamp >= results[i].__timestamp);
                    }
                });

                it('should limit filtered results', async () => {
                    const results = await db.filter('test_filters', [], 'asc', 2);
                    assert.strictEqual(results.length, 2);
                });
            });

            describe('Queue Operations', () => {
                const queueName = 'test_queue';

                beforeEach(async () => {
                    await db.createCollection(queueName, []);
                });

                it('should add items to queue', async () => {
                    const item = {data: 'test data'};
                    const pk = await db.addInQueue(queueName, item, true);
                    assert(pk);
                });

                it('should get queue size', async () => {
                    const items = [
                        {data: 'item1'},
                        {data: 'item2'}
                    ];

                    await Promise.all(items.map(item =>
                        db.addInQueue(queueName, item, true)
                    ));

                    const size = await db.queueSize(queueName);
                    assert.strictEqual(size, 2);
                });

                it('should list queue items', async () => {
                    const items = [
                        {data: 'item1'},
                        {data: 'item2'}
                    ];

                    await Promise.all(items.map(item =>
                        db.addInQueue(queueName, item, true)
                    ));

                    const list = await db.listQueue(queueName, 'asc');
                    assert.strictEqual(list.length, 2);
                });

                it('should get and delete queue items', async () => {
                    const item = {data: 'test data'};
                    const pk = await db.addInQueue(queueName, item, true);

                    const queueItem = await db.getObjectFromQueue(queueName, pk);
                    assert.strictEqual(queueItem.data, 'test data');

                    await db.deleteObjectFromQueue(queueName, pk);
                    const deletedItem = await db.getObjectFromQueue(queueName, pk);
                    assert.strictEqual(deletedItem, null);
                });
            });

            describe('Key-Value Operations', () => {
                it('should write and read string value', async () => {
                    await db.writeKey('testKey', 'testValue');
                    const result = await db.readKey('testKey');
                    assert.strictEqual(result.type, 'string');
                    assert.strictEqual(result.value, 'testValue');
                });

                it('should write and read object value', async () => {
                    const testObj = {foo: 'bar'};
                    await db.writeKey('testKey', testObj);
                    const result = await db.readKey('testKey');
                    assert.strictEqual(result.type, 'object');
                    assert.deepStrictEqual(JSON.parse(result.value), testObj);
                });

                it('should write and read buffer value', async () => {
                    const testBuffer = Buffer.from('test');
                    await db.writeKey('testKey', testBuffer);
                    const result = await db.readKey('testKey');
                    assert.strictEqual(result.type, 'buffer');
                    assert.strictEqual(result.value, testBuffer.toString());
                });

                it('should handle reading non-existent keys', async () => {
                    const result = await db.readKey('nonExistentKey');
                    assert.strictEqual(result, null);
                });
            });

            describe('Error Handling', () => {
                it('should handle invalid table names', async () => {
                    try {
                        await db.createCollection('invalid.table', []);
                        assert.fail('Should have thrown an error');
                    } catch (error) {
                        assert(error instanceof Error);
                    }
                });

                it('should handle worker failures gracefully', async () => {
                    try {
                        await db.executeWorkerTask('nonexistentMethod', []);
                        assert.fail('Should have thrown an error');
                    } catch (error) {
                        assert(error instanceof Error);
                    }
                });
            });
        });
    });
});