const assert = require('assert');
const SQLAdapter = require('../sqlAdapter');
const ConnectionRegistry = require('../connectionRegistry');

describe('PostgreSQL Adapter Tests', () => {
    let db;
    let connection;

    before(async function () {
        this.timeout(10000);
        try {
            const isAvailable = await ConnectionRegistry.testConnection(ConnectionRegistry.POSTGRESQL);
            if (!isAvailable) {
                console.warn('Warning: Could not connect to PostgreSQL. Skipping tests.');
                this.skip();
                return;
            }

            connection = await ConnectionRegistry.createConnection(ConnectionRegistry.POSTGRESQL);
            db = new SQLAdapter(ConnectionRegistry.POSTGRESQL, connection);
            await db.initPromise;
        } catch (err) {
            console.error('Failed to initialize PostgreSQL:', err);
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
            const collections = await new Promise((resolve, reject) => {
                db.getCollections((err, cols) => {
                    if (err) reject(err);
                    else resolve(cols || []);
                });
            });

            for (const collection of collections) {
                await new Promise((resolve, reject) => {
                    db.removeCollection(collection, (err) => {
                        if (err) reject(err);
                        else resolve();
                    });
                });
            }
        } catch (err) {
            console.error('Error in beforeEach:', err);
        }
    });

    describe('Collection Management', () => {
        it('should create a new collection', function (done) {
            this.timeout(10000);
            db.createCollection('test_collection', [], (err) => {
                if (err) return done(err);
                db.getCollections((err, collections) => {
                    if (err) return done(err);
                    try {
                        assert(collections.includes('test_collection'));
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });
        });

        it('should remove a collection', function (done) {
            this.timeout(10000);
            db.createCollection('test_collection', [], (err) => {
                if (err) return done(err);
                db.removeCollection('test_collection', (err) => {
                    if (err) return done(err);
                    db.getCollections((err, collections) => {
                        if (err) return done(err);
                        try {
                            assert(!collections.includes('test_collection'));
                            done();
                        } catch (e) {
                            done(e);
                        }
                    });
                });
            });
        });
    });

    describe('Record Operations', () => {
        beforeEach(function (done) {
            this.timeout(10000);
            db.createCollection('test_records', [], (err) => {
                if (err) return done(err);
                done();
            });
        });

        it('should insert a record', function (done) {
            const record = {name: 'Test', value: 123};
            db.insertRecord('test_records', 'key1', record, (err, result) => {
                if (err) return done(err);
                try {
                    assert.strictEqual(result.name, 'Test');
                    assert.strictEqual(result.value, 123);
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should handle insert of complex objects', function (done) {
            const record = {
                name: 'Test',
                nested: {key: 'value'},
                array: [1, 2, 3]
            };
            db.insertRecord('test_records', 'key1', record, (err, result) => {
                if (err) return done(err);
                try {
                    assert.deepStrictEqual(result.nested, {key: 'value'});
                    assert.deepStrictEqual(result.array, [1, 2, 3]);
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should not allow duplicate primary keys', function (done) {
            this.timeout(10000);
            const record = {name: 'Test', value: 123};
            db.insertRecord('test_records', 'key1', record, (err) => {
                if (err) return done(err);
                db.insertRecord('test_records', 'key1', record, (err) => {
                    try {
                        assert(err instanceof Error);
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });
        });

        it('should update a record', function (done) {
            this.timeout(10000);
            const record = {name: 'Test', value: 123};
            const updatedRecord = {name: 'Updated', value: 456};

            db.insertRecord('test_records', 'key1', record, (err) => {
                if (err) return done(err);
                db.updateRecord('test_records', 'key1', updatedRecord, (err, result) => {
                    if (err) return done(err);
                    try {
                        assert.strictEqual(result.name, 'Updated');
                        assert.strictEqual(result.value, 456);
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });
        });

        it('should delete a record', function (done) {
            this.timeout(10000);
            const record = {name: 'Test', value: 123};
            db.insertRecord('test_records', 'key1', record, (err) => {
                if (err) return done(err);
                db.deleteRecord('test_records', 'key1', (err, result) => {
                    if (err) return done(err);
                    try {
                        assert(result);
                        db.getRecord('test_records', 'key1', (err, getResult) => {
                            if (err) return done(err);
                            assert.strictEqual(getResult, null);
                            done();
                        });
                    } catch (e) {
                        done(e);
                    }
                });
            });
        });
    });

    describe('Filter Operations', () => {
        beforeEach(function (done) {
            this.timeout(10000);
            db.createCollection('test_filters', [], (err) => {
                if (err) return done(err);

                // Insert test records with various data types
                const records = [
                    {name: 'Alice', score: 10, active: true, tags: ['user', 'admin']},
                    {name: 'Bob', score: 20, active: false, tags: ['user']},
                    {name: 'Charlie', score: 30, active: true, tags: ['guest']},
                    {name: 'David', score: null, active: null, description: 'test user'},
                ];

                let completed = 0;
                records.forEach((record, index) => {
                    db.insertRecord('test_filters', `key${index}`, record, (err) => {
                        if (err) return done(err);
                        completed++;
                        if (completed === records.length) {
                            done();
                        }
                    });
                });
            });
        });

        it('should filter numeric values', function (done) {
            db.filter('test_filters', ['score > 15'], 'asc', null, (err, results) => {
                if (err) return done(err);
                try {
                    assert.strictEqual(results.length, 2);
                    assert(results.every(r => r.score > 15));
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should filter string values', function (done) {
            db.filter('test_filters', ["name = 'Alice'"], 'asc', null, (err, results) => {
                if (err) return done(err);
                try {
                    assert.strictEqual(results.length, 1);
                    assert.strictEqual(results[0].name, 'Alice');
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should handle LIKE operator case-insensitively', function (done) {
            db.filter('test_filters', ["name LIKE '%LIC%'"], 'asc', null, (err, results) => {
                if (err) return done(err);
                try {
                    assert.strictEqual(results.length, 1);
                    assert.strictEqual(results[0].name, 'Alice');
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should filter boolean values', function (done) {
            db.filter('test_filters', ["active = true"], 'asc', null, (err, results) => {
                if (err) return done(err);
                try {
                    assert.strictEqual(results.length, 2);
                    assert(results.every(r => r.active === true));
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should handle NULL values', function (done) {
            db.filter('test_filters', ["score IS NULL"], 'asc', null, (err, results) => {
                if (err) return done(err);
                try {
                    assert.strictEqual(results.length, 1);
                    assert.strictEqual(results[0].name, 'David');
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should handle multiple conditions', function (done) {
            db.filter('test_filters', ['score > 15', "active = false"], 'asc', null, (err, results) => {
                if (err) return done(err);
                try {
                    assert.strictEqual(results.length, 1);
                    assert.strictEqual(results[0].name, 'Bob');
                    assert.strictEqual(results[0].score, 20);
                    assert.strictEqual(results[0].active, false);
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should sort filtered results', function (done) {
            db.filter('test_filters', ['score > 0'], 'desc', null, (err, results) => {
                if (err) return done(err);
                try {
                    assert.strictEqual(results.length, 3);
                    const scores = results.map(r => r.score);
                    assert(scores.every((score, index) =>
                            index === 0 || score <= scores[index - 1]),
                        'Results should be sorted by score in descending order'
                    );
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should limit filtered results', function (done) {
            db.filter('test_filters', ['score > 0'], 'asc', 2, (err, results) => {
                if (err) return done(err);
                try {
                    assert.strictEqual(results.length, 2);
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should handle invalid filter conditions gracefully', function (done) {
            db.filter('test_filters', ['invalid condition'], 'asc', null, (err) => {
                try {
                    assert(err instanceof Error);
                    assert(err.message.includes('Invalid condition structure'));
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });
    });

    describe('Queue Operations', () => {
        const queueName = 'test_queue';

        beforeEach(function (done) {
            this.timeout(10000);
            db.createCollection(queueName, [], done);
        });

        it('should add items to queue', function (done) {
            const item = {data: 'test data'};
            db.addInQueue(queueName, item, true, (err, pk) => {
                if (err) return done(err);
                try {
                    assert(pk);
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should get queue size', function (done) {
            this.timeout(10000);
            const items = [
                {data: 'item1'},
                {data: 'item2'}
            ];

            let completed = 0;
            items.forEach(item => {
                db.addInQueue(queueName, item, true, (err) => {
                    if (err) return done(err);
                    completed++;
                    if (completed === items.length) {
                        db.queueSize(queueName, (err, size) => {
                            if (err) return done(err);
                            try {
                                assert.strictEqual(size, 2);
                                done();
                            } catch (e) {
                                done(e);
                            }
                        });
                    }
                });
            });
        });

        it('should list queue items', function (done) {
            this.timeout(10000);
            const items = [
                {data: 'item1'},
                {data: 'item2'}
            ];

            let completed = 0;
            items.forEach(item => {
                db.addInQueue(queueName, item, true, (err) => {
                    if (err) return done(err);
                    completed++;
                    if (completed === items.length) {
                        db.listQueue(queueName, 'asc', null, (err, list) => {
                            if (err) return done(err);
                            try {
                                assert.strictEqual(list.length, 2);
                                done();
                            } catch (e) {
                                done(e);
                            }
                        });
                    }
                });
            });
        });

        it('should handle queue with unique items', function (done) {
            const item = {data: 'unique item'};
            db.addInQueue(queueName, item, true, (err, pk1) => {
                if (err) return done(err);
                db.addInQueue(queueName, item, true, (err, pk2) => {
                    if (err) return done(err);
                    try {
                        assert.notStrictEqual(pk1, pk2);
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });
        });
    });

    describe('Key-Value Operations', function () {
        this.timeout(20000);

        beforeEach(async function () {
            await db.initPromise;
        });

        it('should write and read string value', function (done) {
            db.writeKey('testKey', 'testValue', function (err) {
                if (err) return done(err);
                db.readKey('testKey', function (err, result) {
                    if (err) return done(err);
                    try {
                        assert.strictEqual(result.type, 'string');
                        assert.strictEqual(result.value, 'testValue');
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });
        });

        it('should write and read object value', function (done) {
            const testObj = {foo: 'bar'};
            db.writeKey('testKey', testObj, function (err) {
                if (err) return done(err);
                db.readKey('testKey', function (err, result) {
                    if (err) return done(err);
                    try {
                        assert.strictEqual(result.type, 'object');
                        const parsedValue = JSON.parse(result.value);
                        assert.deepStrictEqual(parsedValue, testObj);
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });
        });

        it('should write and read buffer value', function (done) {
            const testBuffer = Buffer.from('test');
            db.writeKey('testKey', testBuffer, function (err) {
                if (err) return done(err);
                db.readKey('testKey', function (err, result) {
                    if (err) return done(err);
                    try {
                        assert.strictEqual(result.type, 'buffer');
                        assert.strictEqual(result.value, testBuffer.toString());
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });
        });

        it('should handle overwriting existing keys', function (done) {
            db.writeKey('testKey', 'initialValue', function (err) {
                if (err) return done(err);
                db.writeKey('testKey', 'updatedValue', function (err) {
                    if (err) return done(err);
                    db.readKey('testKey', function (err, result) {
                        if (err) return done(err);
                        try {
                            assert.strictEqual(result.type, 'string');
                            assert.strictEqual(result.value, 'updatedValue');
                            done();
                        } catch (e) {
                            done(e);
                        }
                    });
                });
            });
        });

        it('should handle reading non-existent keys', function (done) {
            db.readKey('nonExistentKey', function (err, result) {
                if (err) return done(err);
                try {
                    assert.strictEqual(result, null);
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });
    });

    describe('Error Handling', () => {
        it('should handle invalid table names', function (done) {
            db.createCollection('invalid.table', [], (err) => {
                try {
                    assert(err instanceof Error);
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should handle invalid filter conditions', function (done) {
            db.createCollection('test_errors', [], (err) => {
                if (err) return done(err);
                db.filter('test_errors', ['invalid syntax'], 'asc', null, (err) => {
                    try {
                        assert(err instanceof Error);
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });
        });
    });

    describe('Additional Collection Operations', () => {
        beforeEach(function (done) {
            this.timeout(10000);
            db.createCollection('test_additional', [], (err) => {
                if (err) return done(err);
                done();
            });
        });

        it('should add an index to existing collection', function (done) {
            db.addIndex('test_additional', 'test_field', (err) => {
                if (err) return done(err);
                // Index creation success is implied if no error
                done();
            });
        });

        it('should get one record from collection', function (done) {
            const record = {name: 'Test', value: 123};
            db.insertRecord('test_additional', 'key1', record, (err) => {
                if (err) return done(err);
                db.getOneRecord('test_additional', (err, result) => {
                    if (err) return done(err);
                    try {
                        assert.strictEqual(result.name, 'Test');
                        assert.strictEqual(result.value, 123);
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });
        });

        it('should get all records from collection', function (done) {
            const records = [
                {name: 'Test1', value: 123},
                {name: 'Test2', value: 456}
            ];

            let completed = 0;
            records.forEach((record, index) => {
                db.insertRecord('test_additional', `key${index}`, record, (err) => {
                    if (err) return done(err);
                    completed++;
                    if (completed === records.length) {
                        db.getAllRecords('test_additional', (err, results) => {
                            if (err) return done(err);
                            try {
                                assert.strictEqual(results.length, 2);
                                assert(results.some(r => r.name === 'Test1' && r.value === 123));
                                assert(results.some(r => r.name === 'Test2' && r.value === 456));
                                done();
                            } catch (e) {
                                done(e);
                            }
                        });
                    }
                });
            });
        });

        it('should remove collection asynchronously', async function () {
            try {
                await db.removeCollectionAsync('test_additional');
                const collections = await new Promise((resolve, reject) => {
                    db.getCollections((err, cols) => {
                        if (err) reject(err);
                        else resolve(cols);
                    });
                });
                assert(!collections.includes('test_additional'));
            } catch (err) {
                throw err;
            }
        });
    });

    describe('Database State Management', () => {
        it('should handle refresh operation', function (done) {
            db.refresh((err) => {
                if (err) return done(err);
                done();
            });
        });

        it('should handle async refresh operation', async function () {
            try {
                await db.refreshAsync();
                // Success is implied if no error is thrown
            } catch (err) {
                throw err;
            }
        });

        it('should handle database save operation', function (done) {
            db.saveDatabase((err, result) => {
                if (err) return done(err);
                try {
                    assert.strictEqual(result.message, 'Database saved');
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });
    });
});