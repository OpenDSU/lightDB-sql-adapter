const syndicate = require('syndicate');
const {isMainThread, parentPort} = require('worker_threads');
const {StrategyFactory} = require('./strategyFactory');

if (isMainThread) {
    class SQLAdapter {
        constructor(type) {
            this.type = type;
            this.READ_WRITE_KEY_TABLE = "KeyValueTable";
            this.debug = process.env.DEBUG === 'true';

            // Create a worker pool
            this.workerPool = syndicate.createWorkerPool({
                bootScript: __filename,
                maximumNumberOfWorkers: 4,
                workerOptions: {
                    workerData: {type: this.type}
                }
            });
        }

        async close() {
            try {
                // Check if pool cleanup methods exist before calling
                if (this.workerPool && typeof this.workerPool.drain === 'function') {
                    await this.workerPool.drain();
                }
                if (this.workerPool && typeof this.workerPool.clear === 'function') {
                    await this.workerPool.clear();
                }
                if (this.workerPool && typeof this.workerPool.terminate === 'function') {
                    await this.workerPool.terminate();
                }
            } catch (error) {
                console.error('Error closing worker pool:', error);
                throw error;
            }
        }

        _executeTask(taskName, args) {
            return new Promise((resolve, reject) => {
                this.workerPool.addTask({taskName, args}, (err, result) => {
                    if (err) reject(err);
                    else resolve(result.success ? result.result : Promise.reject(result.error));
                });
            });
        }

        refresh(callback) {
            this._executeTask('refresh', [])
                .then(() => callback())
                .catch(error => callback(error));
        }

        async refreshAsync() {
            return this._executeTask('refresh', []);
        }

        saveDatabase(callback) {
            this._executeTask('saveDatabase', [])
                .then(() => callback(undefined, {message: "Database saved"}))
                .catch(error => callback(error));
        }

        async saveDatabaseAsync() {
            await this._executeTask('saveDatabase', []);
            return {message: "Database saved"};
        }

        async count(tableName) {
            return this._executeTask('count', [tableName]);
        }

        getCollections(callback) {
            this._executeTask('getCollections', [])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        createCollection(tableName, indicesList, callback) {
            this._executeTask('createCollection', [tableName, indicesList])
                .then(result => callback(null, result))
                .catch(error => {
                    // Convert raw error to Error instance
                    const err = error instanceof Error ? error : new Error(error.message);
                    err.isTestError = error.isTestError;
                    callback(err);
                });
        }

        removeCollection(tableName, callback) {
            this._executeTask('removeCollection', [tableName])
                .then(() => callback())
                .catch(error => callback(error));
        }

        async removeCollectionAsync(tableName) {
            return this._executeTask('removeCollectionAsync', [tableName]);
        }

        addIndex(tableName, property, callback) {
            this._executeTask('addIndex', [tableName, property])
                .then(() => callback())
                .catch(error => callback(error));
        }

        getOneRecord(tableName, callback) {
            this._executeTask('getOneRecord', [tableName])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        getAllRecords(tableName, callback) {
            this._executeTask('getAllRecords', [tableName])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        insertRecord(tableName, pk, record, callback) {
            this._executeTask('insertRecord', [tableName, pk, record])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        updateRecord(tableName, pk, record, callback) {
            this._executeTask('updateRecord', [tableName, pk, record])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        deleteRecord(tableName, pk, callback) {
            this._executeTask('deleteRecord', [tableName, pk])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        getRecord(tableName, pk, callback) {
            this._executeTask('getRecord', [tableName, pk])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        filter(tableName, filterConditions = [], sort = 'asc', max = null, callback) {
            this._executeTask('filter', [tableName, filterConditions, sort, max])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        addInQueue(queueName, object, ensureUniqueness = false, callback) {
            this._executeTask('addInQueue', [queueName, object, ensureUniqueness])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        queueSize(queueName, callback) {
            this.count(queueName)
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        listQueue(queueName, sortAfterInsertTime = 'asc', onlyFirstN = null, callback) {
            this._executeTask('listQueue', [queueName, sortAfterInsertTime, onlyFirstN])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        getObjectFromQueue(queueName, hash, callback) {
            this._executeTask('getObjectFromQueue', [queueName, hash])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        deleteObjectFromQueue(queueName, hash, callback) {
            this._executeTask('deleteObjectFromQueue', [queueName, hash])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        writeKey(key, value, callback) {
            let valueObject = {
                type: typeof value,
                value: value
            };

            if (Buffer.isBuffer(value)) {
                valueObject = {
                    type: "buffer",
                    value: value.toString()
                };
            } else if (value !== null && typeof value === "object") {
                valueObject = {
                    type: "object",
                    value: JSON.stringify(value)
                };
            }

            this._executeTask('writeKey', [key, valueObject])
                .then(result => callback(null, result))
                .catch(error => callback(error));
        }

        readKey(key, callback) {
            this._executeTask('readKey', [key])
                .then(result => {
                    if (!result) {
                        return callback(null, null);
                    }
                    const parsed = typeof result === 'string' ? JSON.parse(result) : result;
                    callback(null, parsed);
                })
                .catch(error => callback(error));
        }
    }

    module.exports = SQLAdapter;

} else {
    // Worker code
    const {workerData} = require('worker_threads');
    const {StrategyFactory} = require('./strategyFactory');
    const strategy = StrategyFactory.createStrategy(workerData.type);

    // Create a pool at worker initialization
    const ConnectionRegistry = require('./connectionRegistry');
    let pool = null;

    async function initializePool() {
        try {
            pool = await ConnectionRegistry.createConnection(workerData.type);
            if (process.env.DEBUG) {
                console.log('Pool initialized:', pool ? 'yes' : 'no');
                console.log('Pool type:', pool ? Object.getPrototypeOf(pool).constructor.name : 'N/A');
            }
            return pool;
        } catch (error) {
            console.error('Pool initialization error:', error);
            throw error;
        }
    }

    parentPort.on('message', async (message) => {
        const {taskName, args} = message;

        try {
            if (!pool) {
                await initializePool();
                await strategy.ensureCollectionsTable(pool);
            }

            let result;
            if (process.env.DEBUG) {
                console.log('Task:', taskName);
                console.log('Args:', args);
            }

            switch (taskName) {
                case 'refresh':
                    result = await strategy.refresh(pool);
                    break;

                case 'saveDatabase':
                    result = await strategy.saveDatabase(pool);
                    break;

                case 'count':
                    const countQuery = strategy.count(args[0]);
                    result = await strategy.executeQuery(pool, countQuery);
                    result = strategy.parseCountResult(result);
                    break;

                case 'getCollections':
                    const collectionsQuery = strategy.getCollections();
                    result = await strategy.executeQuery(pool, collectionsQuery);
                    result = strategy.parseCollectionsResult(result);
                    break;

                case 'createCollection':
                    try {
                        // Make sure collections table exists
                        await strategy.ensureCollectionsTable(pool);

                        const createQueries = strategy.createCollection(args[0], args[1]);
                        if (process.env.DEBUG) {
                            console.log('Create queries:', createQueries);
                        }

                        result = await strategy.executeTransaction(pool, createQueries);

                        // Then verify the collection exists
                        const collectionsQuery = strategy.getCollections();
                        const verifyResult = await strategy.executeQuery(pool, collectionsQuery);
                        const collections = strategy.parseCollectionsResult(verifyResult);

                        if (!collections.includes(args[0])) {
                            throw new Error(`Failed to create collection ${args[0]}`);
                        }

                        result = {
                            success: true,
                            message: `Collection ${args[0]} created`,
                            collections: collections
                        };
                    } catch (error) {
                        console.error('Collection creation error:', error);
                        throw error;
                    }
                    break;

                case 'removeCollection':
                    const removeQueries = strategy.removeCollection(args[0]);
                    result = await strategy.executeTransaction(pool, removeQueries);
                    result = serializeResult(result); // Serialize before sending
                    break;

                case 'addIndex':
                    const indexQuery = strategy.addIndex(args[0], args[1]);
                    result = await strategy.executeQuery(pool, indexQuery);
                    break;

                case 'getOneRecord':
                    const oneRecordQuery = strategy.getOneRecord(args[0]);
                    result = await strategy.executeQuery(pool, oneRecordQuery);
                    result = strategy.parseGetResult(result);
                    break;

                case 'getAllRecords':
                    const allRecordsQuery = strategy.getAllRecords(args[0]);
                    result = await strategy.executeQuery(pool, allRecordsQuery);
                    result = strategy.parseFilterResults(result);
                    break;

                case 'insertRecord':
                    try {
                        const timestamp = Date.now();
                        const insertQuery = strategy.insertRecord(args[0]);

                        if (process.env.DEBUG) {
                            console.log('Insert query:', insertQuery);
                            console.log('Parameters:', [args[1], JSON.stringify(args[2]), timestamp]);
                        }

                        result = await strategy.executeQuery(pool, insertQuery, [args[1], JSON.stringify(args[2]), timestamp]);

                        // Handle result serialization
                        const serializedResult = {
                            rows: result.rows.map(row => ({
                                pk: row.pk,
                                data: row.data,
                                __timestamp: row.__timestamp
                            })),
                            rowCount: result.rowCount,
                            command: result.command
                        };

                        result = strategy.parseInsertResult(serializedResult, args[1], args[2]);
                    } catch (error) {
                        // Convert postgres error into a proper Error object
                        if (error.code === '23505') { // Unique violation
                            throw new Error(`Record with key ${args[1]} already exists`);
                        }
                        throw error;
                    }
                    break;

                case 'updateRecord':
                    const updateTimestamp = Date.now();
                    const updateQuery = strategy.updateRecord(args[0]);
                    result = await strategy.executeQuery(pool, updateQuery, [args[1], JSON.stringify(args[2]), updateTimestamp]);
                    result = strategy.parseUpdateResult(result);
                    break;

                case 'deleteRecord':
                    const deleteQuery = strategy.deleteRecord(args[0]);
                    result = await strategy.executeQuery(pool, deleteQuery, [args[1]]);
                    result = strategy.parseDeleteResult(result, args[1]);
                    break;

                case 'getRecord':
                    const getQuery = strategy.getRecord(args[0]);
                    result = await strategy.executeQuery(pool, getQuery, [args[1]]);
                    result = strategy.parseGetResult(result);
                    break;

                case 'filter':
                    const [tableName, conditions, sort, max] = args;
                    let filterConditions = '';
                    if (conditions && conditions.length) {
                        filterConditions = strategy.convertConditionsToLokiQuery(conditions);
                    }
                    const sortConfig = {
                        field: '__timestamp',
                        direction: (sort === 'desc' ? 'DESC' : 'ASC')
                    };
                    const filterQuery = strategy.filter(tableName, filterConditions, sortConfig, max);
                    result = await strategy.executeQuery(pool, filterQuery);
                    result = strategy.parseFilterResults(result);
                    break;

                case 'addInQueue':
                    result = await strategy.addInQueue(args[0], args[1], args[2]);
                    break;

                case 'listQueue':
                    const [queueName, sortAfterInsertTime, onlyFirstN] = args;
                    const listQuery = strategy.listQueue(queueName, sortAfterInsertTime, onlyFirstN);
                    result = await strategy.executeQuery(pool, listQuery);
                    result = strategy.parseFilterResults(result).map(r => r.pk);
                    break;

                case 'getObjectFromQueue':
                    const getQueueQuery = strategy.getObjectFromQueue(args[0], args[1]);
                    result = await strategy.executeQuery(pool, getQueueQuery);
                    result = strategy.parseQueueResult(result);
                    break;

                case 'deleteObjectFromQueue':
                    const deleteQueueQuery = strategy.deleteObjectFromQueue(args[0], args[1]);
                    result = await strategy.executeQuery(pool, deleteQueueQuery);
                    result = strategy.parseQueueResult(result);
                    break;

                case 'writeKey':
                    try {
                        // Ensure table exists
                        await strategy.ensureKeyValueTable(pool);

                        const writeQuery = strategy.writeKey();
                        console.log('Write key query:', writeQuery);

                        result = await strategy.executeQuery(pool, writeQuery, [args[0], JSON.stringify(args[1]), Date.now()]);
                        result = strategy.parseWriteKeyResult(result);
                    } catch (error) {
                        console.error('Write key error:', error);
                        throw error;
                    }
                    break;

                case 'readKey':
                    try {
                        const readQuery = strategy.readKey();
                        console.log('Read key query:', readQuery);

                        result = await strategy.executeQuery(pool, readQuery, [args[0]]);
                        result = strategy.parseReadKeyResult(result);
                    } catch (error) {
                        console.error('Read key error:', error);
                        throw error;
                    }
                    break;

                default:
                    throw new Error(`Unknown task: ${taskName}`);
            }

            parentPort.postMessage({success: true, result});
        } catch (error) {
            // Convert error to a serializable format
            const serializedError = {
                message: error.message,
                stack: error.stack,
                code: error.code,
                constraint: error.constraint,
                detail: error.detail,
                // Only include these if testing db errors
                isTestError: true,
                type: 'DatabaseError'
            };

            parentPort.postMessage({
                success: false,
                error: serializedError
            });
        }
    });

    // Signal that the worker is ready
    parentPort.postMessage('ready');
}

function serializeResult(result) {
    if (!result) return null;

    // If it's an array of results (like from a transaction)
    if (Array.isArray(result)) {
        return result.map(r => ({
            rows: r.rows,
            rowCount: r.rowCount,
            command: r.command
        }));
    }

    // Single result
    return {
        rows: result.rows,
        rowCount: result.rowCount,
        command: result.command
    };
}