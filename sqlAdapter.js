// sqlAdapter.js
const syndicate = require('syndicate');
const {isMainThread, parentPort} = require('worker_threads');
const {StrategyFactory} = require('./strategyFactory');
const ConnectionRegistry = require('./connectionRegistry');


class SQLAdapter {
    constructor(type) {
        this.type = type;
        this.READ_WRITE_KEY_TABLE = "KeyValueTable";
        this.debug = process.env.DEBUG === 'true';

        this.workerPool = syndicate.createWorkerPool({
            bootScript: require("path").join(__dirname, "./workerScript.js"),
            maximumNumberOfWorkers: 4,
            workerOptions: {
                workerData: {
                    type: this.type,
                    config: ConnectionRegistry.DEFAULT_CONFIGS[this.type.toLowerCase()]
                }
            }
        });
    }

    async close() {
        try {
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
            try {
                // Sanitize args to ensure they're serializable
                const safeArgs = args.map(arg => {
                    if (arg === null || arg === undefined) return arg;
                    if (typeof arg === 'function') return null;
                    if (Buffer.isBuffer(arg)) return arg.toString('base64');
                    if (typeof arg === 'object') {
                        return JSON.parse(JSON.stringify(arg));
                    }
                    return arg;
                });

                // Include workerData in the message
                this.workerPool.addTask({
                    taskName,
                    args: safeArgs,
                    workerData: {
                        type: this.type,
                        config: ConnectionRegistry.DEFAULT_CONFIGS[this.type.toLowerCase()]
                    }
                }, (err, result) => {
                    if (err) {
                        const error = new Error(err.message || 'Unknown error');
                        if (err.code) error.code = err.code;
                        if (err.type) error.type = err.type;
                        reject(error);
                    } else {
                        if (!result.success) {
                            const error = new Error(result.error?.message || 'Unknown error');
                            if (result.error?.code) error.code = result.error.code;
                            if (result.error?.type) error.type = result.error.type;
                            reject(error);
                        } else {
                            resolve(result.result);
                        }
                    }
                });
            } catch (err) {
                reject(new Error('Task execution failed: ' + (err.message || 'Unknown error')));
            }
        });
    }

    // Generic method to wrap callback-style operations
    _executeWithCallback(taskName, args, callback) {
        this._executeTask(taskName, args)
            .then(result => callback(null, result))
            .catch(error => {
                // Ensure error is properly formatted
                if (!(error instanceof Error)) {
                    error = new Error(error.message || 'Unknown error');
                }
                callback(error);
            });
    }

    // Database operations with callbacks
    refresh(callback) {
        this._executeWithCallback('refresh', [], callback);
    }

    saveDatabase(callback) {
        this._executeWithCallback('saveDatabase', [], callback);
    }

    getCollections(callback) {
        this._executeWithCallback('getCollections', [], callback);
    }

    createCollection(tableName, indicesList, callback) {
        this._executeWithCallback('createCollection', [tableName, indicesList], callback);
    }

    removeCollection(tableName, callback) {
        this._executeWithCallback('removeCollection', [tableName], callback);
    }

    addIndex(tableName, property, callback) {
        this._executeWithCallback('addIndex', [tableName, property], callback);
    }

    getOneRecord(tableName, callback) {
        this._executeWithCallback('getOneRecord', [tableName], callback);
    }

    getAllRecords(tableName, callback) {
        this._executeWithCallback('getAllRecords', [tableName], callback);
    }

    insertRecord(tableName, pk, record, callback) {
        this._executeWithCallback('insertRecord', [tableName, pk, record], callback);
    }

    updateRecord(tableName, pk, record, callback) {
        this._executeWithCallback('updateRecord', [tableName, pk, record], callback);
    }

    deleteRecord(tableName, pk, callback) {
        this._executeWithCallback('deleteRecord', [tableName, pk], callback);
    }

    getRecord(tableName, pk, callback) {
        this._executeWithCallback('getRecord', [tableName, pk], callback);
    }

    filter(tableName, filterConditions = [], sort = 'asc', max = null, callback) {
        this._executeWithCallback('filter', [tableName, filterConditions, sort, max], callback);
    }

    addInQueue(queueName, object, ensureUniqueness = false, callback) {
        this._executeWithCallback('addInQueue', [queueName, object, ensureUniqueness], callback);
    }

    queueSize(queueName, callback) {
        this._executeWithCallback('queueSize', [queueName], callback);
    }

    listQueue(queueName, sortAfterInsertTime = 'asc', onlyFirstN = null, callback) {
        this._executeWithCallback('listQueue', [queueName, sortAfterInsertTime, onlyFirstN], callback);
    }

    getObjectFromQueue(queueName, hash, callback) {
        this._executeWithCallback('getObjectFromQueue', [queueName, hash], callback);
    }

    deleteObjectFromQueue(queueName, hash, callback) {
        this._executeWithCallback('deleteObjectFromQueue', [queueName, hash], callback);
    }

    // Special handling for writeKey and readKey due to value processing
    writeKey(key, value, callback) {
        const valueObject = this._processValueForStorage(value);
        this._executeWithCallback('writeKey', [key, valueObject], callback);
    }

    readKey(key, callback) {
        this._executeWithCallback('readKey', [key], (error, result) => {
            if (error) return callback(error);
            if (!result) return callback(null, null);
            callback(null, typeof result === 'string' ? JSON.parse(result) : result);
        });
    }

    // Async versions of operations
    async refreshAsync() {
        return this._executeTask('refresh', []);
    }

    async removeCollectionAsync(tableName) {
        return this._executeTask('removeCollectionAsync', [tableName]);
    }

    async count(tableName) {
        return this._executeTask('count', [tableName]);
    }

    async saveDatabaseAsync() {
        await this._executeTask('saveDatabase', []);
        return {message: "Database saved"};
    }

    // Helper methods
    _processValueForStorage(value) {
        if (Buffer.isBuffer(value)) {
            return {
                type: "buffer",
                value: value.toString()
            };
        }
        if (value !== null && typeof value === "object") {
            return {
                type: "object",
                value: JSON.stringify(value)
            };
        }
        return {
            type: typeof value,
            value: value
        };
    }
}

module.exports = SQLAdapter;