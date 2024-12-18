// sqlAdapter.js
const syndicate = require('syndicate');
const path = require('path');

class SQLAdapter {
    READ_WRITE_KEY_TABLE;
    debug;
    workerPool;
    config;

    constructor(config) {
        this.READ_WRITE_KEY_TABLE = "KeyValueTable";
        this.debug = process.env.DEBUG === 'true';

        this.config = config;

        this.workerPool = syndicate.createWorkerPool({
            bootScript: path.join(__dirname, "./workerScript.js"),
            maximumNumberOfWorkers: 4,
            workerOptions: {
                workerData: {
                    config
                }
            }
        });
        console.log("creating new sqlAdapter instance.");
    }

    close = async () => {
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

    _executeTask = (taskName, args) => {
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
                        config: this.config,
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

    _executeWithCallback = (taskName, args, callback) => {
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

    createDatabase = (forDID, callback) => {
        this._executeWithCallback('createDatabase', [], callback);
    }

    // Database operations with callbacks
    refresh = (forDID, callback) => {
        this._executeWithCallback('refresh', [], callback);
    }

    saveDatabase = (forDID, callback) => {
        this._executeWithCallback('saveDatabase', [], callback);
    }

    getCollections = (forDID, callback) => {
        this._executeWithCallback('getCollections', [], callback);
    }

    createCollection = (forDID, tableName, indicesList, callback) => {
        this._executeWithCallback('createCollection', [tableName, indicesList], callback);
    }

    removeCollection = (forDID, tableName, callback) => {
        this._executeWithCallback('removeCollection', [tableName], callback);
    }

    addIndex = (forDID, tableName, property, callback) => {
        this._executeWithCallback('addIndex', [tableName, property], callback);
    }

    getOneRecord = (forDID, tableName, callback) => {
        this._executeWithCallback('getOneRecord', [tableName], callback);
    }

    getAllRecords = (forDID, tableName, callback) => {
        this._executeWithCallback('getAllRecords', [tableName], callback);
    }

    insertRecord = (forDID, tableName, pk, record, callback) => {
        this._executeWithCallback('insertRecord', [tableName, pk, record], callback);
    }

    updateRecord = (forDID, tableName, pk, record, callback) => {
        this._executeWithCallback('updateRecord', [tableName, pk, record], callback);
    }

    deleteRecord = (forDID, tableName, pk, callback) => {
        this._executeWithCallback('deleteRecord', [tableName, pk], callback);
    }

    getRecord = (forDID, tableName, pk, callback) => {
        this._executeWithCallback('getRecord', [tableName, pk], callback);
    }

    filter = (forDID, tableName, filterConditions = [], sort = 'asc', max = null, callback) => {

        // Handle when filterConditions is the callback
        if (typeof filterConditions === 'function') {
            callback = filterConditions;
            filterConditions = [];
            sort = 'asc';
            max = null;
        }
        // Handle when sort is the callback
        else if (typeof sort === 'function') {
            callback = sort;
            sort = 'asc';
            max = null;
        }
        // Handle when max is the callback
        else if (typeof max === 'function') {
            callback = max;
            max = null;
        }
        this._executeWithCallback('filter', [tableName, filterConditions, sort, max], callback);
    }

    addInQueue = (forDID, queueName, object, ensureUniqueness = false, callback) => {
        this._executeWithCallback('addInQueue', [queueName, object, ensureUniqueness], callback);
    }

    queueSize = (forDID, queueName, callback) => {
        this._executeWithCallback('queueSize', [queueName], callback);
    }

    listQueue = (forDID, queueName, sortAfterInsertTime = 'asc', onlyFirstN = null, callback) => {
        this._executeWithCallback('listQueue', [queueName, sortAfterInsertTime, onlyFirstN], callback);
    }

    getObjectFromQueue = (forDID, queueName, hash, callback) => {
        this._executeWithCallback('getObjectFromQueue', [queueName, hash], callback);
    }

    deleteObjectFromQueue = (forDID, queueName, hash, callback) => {
        this._executeWithCallback('deleteObjectFromQueue', [queueName, hash], callback);
    }

    writeKey = (forDID, key, value, callback) => {
        const valueObject = this._processValueForStorage(value);
        this._executeWithCallback('writeKey', [key, valueObject], callback);
    }

    readKey = (forDID, key, callback) => {
        this._executeWithCallback('readKey', [key], (error, result) => {
            if (error) return callback(error);
            if (!result) return callback(null, null);
            callback(null, typeof result === 'string' ? JSON.parse(result) : result);
        });
    }

    // Async versions of operations
    refreshAsync = async () => {
        return this._executeTask('refresh', []);
    }

    removeCollectionAsync = async (forDID, tableName) => {
        return this._executeTask('removeCollectionAsync', [tableName]);
    }

    count = async (forDID, tableName, callback) => {
        return this._executeWithCallback('count', [tableName], callback);
    }

    saveDatabaseAsync = async (forDID) => {
        await this._executeTask('saveDatabase', []);
        return {message: "Database saved"};
    }

    // Helper methods
    _processValueForStorage = (value) => {
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