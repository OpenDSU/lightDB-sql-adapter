// sqlAdapter.js
const crypto = require('crypto');
const {StrategyFactory} = require('./strategyFactory');
const ConnectionRegistry = require('./connectionRegistry');

class SQLAdapter {
    constructor(type, connection) {
        this.type = type;
        this.connection = connection;
        this.strategy = StrategyFactory.createStrategy(type);
        this.logger = console;
        this.READ_WRITE_KEY_TABLE = "KeyValueTable";
        this.debug = process.env.DEBUG === 'true';

        // Initialize connection and schema
        this.initPromise = this.#initialize().catch(err => {
            this.logger.error(`Failed to initialize schema:`, err);
            throw err;
        });
    }

    async #initialize() {
        await this.#initializeSchema();
        return this;
    }

    async #initializeSchema() {
        const collectionsTable = this.strategy.createCollectionsTable();
        const keyValueTable = this.strategy.createKeyValueTable(this.READ_WRITE_KEY_TABLE);

        if (this.debug) {
            this.logger.log('Collections Table Query:', collectionsTable);
            this.logger.log('KeyValue Table Query:', keyValueTable);
        }

        if (!collectionsTable || !collectionsTable.query) {
            throw new Error('Invalid collections table creation query');
        }
        if (!keyValueTable || !keyValueTable.query) {
            throw new Error('Invalid key-value table creation query');
        }

        await this.strategy.executeTransaction(this.connection, [
            collectionsTable,
            keyValueTable
        ]);
    }

    async close() {
        if (this.connection) {
            await this.strategy.closeConnection(this.connection);
        }
    }

    refresh(callback) {
        // No-op for SQL databases
        callback();
    }

    refreshAsync() {
        return Promise.resolve();
    }

    saveDatabase(callback) {
        // Auto-save is handled by SQL databases
        callback(undefined, {message: "Database saved"});
    }

    async count(tableName, callback) {
        try {
            const result = await this.strategy.executeQuery(
                this.connection,
                this.strategy.count(tableName)
            );
            callback(null, this.strategy.parseCountResult(result));
        } catch (err) {
            callback(err);
        }
    }

    async getCollections(callback) {
        try {
            const result = await this.strategy.executeQuery(
                this.connection,
                this.strategy.getCollections()
            );
            callback(null, this.strategy.parseCollectionsResult(result));
        } catch (err) {
            callback(err);
        }
    }

    async createCollection(tableName, indicesList, callback) {
        if (typeof indicesList === "function") {
            callback = indicesList;
            indicesList = undefined;
        }

        try {
            const queries = this.strategy.createCollection(tableName, indicesList);
            await this.strategy.executeTransaction(this.connection, queries);
            callback(undefined, {message: `Collection ${tableName} created`});
        } catch (err) {
            const error = err instanceof Error ? err : new Error(err.message || 'Unknown error');
            callback(error);
        }
    }

    async removeCollection(tableName, callback) {
        try {
            await this.strategy.executeTransaction(
                this.connection,
                this.strategy.removeCollection(tableName)
            );
            callback();
        } catch (err) {
            callback(err);
        }
    }

    async removeCollectionAsync(tableName) {
        return await this.strategy.removeCollectionAsync(this.connection, tableName);
    }

    async addIndex(tableName, property, callback) {
        try {
            await this.strategy.executeQuery(
                this.connection,
                this.strategy.addIndex(tableName, property)
            );
            callback();
        } catch (err) {
            callback(err);
        }
    }

    async getOneRecord(tableName, callback) {
        try {
            const result = await this.strategy.executeQuery(
                this.connection,
                this.strategy.getOneRecord(tableName)
            );
            callback(null, this.strategy.parseGetResult(result));
        } catch (err) {
            callback(err);
        }
    }

    async getAllRecords(tableName, callback) {
        try {
            const result = await this.strategy.executeQuery(
                this.connection,
                this.strategy.getAllRecords(tableName)
            );
            callback(null, this.strategy.parseFilterResults(result));
        } catch (err) {
            callback(err);
        }
    }

    async insertRecord(tableName, pk, record, callback) {
        try {
            const timestamp = Date.now();
            const query = this.strategy.insertRecord(tableName);
            const result = await this.strategy.executeQuery(
                this.connection,
                query,
                [pk, JSON.stringify(record), timestamp]  // Pass the parameters here
            );
            callback(null, this.strategy.parseInsertResult(result, pk, record));
        } catch (err) {
            callback(err);
        }
    }

    async updateRecord(tableName, pk, record, callback) {
        try {
            const timestamp = Date.now();
            const query = this.strategy.updateRecord(tableName);
            const result = await this.strategy.executeQuery(
                this.connection,
                query,
                [pk, JSON.stringify(record), timestamp]
            );
            callback(null, this.strategy.parseUpdateResult(result));
        } catch (err) {
            callback(err);
        }
    }

    async deleteRecord(tableName, pk, callback) {
        try {
            const query = this.strategy.deleteRecord(tableName);
            const result = await this.strategy.executeQuery(
                this.connection,
                query,
                [pk]
            );
            callback(null, this.strategy.parseDeleteResult(result));
        } catch (err) {
            callback(err);
        }
    }

    async getRecord(tableName, pk, callback) {
        try {
            const result = await this.strategy.executeQuery(
                this.connection,
                this.strategy.getRecord(tableName),
                {pk}
            );
            callback(null, this.strategy.parseGetResult(result, pk));
        } catch (err) {
            callback(err);
        }
    }

    async filter(tableName, filterConditions, sort, max, callback) {
        if (typeof filterConditions === "function") {
            callback = filterConditions;
            filterConditions = undefined;
            sort = "asc";
            max = Infinity;
        }

        try {
            let conditions = '';
            let sortConfig = {
                field: '__timestamp',
                direction: (sort === 'desc' ? 'DESC' : 'ASC')
            };

            if (filterConditions && filterConditions.length) {
                conditions = this.strategy.convertConditionsToLokiQuery(filterConditions);
            }

            const result = await this.strategy.executeQuery(
                this.connection,
                this.strategy.filter(tableName, conditions, sortConfig, max)
            );

            callback(null, this.strategy.parseFilterResults(result));
        } catch (err) {
            callback(err);
        }
    }

    // Queue operations
    async addInQueue(queueName, object, ensureUniqueness, callback) {
        if (typeof ensureUniqueness === "function") {
            callback = ensureUniqueness;
            ensureUniqueness = false;
        }

        try {
            const pk = await this.strategy.addInQueue(
                this.connection,
                queueName,
                object,
                ensureUniqueness
            );
            callback(null, pk);
        } catch (err) {
            callback(err);
        }
    }

    queueSize(queueName, callback) {
        this.count(queueName, callback);
    }

    listQueue(queueName, sortAfterInsertTime, onlyFirstN, callback) {
        if (typeof sortAfterInsertTime === "function") {
            callback = sortAfterInsertTime;
            sortAfterInsertTime = "asc";
            onlyFirstN = undefined;
        }

        this.filter(queueName, [], sortAfterInsertTime, onlyFirstN, (err, results) => {
            if (err) {
                if (this.strategy.isTableNotExistsError?.(err)) {
                    callback(undefined, []);
                    return;
                }
                callback(err);
                return;
            }
            callback(null, results.map(r => r.pk));
        });
    }

    async getObjectFromQueue(queueName, hash, callback) {
        try {
            const query = this.strategy.getObjectFromQueue(queueName, hash);
            const result = await this.strategy.executeQuery(
                this.connection,
                query,
                [hash]
            );
            callback(null, this.strategy.parseGetResult(result));
        } catch (err) {
            callback(err);
        }
    }

    async deleteObjectFromQueue(queueName, hash, callback) {
        try {
            const query = this.strategy.deleteObjectFromQueue(queueName, hash);
            const result = await this.strategy.executeQuery(
                this.connection,
                query,
                [hash]
            );
            callback(null, this.strategy.parseDeleteResult(result));
        } catch (err) {
            callback(err);
        }
    }

    // Key-value operations
    async writeKey(key, value, callback) {
        try {
            await this.initPromise;

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

            const timestamp = Date.now();
            const query = this.strategy.writeKey(this.READ_WRITE_KEY_TABLE);
            const result = await this.strategy.executeQuery(
                this.connection,
                query,
                [key, JSON.stringify(valueObject), timestamp]
            );
            callback(null, valueObject);
        } catch (err) {
            callback(err);
        }
    }

    async readKey(key, callback) {
        try {
            await this.initPromise;
            const query = this.strategy.readKey(this.READ_WRITE_KEY_TABLE);
            const result = await this.strategy.executeQuery(
                this.connection,
                query,
                [key]
            );

            const parsedResult = this.strategy.parseReadKeyResult(result);
            if (!parsedResult) {
                callback(null, null);
                return;
            }

            callback(null, typeof parsedResult === 'string' ? JSON.parse(parsedResult) : parsedResult);
        } catch (err) {
            callback(err);
        }
    }
}

module.exports = SQLAdapter;