// sqlAdapter.js
const crypto = require('crypto');
const SQLWorkerAdapter = require('./sqlWorkerAdapter');

class SQLAdapter {
    constructor(type) {
        this.type = type;
        this.workerAdapter = new SQLWorkerAdapter(type);
        this.READ_WRITE_KEY_TABLE = "KeyValueTable";
        this.debug = process.env.DEBUG === 'true';

        // Forward worker events
        this.workerAdapter.on('error', error => console.error('Worker error:', error));
        this.workerAdapter.on('exit', code => {
            if (code !== 0) {
                console.error(`Worker stopped with exit code ${code}`);
            }
        });
    }

    async close() {
        await this.workerAdapter.close();
    }

    refresh(callback) {
        // Even though this is a no-op, we'll still run it through the worker
        // to maintain consistency with the worker pattern
        this.workerAdapter.executeWorkerTask('refresh', [])
            .then(() => callback())
            .catch(error => callback(error));
    }

    async refreshAsync() {
        return this.workerAdapter.executeWorkerTask('refresh', []);
    }

    saveDatabase(callback) {
        this.workerAdapter.executeWorkerTask('saveDatabase', [])
            .then(result => callback(undefined, {message: "Database saved"}))
            .catch(error => callback(error));
    }

    async saveDatabaseAsync() {
        await this.workerAdapter.executeWorkerTask('saveDatabase', []);
        return {message: "Database saved"};
    }

    async count(tableName) {
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['count', [tableName]]
        );
        return this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseCountResult', result]
        );
    }

    async getCollections() {
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['getCollections', []]
        );
        return this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseCollectionsResult', result]
        );
    }

    async createCollection(tableName, indicesList) {
        try {
            const result = await this.workerAdapter.executeWorkerTask(
                'executeQuery',
                ['createCollection', [tableName, indicesList]]
            );
            return {message: `Collection ${tableName} created`};
        } catch (error) {
            // Convert complex error to simple string
            const errorMessage = error.message || 'Unknown error occurred';
            throw new Error(`Failed to create collection: ${errorMessage}`);
        }
    }

    async removeCollection(tableName) {
        return this.workerAdapter.executeWorkerTask(
            'executeTransaction',
            ['removeCollection', [tableName]]
        );
    }

    async removeCollectionAsync(tableName) {
        return this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['removeCollectionAsync', [tableName]]
        );
    }

    async addIndex(tableName, property) {
        return this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['addIndex', [tableName, property]]
        );
    }

    async getOneRecord(tableName) {
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['getOneRecord', [tableName]]
        );
        return this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseGetResult', result]
        );
    }

    async getAllRecords(tableName) {
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['getAllRecords', [tableName]]
        );
        return this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseFilterResults', result]
        );
    }

    async insertRecord(tableName, pk, record) {
        const timestamp = Date.now();
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['insertRecord', [tableName, pk, JSON.stringify(record), timestamp]]
        );
        return this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseInsertResult', [result, pk, record]]
        );
    }

    async updateRecord(tableName, pk, record) {
        const timestamp = Date.now();
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['updateRecord', [tableName, pk, JSON.stringify(record), timestamp]]
        );
        return this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseUpdateResult', result]
        );
    }

    async deleteRecord(tableName, pk) {
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['deleteRecord', [tableName, pk]]
        );
        return this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseDeleteResult', [result, pk]]
        );
    }

    async getRecord(tableName, pk) {
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['getRecord', [tableName, pk]]
        );
        return this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseGetResult', result]
        );
    }

    async filter(tableName, filterConditions = [], sort = 'asc', max = null) {
        let conditions = '';
        let sortConfig = {
            field: '__timestamp',
            direction: (sort === 'desc' ? 'DESC' : 'ASC')
        };

        if (filterConditions && filterConditions.length) {
            const result = await this.workerAdapter.executeWorkerTask(
                'executeQuery',
                ['convertConditionsToLokiQuery', [filterConditions]]
            );
            conditions = result;
        }

        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['filter', [tableName, conditions, sortConfig, max]]
        );
        return this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseFilterResults', result]
        );
    }

    // Queue operations
    async addInQueue(queueName, object, ensureUniqueness = false) {
        return this.workerAdapter.executeWorkerTask(
            'addInQueue',
            [queueName, object, ensureUniqueness]
        );
    }

    async queueSize(queueName) {
        return this.count(queueName);
    }

    async listQueue(queueName, sortAfterInsertTime = 'asc', onlyFirstN = null) {
        const results = await this.filter(queueName, [], sortAfterInsertTime, onlyFirstN);
        return results.map(r => r.pk);
    }

    async getObjectFromQueue(queueName, hash) {
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['getObjectFromQueue', [queueName, hash]]
        );
        return this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseGetResult', result]
        );
    }

    async deleteObjectFromQueue(queueName, hash) {
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['deleteObjectFromQueue', [queueName, hash]]
        );
        return this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseDeleteResult', result]
        );
    }

    // Key-value operations
    async writeKey(key, value) {
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
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['writeKey', [this.READ_WRITE_KEY_TABLE, key, JSON.stringify(valueObject), timestamp]]
        );
        return valueObject;
    }

    async readKey(key) {
        const result = await this.workerAdapter.executeWorkerTask(
            'executeQuery',
            ['readKey', [this.READ_WRITE_KEY_TABLE, key]]
        );

        const parsedResult = await this.workerAdapter.executeWorkerTask(
            'parseResult',
            ['parseReadKeyResult', result]
        );

        if (!parsedResult) {
            return null;
        }

        return typeof parsedResult === 'string' ? JSON.parse(parsedResult) : parsedResult;
    }
}

module.exports = SQLAdapter;