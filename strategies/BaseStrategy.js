// strategies/BaseStrategy.js
class BaseStrategy {
    // Database schema operations
    createCollection(tableName, indicesList) {
        throw new Error('Not implemented');
    }

    addIndex(tableName, property) {
        throw new Error('Not implemented');
    }

    removeCollection(tableName) {
        throw new Error('Not implemented');
    }

    removeCollectionAsync(tableName) {
        throw new Error('Not implemented');
    }

    // Collection information
    getCollections() {
        throw new Error('Not implemented');
    }

    listCollections() {
        throw new Error('Not implemented');
    }

    count(tableName) {
        throw new Error('Not implemented');
    }

    // Database state management
    async close(connection) {
        throw new Error('Not implemented');
    }

    refreshInProgress() {
        throw new Error('Not implemented');
    }

    refresh(callback) {
        throw new Error('Not implemented');
    }

    refreshAsync() {
        throw new Error('Not implemented');
    }

    saveDatabase(callback) {
        throw new Error('Not implemented');
    }

    // Record operations
    insertRecord(tableName, pk, record) {
        throw new Error('Not implemented');
    }

    updateRecord(tableName, pk, record) {
        throw new Error('Not implemented');
    }

    deleteRecord(tableName, pk) {
        throw new Error('Not implemented');
    }

    getRecord(tableName, pk) {
        throw new Error('Not implemented');
    }

    getOneRecord(tableName) {
        throw new Error('Not implemented');
    }

    getAllRecords(tableName) {
        throw new Error('Not implemented');
    }

    filter(tableName, conditions, sort, max) {
        throw new Error('Not implemented');
    }

    convertConditionsToLokiQuery(conditions) {
        throw new Error('Not implemented');
    }

    __getSortingField(filterConditions) {
        throw new Error('Not implemented');
    }

    // Queue operations
    addInQueue(queueName, object, ensureUniqueness) {
        throw new Error('Not implemented');
    }

    queueSize(queueName) {
        throw new Error('Not implemented');
    }

    listQueue(queueName, sortAfterInsertTime, onlyFirstN) {
        throw new Error('Not implemented');
    }

    getObjectFromQueue(queueName, hash) {
        throw new Error('Not implemented');
    }

    deleteObjectFromQueue(queueName, hash) {
        throw new Error('Not implemented');
    }

    // Key-value operations
    writeKey(tableName) {
        throw new Error('Not implemented');
    }

    readKey(key) {
        throw new Error('Not implemented');
    }

    // Storage reference
    get storageDB() {
        throw new Error('Not implemented');
    }

    set storageDB(value) {
        throw new Error('Not implemented');
    }

    // Transaction handling
    async executeQuery(connection, query, params = {}) {
        throw new Error('Not implemented');
    }

    async executeTransaction(connection, queries) {
        throw new Error('Not implemented');
    }

    parseWriteKeyResult(result) {
        throw new Error('Not implemented');
    }

    parseReadKeyResult(result) {
        throw new Error('Not implemented');
    }

    /*
    // KeySSI operations - To be implemented later
    getCapableOfSigningKeySSI(keySSI, callback) { throw new Error('Not implemented'); }
    storeSeedSSI(seedSSI, alias, callback) { throw new Error('Not implemented'); }
    signForKeySSI(keySSI, hash, callback) { throw new Error('Not implemented'); }

    // DID operations - To be implemented later
    getPrivateInfoForDID(did, callback) { throw new Error('Not implemented'); }
    __ensureAreDIDDocumentsThenExecute(did, fn, callback) { throw new Error('Not implemented'); }
    storeDID(storedDID, privateKeys, callback) { throw new Error('Not implemented'); }
    signForDID(didThatIsSigning, hash, callback) { throw new Error('Not implemented'); }
    verifyForDID(didThatIsVerifying, hash, signature, callback) { throw new Error('Not implemented'); }
    encryptMessage(didFrom, didTo, message, callback) { throw new Error('Not implemented'); }
    decryptMessage(didTo, encryptedMessage, callback) { throw new Error('Not implemented'); }
    */

    async executeTask(pool, taskName, args) {
        switch (taskName) {
            case 'refresh':
                return await this.refresh(pool);

            case 'saveDatabase':
                return await this.saveDatabase(pool);

            case 'count':
                const countQuery = this.count(args[0]);
                const countResult = await this.executeQuery(pool, countQuery);
                return this.parseCountResult(countResult);

            case 'getCollections':
                const collectionsQuery = this.getCollections();
                const collectionsResult = await this.executeQuery(pool, collectionsQuery);
                return this.parseCollectionsResult(collectionsResult);

            case 'createCollection':
                await this.ensureCollectionsTable(pool);
                const createQueries = this.createCollection(args[0], args[1]);
                return await this.executeTransaction(pool, createQueries);

            case 'removeCollection':
                const removeQueries = this.removeCollection(args[0]);
                return await this.executeTransaction(pool, removeQueries);

            case 'removeCollectionAsync':
                await this.ensureCollectionsTable(pool);
                const asyncRemoveQueries = this.removeCollection(args[0]);
                return await this.executeTransaction(pool, asyncRemoveQueries);

            case 'addIndex':
                const indexQuery = this.addIndex(args[0], args[1]);
                return await this.executeQuery(pool, indexQuery);

            case 'getOneRecord':
                const oneRecordQuery = this.getOneRecord(args[0]);
                const oneRecordResult = await this.executeQuery(pool, oneRecordQuery);
                return this.parseGetResult(oneRecordResult);

            case 'getAllRecords':
                const allRecordsQuery = this.getAllRecords(args[0]);
                const allRecordsResult = await this.executeQuery(pool, allRecordsQuery);
                return this.parseFilterResults(allRecordsResult);

            case 'insertRecord':
                const timestamp = Date.now();
                const insertQuery = this.insertRecord(args[0]);
                const insertResult = await this.executeQuery(pool, insertQuery, [args[1], JSON.stringify(args[2]), timestamp]);
                return this.parseInsertResult(insertResult, args[1], args[2]);

            case 'updateRecord':
                const updateQuery = this.updateRecord(args[0]);
                const updateResult = await this.executeQuery(pool, updateQuery, [args[1], JSON.stringify(args[2]), Date.now()]);
                return this.parseUpdateResult(updateResult);

            case 'deleteRecord':
                const deleteQuery = this.deleteRecord(args[0]);
                const deleteResult = await this.executeQuery(pool, deleteQuery, [args[1]]);
                return this.parseDeleteResult(deleteResult, args[1]);

            case 'getRecord':
                const getQuery = this.getRecord(args[0]);
                const getResult = await this.executeQuery(pool, getQuery, [args[1]]);
                return this.parseGetResult(getResult);

            case 'filter':
                const [tableName, conditions, sort, max] = args;
                const filterConditions = conditions && conditions.length > 0 ?
                    this.convertConditionsToLokiQuery(conditions) : '';
                const sortConfig = {
                    field: '__timestamp',
                    direction: (sort === 'desc' ? 'DESC' : 'ASC')
                };
                const query = `
                SELECT pk, data, __timestamp 
                FROM "${tableName}"
                ${filterConditions ? ` WHERE ${filterConditions}` : ''}
                ORDER BY ${sortConfig.field === '__timestamp' ? '__timestamp' : `(data->>'${sortConfig.field}')`} ${sortConfig.direction}
                ${max ? ` LIMIT ${max}` : ''}
            `;
                const filterResult = await this.executeQuery(pool, query);
                return this.parseFilterResults(filterResult);

            case 'addInQueue':
                return await this.addInQueue(pool, ...args);

            case 'queueSize':
                const queueSizeQuery = this.queueSize(args[0]);
                const queueSizeResult = await this.executeQuery(pool, queueSizeQuery);
                return this.parseQueueSizeResult(queueSizeResult);

            case 'listQueue':
                const [queueName, sortAfterInsertTime, onlyFirstN] = args;
                const listQuery = this.listQueue(queueName, sortAfterInsertTime, onlyFirstN);
                const listResult = await this.executeQuery(pool, listQuery);
                return this.parseFilterResults(listResult).map(r => r.pk);

            case 'writeKey':
                await this.ensureKeyValueTable(pool);
                const writeQuery = this.writeKey();
                const writeResult = await this.executeQuery(pool, writeQuery, [args[0], JSON.stringify(args[1]), Date.now()]);
                return this.parseWriteKeyResult(writeResult);

            case 'readKey':
                const readQuery = this.readKey();
                const readResult = await this.executeQuery(pool, readQuery, [args[0]]);
                return this.parseReadKeyResult(readResult);

            default:
                throw new Error(`Unknown task: ${taskName}`);
        }
    }
}

module.exports = BaseStrategy;