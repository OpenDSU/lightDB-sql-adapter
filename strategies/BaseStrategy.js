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
}

module.exports = BaseStrategy;