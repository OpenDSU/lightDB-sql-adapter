// strategies/BaseStrategy.js
class BaseStrategy {
    // Database schema operations
    async createCollection(connection, tableName, indicesList) {
        throw new Error('Not implemented');
    }

    async removeCollection(connection, tableName) {
        throw new Error('Not implemented');
    }

    async removeCollectionAsync(connection, tableName) {
        throw new Error('Not implemented');
    }

    async addIndex(connection, tableName, property) {
        throw new Error('Not implemented');
    }

    // Collection information
    async getCollections(connection) {
        throw new Error('Not implemented');
    }

    async listCollections(connection) {
        throw new Error('Not implemented');
    }

    async count(connection, tableName) {
        throw new Error('Not implemented');
    }

    // Database state management
    async close(connection) {
        throw new Error('Not implemented');
    }

    async refreshInProgress(connection) {
        throw new Error('Not implemented');
    }

    async refresh(connection) {
        throw new Error('Not implemented');
    }

    async refreshAsync(connection) {
        throw new Error('Not implemented');
    }

    async saveDatabase(connection) {
        throw new Error('Not implemented');
    }

    // Record operations
    async insertRecord(connection, tableName, pk, record) {
        throw new Error('Not implemented');
    }

    async updateRecord(connection, tableName, pk, record) {
        throw new Error('Not implemented');
    }

    async deleteRecord(connection, tableName, pk) {
        throw new Error('Not implemented');
    }

    async getRecord(connection, tableName, pk) {
        throw new Error('Not implemented');
    }

    async getOneRecord(connection, tableName) {
        throw new Error('Not implemented');
    }

    async getAllRecords(connection, tableName) {
        throw new Error('Not implemented');
    }

    async filter(connection, tableName, conditions, sort, max) {
        throw new Error('Not implemented');
    }

    async convertConditionsToLokiQuery(connection, conditions) {
        throw new Error('Not implemented');
    }

    async __getSortingField(connection, filterConditions) {
        throw new Error('Not implemented');
    }

    // Queue operations
    async addInQueue(connection, queueName, object, ensureUniqueness) {
        throw new Error('Not implemented');
    }

    async queueSize(connection, queueName) {
        throw new Error('Not implemented');
    }

    async listQueue(connection, queueName, sortAfterInsertTime, onlyFirstN) {
        throw new Error('Not implemented');
    }

    async getObjectFromQueue(connection, queueName, hash) {
        throw new Error('Not implemented');
    }

    async deleteObjectFromQueue(connection, queueName, hash) {
        throw new Error('Not implemented');
    }

    // Key-value operations
    async writeKey(connection, key, value) {
        throw new Error('Not implemented');
    }

    async readKey(connection, key) {
        throw new Error('Not implemented');
    }

    async ensureCollectionsTable(connection) {
        throw new Error('Not implemented');
    }


    async ensureKeyValueTable(connection) {
        throw new Error('Not implemented');
    }

    // Transaction handling
    async executeQuery(connection, query, params = {}) {
        throw new Error('Not implemented');
    }

    async executeTransaction(connection, queries) {
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