function SQLAdapter(config) {
    const logger = $$.getLogger("SQLAdapter", "SQLAdapter.js");
    const SQLDecorator = require("./sqlDecorator");
    const openDSU = require("opendsu");
    const aclAPI = require("acl-magic");
    const utils = openDSU.loadAPI("utils");
    logger.info("Creating SQLAdapter instance");
    const EnclaveMixin = openDSU.loadAPI("enclave").EnclaveMixin;
    EnclaveMixin(this);

    let refreshInProgress = false;

    this.close = async () => {
        return await this.storageDB.close();
    }

    this.refreshInProgress = (forDID) => {
        return refreshInProgress;
    }

    this.refresh = (forDID, callback) => {
        refreshInProgress = true;
        this.storageDB.refresh((err) => {
            refreshInProgress = false;
            callback(err);
        });
    }

    this.saveDatabase = (forDID, callback) => {
        this.storageDB.saveDatabase(callback);
    }

    this.createDatabase = (forDID, callback) => {
        this.storageDB.createDatabase(callback);
    }

    this.cleanupDatabase = (forDID, callback) => {
        this.storageDB.cleanupDatabase(callback);
    }

    this.removeCollection = (forDID, tableName, callback) => {
        this.storageDB.removeCollection(tableName, callback);
    }

    this.removeCollectionAsync = (forDID, tableName) => {
        return new Promise((resolve, reject) => {
            this.storageDB.removeCollection(tableName, (err) => {
                if (err) {
                    return reject(err);
                }
                resolve();
            });
        });
    }

    this.refreshAsync = () => {
        let self = this;
        return new Promise((resolve, reject) => {
            self.storageDB.refresh((err) => {
                if (err) {
                    return reject(err);
                }
                resolve();
            });
        });
    }

    const WRITE_ACCESS = "write";
    const READ_ACCESS = "read";
    const WILDCARD = "*";
    const persistence = aclAPI.createEnclavePersistence(this);

    this.grantWriteAccess = (forDID, callback) => {
        persistence.grant(WRITE_ACCESS, WILDCARD, forDID, (err) => {
            if (err) {
                return callback(err);
            }

            this.grantReadAccess(forDID, callback);
        });
    }

    this.hasWriteAccess = (forDID, callback) => {
        persistence.loadResourceDirectGrants(WRITE_ACCESS, forDID, (err, usersWithAccess) => {
            if (err) {
                return callback(err);
            }

            callback(undefined, usersWithAccess.indexOf(WILDCARD) !== -1);
        });
    }

    this.revokeWriteAccess = (forDID, callback) => {
        persistence.ungrant(WRITE_ACCESS, WILDCARD, forDID, callback);
    }

    this.grantReadAccess = (forDID, callback) => {
        persistence.grant(READ_ACCESS, WILDCARD, forDID, callback);
    }

    this.hasReadAccess = (forDID, callback) => {
        persistence.loadResourceDirectGrants(READ_ACCESS, forDID, (err, usersWithAccess) => {
            if (err) {
                return callback(err);
            }

            callback(undefined, usersWithAccess.indexOf(WILDCARD) !== -1);
        });
    }

    this.revokeReadAccess = (forDID, callback) => {
        persistence.ungrant(READ_ACCESS, WILDCARD, forDID, err => {
            if (err) {
                return callback(err);
            }

            this.revokeWriteAccess(forDID, callback);
        });
    }

    this.getOneRecord = (forDID, tableName, callback) => {
        this.storageDB.getOneRecord(tableName, callback);
    }

    this.count = (forDID, tableName, callback) => {
        this.storageDB.count(tableName, callback);
    }

    this.addInQueue = (forDID, queueName, encryptedObject, ensureUniqueness, callback) => {
        this.storageDB.addInQueue(queueName, encryptedObject, ensureUniqueness, callback);
    }

    this.queueSize = (forDID, queueName, callback) => {
        this.count(queueName, callback);
    }

    this.listQueue = (forDID, queueName, sortAfterInsertTime, onlyFirstN, callback) => {
        this.storageDB.listQueue(queueName, sortAfterInsertTime, onlyFirstN, callback);
    }

    this.getObjectFromQueue = (forDID, queueName, hash, callback) => {
        return this.getRecord(forDID, queueName, hash, callback)
    }

    this.deleteObjectFromQueue = (forDID, queueName, hash, callback) => {
        return this.deleteRecord(forDID, queueName, hash, callback)
    }

    this.getCollections = (forDID, callback) => {
        this.storageDB.getCollections(callback);
    }

    this.createCollection = (forDID, tableName, indicesList, callback) => {
        if (typeof indicesList === "function") {
            callback = indicesList;
            indicesList = undefined;
        }
        this.storageDB.createCollection(tableName, indicesList, callback);
    }

    this.getAllRecords = (forDID, tableName, callback) => {
        this.storageDB.getAllRecords(tableName, callback);
    }

    this.insertRecord = (forDID, tableName, pk, record, callback) => {
        this.storageDB.insertRecord(tableName, pk, record, callback);
    }

    this.updateRecord = (forDID, tableName, pk, record, callback) => {
        this.storageDB.updateRecord(tableName, pk, record, callback);
    }

    this.deleteRecord = (forDID, tableName, pk, callback) => {
        this.storageDB.deleteRecord(tableName, pk, callback);
    }

    this.getRecord = (forDID, tableName, pk, callback) => {
        this.storageDB.getRecord(tableName, pk, callback);
    }

    this.filter = (forDID, tableName, filterConditions, sort = 'asc', max = null, callback) => {
        if (typeof filterConditions === "function") {
            callback = filterConditions;
            filterConditions = [];
            sort = 'asc';
            max = null;
        } else if (typeof sort === "function") {
            callback = sort;
            sort = 'asc';
            max = null;
        } else if (typeof max === "function") {
            callback = max;
            max = null;
        }
        this.storageDB.filter(tableName, filterConditions, sort, max, callback);
    }

    this.writeKey = (forDID, key, value, callback) => {
        this.storageDB.writeKey(key, value, callback);
    }

    this.readKey = (forDID, key, callback) => {
        this.storageDB.readKey(key, callback);
    }

    this.allowedInReadOnlyMode = function (functionName) {
        let readOnlyFunctions = ["getCollections",
            "listQueue",
            "queueSize",
            "count",
            "hasReadAccess",
            "getPrivateInfoForDID",
            "getCapableOfSigningKeySSI",
            "getPathKeyMapping",
            "getDID",
            "getPrivateKeyForSlot",
            "getIndexedFields",
            "getRecord",
            "getAllTableNames",
            "filter",
            "readKey",
            "getAllRecords",
            "getReadForKeySSI",
            "verifyForDID",
            "encryptMessage",
            "decryptMessage"];

        return readOnlyFunctions.indexOf(functionName) !== -1;
    }

    utils.bindAutoPendingFunctions(this, ["on", "off", "dispatchEvent", "beginBatch", "isInitialised", "getEnclaveType", "getDID", "getUniqueIdAsync"]);

    this.storageDB = new SQLDecorator(config);
    this.finishInitialisation();
}

module.exports = SQLAdapter;