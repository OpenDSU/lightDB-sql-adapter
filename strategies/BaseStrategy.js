// strategies/BaseStrategy.js
class BaseStrategy {
    constructor() {
        this._storageDB = null;
        this.READ_WRITE_KEY_TABLE = "KeyValueTable";

        // Initialize OpenDSU dependencies
        // this.openDSU = require("opendsu");
        // this.keySSISpace = this.openDSU.loadAPI("keyssi");
        // this.w3cDID = this.openDSU.loadAPI("w3cdid");
        // this.CryptoSkills = this.w3cDID.CryptographicSkills;

        // Define constant table names
        this.KEY_SSIS_TABLE = "keyssis";
        this.SEED_SSIS_TABLE = "seedssis";
        this.DIDS_PRIVATE_KEYS = "dids_private";
    }

    async createDatabase(connection) {
        throw new Error('Not implemented');
    }

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

    async _convertToSQLQuery(connection, conditions) {
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


    //------------------ KeySSIs -----------------
    // async getCapableOfSigningKeySSI(connection, keySSI) {
    //     if (typeof keySSI === "undefined") {
    //         throw new Error(`A SeedSSI should be specified.`);
    //     }
    //
    //     if (typeof keySSI === "string") {
    //         try {
    //             keySSI = this.keySSISpace.parse(keySSI);
    //         } catch (e) {
    //             throw new Error(`Failed to parse keySSI ${keySSI}`);
    //         }
    //     }
    //
    //     const record = await this.getRecord(connection, this.KEY_SSIS_TABLE, keySSI.getIdentifier());
    //     if (!record) {
    //         throw new Error(`No capable of signing keySSI found for keySSI ${keySSI.getIdentifier()}`);
    //     }
    //
    //     let capableOfSigningKeySSI;
    //     try {
    //         capableOfSigningKeySSI = this.keySSISpace.parse(record.capableOfSigningKeySSI);
    //     } catch (e) {
    //         throw new Error(`Failed to parse keySSI ${record.capableOfSigningKeySSI}`);
    //     }
    //
    //     return capableOfSigningKeySSI;
    // }
    //
    // async storeSeedSSI(connection, seedSSI, alias) {
    //     if (typeof seedSSI === "string") {
    //         try {
    //             seedSSI = this.keySSISpace.parse(seedSSI);
    //         } catch (e) {
    //             throw new Error(`Failed to parse keySSI ${seedSSI}`);
    //         }
    //     }
    //
    //     const keySSIIdentifier = seedSSI.getIdentifier();
    //
    //     const registerDerivedKeySSIs = async (derivedKeySSI) => {
    //         await this.insertRecord(connection, this.KEY_SSIS_TABLE, derivedKeySSI.getIdentifier(), {
    //             capableOfSigningKeySSI: keySSIIdentifier
    //         });
    //
    //         try {
    //             derivedKeySSI = derivedKeySSI.derive();
    //         } catch (e) {
    //             return;
    //         }
    //
    //         await registerDerivedKeySSIs(derivedKeySSI);
    //     };
    //
    //     await this.insertRecord(connection, this.SEED_SSIS_TABLE, alias, {
    //         seedSSI: keySSIIdentifier
    //     });
    //
    //     await registerDerivedKeySSIs(seedSSI);
    // }
    //
    // async signForKeySSI(connection, keySSI, hash) {
    //     const capableOfSigningKeySSI = await this.getCapableOfSigningKeySSI(connection, keySSI);
    //     if (typeof capableOfSigningKeySSI === "undefined") {
    //         throw new Error(`The provided SSI does not grant writing rights`);
    //     }
    //
    //     return await capableOfSigningKeySSI.sign(hash);
    // }
    //
    // //------------------ DIDs -----------------
    // async getPrivateInfoForDID(connection, did) {
    //     const record = await this.getRecord(connection, this.DIDS_PRIVATE_KEYS, did);
    //     if (!record) {
    //         throw new Error(`Failed to get private info for did ${did}`);
    //     }
    //
    //     return record.privateKeys.map(privateKey => {
    //         if (privateKey) {
    //             return Buffer.from(privateKey);
    //         }
    //         return privateKey;
    //     });
    // }
    //
    // async __ensureAreDIDDocumentsThenExecute(did, fn) {
    //     if (typeof did === "string") {
    //         const didDocument = await this.w3cDID.resolveDID(did);
    //         return await fn(didDocument);
    //     }
    //     return await fn(did);
    // }
    //
    // async storeDID(connection, storedDID, privateKeys) {
    //     const record = await this.getRecord(connection, this.DIDS_PRIVATE_KEYS, storedDID);
    //     if (!record) {
    //         return await this.insertRecord(connection, this.DIDS_PRIVATE_KEYS, storedDID, {
    //             privateKeys: privateKeys
    //         });
    //     }
    //
    //     privateKeys.forEach(privateKey => {
    //         record.privateKeys.push(privateKey);
    //     });
    //
    //     return await this.updateRecord(connection, this.DIDS_PRIVATE_KEYS, storedDID, record);
    // }
    //
    // async signForDID(connection, didThatIsSigning, hash) {
    //     const signForDID = async (didDoc) => {
    //         const privateKeys = await this.getPrivateInfoForDID(connection, didDoc.getIdentifier());
    //         try {
    //             return this.CryptoSkills.applySkill(
    //                 didDoc.getMethodName(),
    //                 this.CryptoSkills.NAMES.SIGN,
    //                 hash,
    //                 privateKeys[privateKeys.length - 1]
    //             );
    //         } catch (err) {
    //             throw err;
    //         }
    //     };
    //
    //     return await this.__ensureAreDIDDocumentsThenExecute(didThatIsSigning, signForDID);
    // }
    //
    // async verifyForDID(connection, didThatIsVerifying, hash, signature) {
    //     const verifyForDID = async (didDoc) => {
    //         try {
    //             const publicKey = await didDoc.getPublicKey("pem");
    //             return this.CryptoSkills.applySkill(
    //                 didDoc.getMethodName(),
    //                 this.CryptoSkills.NAMES.VERIFY,
    //                 hash,
    //                 publicKey,
    //                 Buffer.from(signature)
    //             );
    //         } catch (err) {
    //             throw err;
    //         }
    //     };
    //
    //     return await this.__ensureAreDIDDocumentsThenExecute(didThatIsVerifying, verifyForDID);
    // }
    //
    // async encryptMessage(connection, didFrom, didTo, message) {
    //     const encryptMessage = async () => {
    //         const privateKeys = await this.getPrivateInfoForDID(connection, didFrom.getIdentifier());
    //         return await this.CryptoSkills.applySkill(
    //             didFrom.getMethodName(),
    //             this.CryptoSkills.NAMES.ENCRYPT_MESSAGE,
    //             privateKeys,
    //             didFrom,
    //             didTo,
    //             message
    //         );
    //     };
    //
    //     if (typeof didFrom === "string") {
    //         didFrom = await this.w3cDID.resolveDID(didFrom);
    //
    //         if (typeof didTo === "string") {
    //             didTo = await this.w3cDID.resolveDID(didTo);
    //         }
    //     }
    //
    //     return await encryptMessage();
    // }
    //
    // async decryptMessage(connection, didTo, encryptedMessage) {
    //     const decryptMessage = async (didDoc) => {
    //         const privateKeys = await this.getPrivateInfoForDID(connection, didDoc.getIdentifier());
    //         return await this.CryptoSkills.applySkill(
    //             didDoc.getMethodName(),
    //             this.CryptoSkills.NAMES.DECRYPT_MESSAGE,
    //             privateKeys,
    //             didDoc,
    //             encryptedMessage
    //         );
    //     };
    //
    //     return await this.__ensureAreDIDDocumentsThenExecute(didTo, decryptMessage);
    // }
}

module.exports = BaseStrategy;