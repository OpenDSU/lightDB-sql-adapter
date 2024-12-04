// strategies/SQLServerStrategy.js
const BaseStrategy = require('./BaseStrategy');
const sql = require('mssql');
const crypto = require('crypto');

class SQLServerStrategy extends BaseStrategy {
    constructor() {
        super();
        this._storageDB = null;
    }

    // Database schema operations
    createCollectionsTable() {
        return `
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='collections' AND xtype='U')
            CREATE TABLE collections (
                name VARCHAR(255) PRIMARY KEY,
                indices NVARCHAR(MAX)
            );
        `;
    }

    createKeyValueTable(tableName) {
        return `
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE name = '${tableName}' AND type = 'U')
            BEGIN
                CREATE TABLE [${tableName}] (
                    pk NVARCHAR(450) PRIMARY KEY,
                    data NVARCHAR(MAX),
                    __timestamp BIGINT
                )
            END
        `;
    }

    createCollection(tableName, indicesList) {
        const queries = [
            {
                query: this.createKeyValueTable(tableName),
                params: {}
            },
            {
                query: this.insertCollection(),
                params: {
                    name: tableName,
                    indices: JSON.stringify(indicesList || [])
                }
            }
        ];

        if (indicesList && Array.isArray(indicesList)) {
            for (const index of indicesList) {
                queries.push({
                    query: this.createIndex(tableName, index),
                    params: {}
                });
            }
        }

        return queries;
    }

    createIndex(tableName, index) {
        return `
            IF NOT EXISTS (
                SELECT * FROM sys.indexes 
                WHERE name = '${tableName}_${index}'
                AND object_id = OBJECT_ID('${tableName}')
            )
            BEGIN
                CREATE INDEX [${tableName}_${index}] 
                ON [${tableName}] ([pk])
                INCLUDE ([data], [__timestamp]);
            END
        `;
    }

    addIndex(tableName, property) {
        return this.createIndex(tableName, property);
    }

    removeCollection(tableName) {
        return [
            {
                query: `DROP TABLE IF EXISTS ${tableName}`,
                params: {}
            },
            {
                query: this.deleteFromCollection(),
                params: {name: tableName}
            }
        ];
    }

    async removeCollectionAsync(connection, tableName) {
        return await this.executeTransaction(connection, this.removeCollection(tableName));
    }

    // Collection information
    getCollections() {
        return `SELECT name FROM collections`;
    }

    listCollections() {
        return this.getCollections();
    }

    count(tableName) {
        return `SELECT COUNT(*) as count FROM ${tableName}`;
    }

    // Database state management
    async close(connection) {
        return await this.closeConnection(connection);
    }

    async closeConnection(connection) {
        if (connection) {
            await connection.close();
        }
    }

    refreshInProgress() {
        return false;
    }

    async refresh(connection, callback) {
        callback();
    }

    async refreshAsync(connection) {
        return Promise.resolve();
    }

    async saveDatabase(connection, callback) {
        callback(undefined, {message: "Database saved"});
    }

    // Record operations
    insertRecord(tableName) {
        return `
            INSERT INTO [${tableName}] (pk, data, __timestamp)
            VALUES (@pk, @data, @timestamp);
            SELECT * FROM [${tableName}] WHERE pk = @pk;
        `;
    }

    updateRecord(tableName) {
        return `
            UPDATE [${tableName}]
            SET data = @data, __timestamp = @timestamp
            WHERE pk = @pk;
            SELECT *
            FROM [${tableName}]
            WHERE pk = @pk;
        `;
    }

    deleteRecord(tableName) {
        return `DELETE FROM ${tableName} OUTPUT deleted.* WHERE pk = @pk`;
    }

    getRecord(tableName) {
        return `SELECT data, __timestamp FROM ${tableName} WHERE pk = @pk`;
    }

    getOneRecord(tableName) {
        return `SELECT TOP 1 data, __timestamp FROM ${tableName}`;
    }

    getAllRecords(tableName) {
        return `SELECT pk, data, __timestamp FROM ${tableName}`;
    }

    filter(tableName, conditions, sort, max) {
        return `
            SELECT ${max ? `TOP ${max}` : ''} pk, data, __timestamp 
            FROM ${tableName}
            ${conditions ? `WHERE ${conditions}` : ''}
            ORDER BY ${sort.field} ${sort.direction}
        `;
    }

    convertToSQLQuery(conditions) {
        if (!conditions || conditions.length === 0) {
            return {};
        }

        const andConditions = conditions.map(condition => {
            const [field, operator, value] = condition.split(/\s+/);
            return this.formatFilterCondition(field, operator, value);
        });

        return andConditions.join(' AND ');
    }

    __getSortingField(filterConditions) {
        if (filterConditions && filterConditions.length) {
            const splitCondition = filterConditions[0].split(" ");
            return splitCondition[0];
        }
        return '__timestamp';
    }

    formatFilterCondition(field, operator, value) {
        return `JSON_VALUE(data, '$.${field}') ${operator} ${value.replace(/['"]/g, '')}`;
    }

    // Queue operations
    async addInQueue(connection, queueName, object, ensureUniqueness = false) {
        const hash = crypto.createHash('sha256').update(JSON.stringify(object)).digest('hex');
        let pk = hash;

        if (ensureUniqueness) {
            const random = crypto.randomBytes(5).toString('base64');
            pk = `${hash}_${Date.now()}_${random}`;
        }

        const params = {
            pk,
            data: JSON.stringify(object),
            timestamp: Date.now()
        };

        await this.executeQuery(connection, this.insertRecord(queueName), params);
        return pk;
    }

    queueSize(queueName) {
        return this.count(queueName);
    }

    listQueue(queueName, sortAfterInsertTime = 'asc', onlyFirstN) {
        return this.filter(queueName, null,
            {field: '__timestamp', direction: sortAfterInsertTime.toUpperCase()},
            onlyFirstN
        );
    }

    getObjectFromQueue(queueName, hash) {
        return this.getRecord(queueName);
    }

    deleteObjectFromQueue(queueName, hash) {
        return this.deleteRecord(queueName);
    }

    // Key-value operations
    writeKey(tableName) {
        return `
            MERGE ${tableName} AS target
            USING (SELECT @pk AS pk, @data AS data, @timestamp AS __timestamp) AS source
            ON target.pk = source.pk
            WHEN MATCHED THEN UPDATE SET data = source.data, __timestamp = source.__timestamp
            WHEN NOT MATCHED THEN INSERT (pk, data, __timestamp) VALUES (source.pk, source.data, source.__timestamp);
            SELECT data FROM ${tableName} WHERE pk = @pk;
        `;
    }

    readKey(tableName, key) {
        return this.getRecord(tableName);
    }

    // Storage reference
    get storageDB() {
        return this._storageDB;
    }

    set storageDB(value) {
        this._storageDB = value;
    }

    // Collection maintenance
    insertCollection() {
        return `
            MERGE collections AS target
            USING (SELECT @name AS name, @indices AS indices) AS source
            ON target.name = source.name
            WHEN MATCHED THEN UPDATE SET indices = source.indices
            WHEN NOT MATCHED THEN INSERT (name, indices) VALUES (source.name, source.indices);
        `;
    }

    deleteFromCollection() {
        return 'DELETE FROM [collections] WHERE name = @name';
    }

    // Transaction handling
    async executeQuery(connection, query, params = {}) {
        const request = connection.request();

        // Add parameters to request
        for (const [key, value] of Object.entries(params)) {
            if (value !== undefined) {
                const paramName = key.startsWith('@') ? key.substring(1) : key;
                request.input(paramName, value);
            }
        }

        return await request.query(query);
    }

    async executeTransaction(connection, queries) {
        const transaction = await connection.transaction();
        try {
            await transaction.begin();
            const results = [];

            for (const {query, params = {}} of queries) {
                const request = transaction.request();

                // Add parameters to request
                for (const [key, value] of Object.entries(params)) {
                    if (value !== undefined) {
                        const paramName = key.startsWith('@') ? key.substring(1) : key;
                        request.input(paramName, value);
                    }
                }

                results.push(await request.query(query));
            }

            await transaction.commit();
            return results;
        } catch (err) {
            try {
                await transaction.rollback();
            } catch (rollbackError) {
                // Log rollback error but throw original error
                console.error('Rollback error:', rollbackError);
            }
            throw err;
        }
    }

    // Result parsing methods
    parseCountResult(result) {
        return result.recordset[0].count || 0;
    }

    parseCollectionsResult(result) {
        return result.recordset.map(row => row.name);
    }

    parseInsertResult(result, pk, record) {
        return {...record, pk, __timestamp: result.recordset[0].__timestamp};
    }

    parseUpdateResult(result) {
        return result.recordset[0];
    }

    parseDeleteResult(result, pk) {
        return result.recordset[0];
    }

    parseGetResult(result) {
        if (!result || !result.recordset || !result.recordset.length) return null;
        const row = result.recordset[0];
        try {
            return row.data ? JSON.parse(row.data) : null;
        } catch (e) {
            console.error('Error parsing JSON:', e);
            return null;
        }
    }

    parseFilterResults(result) {
        return result.recordset.map(row => ({
            ...JSON.parse(row.data),
            pk: row.pk,
            __timestamp: row.__timestamp
        }));
    }

    parseWriteKeyResult(result) {
        if (!result || !result.recordset || !result.recordset.length) return null;
        try {
            return result.recordset[0].data ? JSON.parse(result.recordset[0].data) : null;
        } catch (e) {
            console.error('Error parsing JSON:', e);
            return null;
        }
    }

    parseReadKeyResult(result) {
        if (!result || !result.recordset || !result.recordset.length) return null;
        try {
            return result.recordset[0].data ? JSON.parse(result.recordset[0].data) : null;
        } catch (e) {
            console.error('Error parsing JSON:', e);
            return null;
        }
    }

    isTableNotExistsError(error) {
        return error.number === 208;  // SQL Server invalid object name error
    }
}

module.exports = SQLServerStrategy;