// strategies/MySQLStrategy.js
const BaseStrategy = require('./BaseStrategy');
const crypto = require('crypto');

class MySQLStrategy extends BaseStrategy {
    constructor() {
        super();
        this._storageDB = null;
    }

    // Database schema operations
    createCollectionsTable() {
        return `
            CREATE TABLE IF NOT EXISTS collections (
                name VARCHAR(255) PRIMARY KEY,
                indices TEXT
            );
        `;
    }

    createKeyValueTable(tableName) {
        return `
            CREATE TABLE IF NOT EXISTS \`${tableName}\` (
                pk VARCHAR(255) PRIMARY KEY,
                data JSON,
                __timestamp BIGINT
            );
        `;
    }

    createCollection(tableName, indicesList) {
        const queries = [
            {
                query: this.createKeyValueTable(tableName),
                params: []
            },
            {
                query: this.insertCollection(),
                params: {name: tableName, indices: JSON.stringify(indicesList || [])}
            }
        ];

        if (indicesList && Array.isArray(indicesList)) {
            for (const index of indicesList) {
                queries.push({
                    query: this.createIndex(tableName, index),
                    params: []
                });
            }
        }

        return queries;
    }

    createIndex(tableName, index) {
        return `
            CREATE INDEX IF NOT EXISTS idx_${tableName}_${index}
            ON ${tableName}((CAST(JSON_EXTRACT(data, '$.${index}') AS SIGNED)));
        `;
    }

    addIndex(tableName, property) {
        return this.createIndex(tableName, property);
    }

    removeCollection(tableName) {
        return [
            {
                query: `DROP TABLE IF EXISTS \`${tableName}\``,
                params: []
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
        return `SELECT COUNT(*) as count FROM \`${tableName}\``;
    }

    // Database state management
    async close(connection) {
        return await this.closeConnection(connection);
    }

    async closeConnection(connection) {
        if (connection) {
            await connection.end();
        }
    }

    refreshInProgress() {
        return false; // MySQL doesn't have a long-running refresh process
    }

    async refresh(connection, callback) {
        // MySQL doesn't need explicit refresh
        callback();
    }

    async refreshAsync(connection) {
        // MySQL doesn't need explicit refresh
        return Promise.resolve();
    }

    async saveDatabase(connection, callback) {
        // MySQL auto-saves, no explicit action needed
        callback(undefined, {message: "Database saved"});
    }

    // Record operations
    insertRecord(tableName) {
        return `
            INSERT INTO \`${tableName}\` (pk, data, __timestamp)
            VALUES (?, ?, ?)
        `;
    }

    updateRecord(tableName) {
        return `
            UPDATE \`${tableName}\`
            SET data        = ?,
                __timestamp = ?
            WHERE pk = ?
        `;
    }

    deleteRecord(tableName) {
        return `DELETE FROM \`${tableName}\` WHERE pk = ?`;
    }

    getRecord(tableName) {
        return `SELECT data, __timestamp FROM \`${tableName}\` WHERE pk = ?`;
    }

    getOneRecord(tableName) {
        return `SELECT data, __timestamp FROM \`${tableName}\` LIMIT 1`;
    }

    getAllRecords(tableName) {
        return `SELECT pk, data, __timestamp FROM \`${tableName}\``;
    }

    filter(tableName, conditions, sort, max) {
        return `
            SELECT pk, data, __timestamp FROM \`${tableName}\`
            ${conditions ? `WHERE ${conditions}` : ''}
            ORDER BY ${sort.field} ${sort.direction}
            ${max ? `LIMIT ${max}` : ''}
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
        return `CAST(JSON_EXTRACT(data, '$.${field}') AS DECIMAL) ${operator} ${value.replace(/['"]/g, '')}`;
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
            INSERT INTO \`${tableName}\` (pk, data, __timestamp)
            VALUES (?, ?, ?)
            ON DUPLICATE KEY UPDATE
            data = VALUES(data), __timestamp = VALUES(__timestamp)
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

    // Transaction handling
    async executeQuery(connection, query, params = {}) {
        // Convert params object to array and handle parameter binding
        const values = [];
        let processedQuery = query;

        // Handle named parameters (@param) to MySQL style (?)
        processedQuery = query.replace(/@(\w+)/g, (match, paramName) => {
            if (params[paramName] !== undefined) {
                values.push(params[paramName]);
                return '?';
            }
            return match;
        });

        return await connection.query(processedQuery, values);
    }

    async executeTransaction(connection, queries) {
        const conn = await connection.getConnection();
        try {
            await conn.beginTransaction();
            const results = [];

            for (const {query, params = {}} of queries) {
                // Process each query's parameters
                const values = [];
                let processedQuery = query;

                processedQuery = query.replace(/@(\w+)/g, (match, paramName) => {
                    if (params[paramName] !== undefined) {
                        values.push(params[paramName]);
                        return '?';
                    }
                    return match;
                });

                results.push(await conn.query(processedQuery, values));
            }

            await conn.commit();
            return results;
        } catch (err) {
            await conn.rollback();
            throw err;
        } finally {
            conn.release();
        }
    }

    // Collection maintenance
    insertCollection() {
        return `
        INSERT INTO collections (name, indices)
        VALUES (?, ?)
        ON DUPLICATE KEY UPDATE indices = ?;
    `;
    }

    deleteFromCollection() {
        return 'DELETE FROM collections WHERE name = ?';
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

    parseCountResult(result) {
        return result[0].count;
    }

    parseCollectionsResult(result) {
        return result.map(row => row.name);
    }

    parseInsertResult(result, pk, record) {
        return {...record, pk};
    }

    parseUpdateResult(result) {
        return result.affectedRows > 0;
    }

    parseDeleteResult(result, pk) {
        return result.affectedRows > 0 ? {pk} : null;
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
        if (!result || !Array.isArray(result)) return [];
        return result.map(row => {
            try {
                return {
                    pk: row.pk,
                    ...JSON.parse(row.data || '{}'),
                    __timestamp: row.__timestamp
                };
            } catch (e) {
                return null;
            }
        }).filter(Boolean);
    }

}

module.exports = MySQLStrategy;