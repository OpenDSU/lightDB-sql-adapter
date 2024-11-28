// strategies/PostgreSQLStrategy.js
const BaseStrategy = require('./BaseStrategy');
const crypto = require('crypto');

class PostgreSQLStrategy extends BaseStrategy {
    constructor() {
        super();
        this._storageDB = null;
    }

    async ensureCollectionsTable(connection) {
        const createTableQuery = `
        CREATE TABLE IF NOT EXISTS collections (
            name VARCHAR(255) PRIMARY KEY,
            indices JSONB
        );
    `;

        await this.executeQuery(connection, createTableQuery);
    }

    // Database schema operations
    createCollectionsTable() {
        return {
            query: `
                CREATE TABLE IF NOT EXISTS collections (
                    name VARCHAR(255) PRIMARY KEY,
                    indices JSONB
                );
            `,
            params: []
        };
    }

    createKeyValueTable(tableName) {
        return {
            query: `
                CREATE TABLE IF NOT EXISTS "${tableName}" (
                    pk TEXT PRIMARY KEY,
                    data JSONB,
                    __timestamp BIGINT
                );
            `,
            params: []
        };
    }

    createCollection(tableName, indicesList) {
        if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
            throw new Error(`Invalid table name: ${tableName}`);
        }

        const createTableQuery = `
        CREATE TABLE IF NOT EXISTS "${tableName}" (
            pk TEXT PRIMARY KEY,
            data JSONB,
            __timestamp BIGINT
        )
    `;

        const insertQuery = `
        INSERT INTO collections (name, indices)
        VALUES ($1, $2)
        ON CONFLICT (name) DO UPDATE
        SET indices = $2
    `;

        return [
            {query: createTableQuery},
            {
                query: insertQuery,
                params: [tableName, JSON.stringify(indicesList || [])]
            }
        ];
    }

    createIndex(tableName, index) {
        return `CREATE INDEX IF NOT EXISTS "${tableName}_${index}" ON "${tableName}" ((data ->>'${index}'));`;
    }

    addIndex(tableName, property) {
        return this.createIndex(tableName, property);
    }

    removeCollection(tableName) {
        return [
            {
                query: `DROP TABLE IF EXISTS "${tableName}"`,
                params: []
            },
            {
                query: this.deleteFromCollection(),
                params: [tableName]
            }
        ];
    }

    async removeCollectionAsync(connection, tableName) {
        return await this.executeTransaction(connection, this.removeCollection(tableName));
    }

    // Collection information
    getCollections() {
        return {query: 'SELECT name FROM collections'};
    }

    listCollections() {
        return this.getCollections();
    }

    count(tableName) {
        return `SELECT COUNT(*) as count FROM "${tableName}"`;
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
        return false; // PostgreSQL doesn't have a long-running refresh process
    }

    async refresh(connection, callback) {
        // PostgreSQL doesn't need explicit refresh
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
        return {
            query: `
            INSERT INTO "${tableName}" (pk, data, __timestamp)
            VALUES ($1, $2::jsonb, $3)
            RETURNING pk, data, __timestamp
        `
        };
    }

    updateRecord(tableName) {
        return {
            query: `
                UPDATE "${tableName}"
                SET data = $2::jsonb,
                __timestamp = $3
                WHERE pk = $1
                    RETURNING pk
                    , data
                    , __timestamp
            `
        };
    }

    deleteRecord(tableName) {
        return {
            query: `
            DELETE FROM "${tableName}"
            WHERE pk = $1
            RETURNING pk, data, __timestamp
        `
        };
    }

    getRecord(tableName) {
        return {
            query: `SELECT data, __timestamp FROM "${tableName}" WHERE pk = $1`
        };
    }

    getOneRecord(tableName) {
        return {
            query: `SELECT data, __timestamp FROM "${tableName}" LIMIT 1`
        };
    }

    getAllRecords(tableName) {
        return {
            query: `SELECT pk, data, __timestamp FROM "${tableName}"`
        };
    }

    filter(tableName, conditions, sort, max) {
        const orderField = sort?.field || '__timestamp';
        const orderDirection = sort?.direction || 'ASC';
        return {
            query: `
            SELECT pk, data, __timestamp
            FROM "${tableName}"
            ${conditions ? `WHERE ${conditions}` : ''}
            ORDER BY "${orderField}" ${orderDirection}
            ${max ? `LIMIT ${max}` : ''}
        `
        };
    }

    convertConditionsToLokiQuery(conditions) {
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
        return `(data->>'${field}')::numeric ${operator} ${value}`;
    }

    // Queue operations
    async addInQueue(connection, queueName, object, ensureUniqueness = false) {
        const hash = crypto.createHash('sha256').update(JSON.stringify(object)).digest('hex');
        let pk = hash;

        if (ensureUniqueness) {
            const random = crypto.randomBytes(5).toString('base64');
            pk = `${hash}_${Date.now()}_${random}`;
        }

        const params = [pk, JSON.stringify(object), Date.now()];
        const result = await this.executeQuery(connection, this.insertRecord(queueName), params);
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
        return {
            query: `
            INSERT INTO "${tableName}" (pk, data, __timestamp)
            VALUES ($1, $2::jsonb, $3)
            ON CONFLICT (pk) DO UPDATE 
            SET data = $2::jsonb, 
                __timestamp = $3
            RETURNING data
        `
        };
    }

    readKey(tableName, key) {
        return {
            query: `SELECT data, __timestamp FROM "${tableName}" WHERE pk = $1`
        };
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
        return {
            query: `
            INSERT INTO collections (name, indices)
            VALUES ($1, $2)
            ON CONFLICT (name) DO UPDATE
            SET indices = $2
        `,
            params: []  // Parameters will be added when executing
        };
    }

    deleteFromCollection() {
        return 'DELETE FROM collections WHERE name = $1';
    }

    // Result parsing methods
    parseCountResult(result) {
        return parseInt(result.rows[0].count);
    }

    parseCollectionsResult(result) {
        if (!result || !result.rows) return [];
        return result.rows.map(row => row.name);
    }

    parseInsertResult(result) {
        if (!result?.rows?.[0]) return null;
        const row = result.rows[0];
        try {
            return {
                ...JSON.parse(row.data),
                pk: row.pk,
                __timestamp: row.__timestamp
            };
        } catch (e) {
            console.error('Error parsing insert result:', e);
            return null;
        }
    }

    parseUpdateResult(result) {
        if (!result || !result.rows || result.rows.length === 0) return null;
        const row = result.rows[0];
        try {
            // Handle data that's already an object or needs parsing
            const data = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
            return {
                ...data,
                pk: row.pk,
                __timestamp: row.__timestamp
            };
        } catch (e) {
            console.error('Error parsing update result:', e);
            return null;
        }
    }

    parseDeleteResult(result) {
        if (!result || !result.rows || result.rows.length === 0) return null;
        return {
            pk: result.rows[0].pk,
            data: result.rows[0].data,
            __timestamp: result.rows[0].__timestamp
        };
    }

    parseGetResult(result) {
        if (!result?.rows?.[0]) return null;
        try {
            if (typeof result.rows[0].data === 'string') {
                return JSON.parse(result.rows[0].data);
            }
            return result.rows[0].data;
        } catch (e) {
            console.error('Error parsing get result:', e);
            return null;
        }
    }

    parseFilterResults(result) {
        if (!result?.rows) return [];
        return result.rows.map(row => {
            try {
                const data = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
                return {
                    ...data,
                    pk: row.pk,
                    __timestamp: row.__timestamp
                };
            } catch (e) {
                console.error('Error parsing filter row:', e);
                return null;
            }
        }).filter(Boolean);
    }


    parseWriteKeyResult(result) {
        if (!result?.rows?.[0]) return null;
        try {
            if (!result.rows[0].data) return null;
            const data = typeof result.rows[0].data === 'string'
                ? JSON.parse(result.rows[0].data)
                : result.rows[0].data;
            return data;
        } catch (e) {
            console.error('Error parsing write key result:', e);
            return null;
        }
    }

    parseReadKeyResult(result) {
        if (!result?.rows?.[0]) return null;
        try {
            if (!result.rows[0].data) return null;
            const data = typeof result.rows[0].data === 'string'
                ? JSON.parse(result.rows[0].data)
                : result.rows[0].data;
            return data;
        } catch (e) {
            console.error('Error parsing read key result:', e);
            return null;
        }
    }

    parseQueueResult(result) {
        if (!result || !result.rows || !result.rows.length === 0) return null;
        const row = result.rows[0];
        return JSON.parse(row.data);
    }

    // Transaction handling
    async executeQuery(connection, query, params = []) {
        const client = await connection.connect();
        try {
            let queryString, queryParams;

            if (typeof query === 'string') {
                queryString = query;
                queryParams = Array.isArray(params) ? params : [];
            } else if (query && typeof query === 'object') {
                queryString = query.query;
                queryParams = Array.isArray(params) ? params : [];
                if (query.params) {
                    queryParams = [...query.params];
                }
            } else {
                throw new Error('Invalid query object');
            }

            if (!queryString) {
                throw new Error('Query string is required');
            }

            const result = await client.query(queryString, queryParams);
            return {
                rows: result.rows,
                rowCount: result.rowCount,
                command: result.command
            };
        } finally {
            client.release();
        }
    }

    async executeTransaction(connection, queries) {
        const client = await connection.connect();
        try {
            await client.query('BEGIN');
            const results = [];

            for (const queryData of queries) {
                const query = typeof queryData === 'string' ? queryData : queryData.query;
                const params = queryData.params || [];

                const result = await client.query(query, params);
                results.push({
                    rows: result.rows,
                    rowCount: result.rowCount,
                    command: result.command
                });
            }

            await client.query('COMMIT');
            return results;
        } catch (err) {
            await client.query('ROLLBACK');
            throw err;
        } finally {
            client.release();
        }
    }
}

module.exports = PostgreSQLStrategy;