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

    async ensureTableExists(connection, tableName) {
        const query = `
        CREATE TABLE IF NOT EXISTS "${tableName}" (
            pk TEXT PRIMARY KEY,
            data JSONB,
            __timestamp BIGINT
        );
    `;
        await this.executeQuery(connection, query);
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

        return [
            {
                query: `
                CREATE TABLE IF NOT EXISTS "${tableName}" (
                    pk TEXT PRIMARY KEY,
                    data JSONB,
                    __timestamp BIGINT
                );
            `
            },
            {
                query: `
                INSERT INTO collections (name, indices)
                VALUES ($1, $2)
                ON CONFLICT (name) DO UPDATE
                SET indices = $2;
            `,
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
        try {
            if (connection && !connection.ended) {
                await connection.end();
            }
        } catch (error) {
            // Ignore connection already closed errors
            if (!error.message.includes('Cannot use a pool after calling end')) {
                throw error;
            }
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
        return `
        INSERT INTO "${tableName}" (pk, data, __timestamp)
        VALUES ($1, $2::jsonb, $3)
        RETURNING *
    `;
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
        `,
            params: []  // Empty array that will be filled during execution
        };
    }

    getRecord(tableName) {
        return {
            query: `SELECT data, __timestamp FROM "${tableName}" WHERE pk = $1`,
            params: []  // Parameters will be provided during execution
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
        const direction = (sort?.direction || 'ASC').toUpperCase();

        return {
            query: `
            SELECT pk, data, __timestamp 
            FROM "${tableName}"
            ${conditions ? `WHERE ${conditions}` : ''}
            ORDER BY ${orderField === '__timestamp' ? '__timestamp' : `(data->>'${orderField}')::numeric`} ${direction}
            ${max ? `LIMIT ${max}` : ''}
        `,
            params: []
        };
    }

    convertConditionsToLokiQuery(conditions) {
        if (!conditions || conditions.length === 0) {
            return '';
        }

        const condition = conditions[0];
        const [field, operator, value] = condition.split(/\s+/);
        return `SELECT pk, data, __timestamp FROM "test_filters" WHERE (data->>'${field}')::numeric ${operator} ${value}`;
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
        return {
            query: `SELECT COUNT(*)::int as count FROM "${queueName}"`
        };
    }

    listQueue(queueName, sortAfterInsertTime = 'asc', onlyFirstN) {
        return {
            query: `
            SELECT pk, data, __timestamp
            FROM "${queueName}"
            ORDER BY __timestamp ${sortAfterInsertTime.toUpperCase()}
            ${onlyFirstN ? `LIMIT ${onlyFirstN}` : ''}
        `
        };
    }

    getObjectFromQueue(queueName, hash) {
        return {
            query: `SELECT data, __timestamp FROM "${queueName}" WHERE pk = $1`,
            params: [hash]
        };
    }

    deleteObjectFromQueue(queueName, hash) {
        return {
            query: `
            DELETE FROM "${queueName}"
            WHERE pk = $1
            RETURNING pk, data, __timestamp
        `,
            params: [hash]
        };
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
        `,
            params: []
        };
    }

    readKey(tableName, key) {
        return {
            query: `SELECT data, __timestamp FROM "${tableName}" WHERE pk = $1`,
            params: [key]
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
        try {
            if (!result?.rows) return [];
            return result.rows.map(row => row.name || '').filter(Boolean);
        } catch (e) {
            console.error('Error parsing collections:', e);
            return [];
        }
    }

    parseInsertResult(result, pk, record) {
        if (!result?.[0]?.rows?.[0]) {
            console.error('Insert result:', result);
            throw new Error('Insert operation failed to return result');
        }

        const row = result[0].rows[0];
        const data = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;

        return {
            ...data,
            pk: row.pk,
            __timestamp: row.__timestamp
        };
    }

    parseUpdateResult(result) {
        if (!result?.rows?.[0]) return null;
        const row = result.rows[0];
        try {
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
        try {
            if (!result?.rows?.[0]?.data) return null;
            return result.rows[0].data;
        } catch (e) {
            console.error('Error parsing write key result:', e);
            return null;
        }
    }

    parseReadKeyResult(result) {
        try {
            if (!result?.rows?.[0]?.data) return null;
            return result.rows[0].data;
        } catch (e) {
            console.error('Error parsing read key result:', e);
            return null;
        }
    }

    parseQueueResult(result) {
        if (!result?.rows?.[0]) return null;
        try {
            return result.rows[0].data;
        } catch (e) {
            console.error('Error parsing queue result:', e);
            return null;
        }
    }

    parseQueueSizeResult(result) {
        try {
            if (!result?.rows?.[0]) return 0;
            return parseInt(result.rows[0].count, 10) || 0;
        } catch (e) {
            console.error('Error parsing queue size:', e);
            return 0;
        }
    }

    // Transaction handling
    async executeQuery(connection, query, params = []) {
        const client = await connection.connect();
        try {
            const queryText = typeof query === 'string' ? query : query.query;

            if (!queryText) {
                throw new Error('Query string is required');
            }

            // Debug logging
            if (process.env.DEBUG) {
                console.log('Query:', queryText);
                console.log('Params:', params);
            }

            const result = await client.query(queryText, params);
            return result;
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