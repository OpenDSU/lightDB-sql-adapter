// strategies/PostgreSQLStrategy.js
const BaseStrategy = require('./BaseStrategy');
const crypto = require('crypto');

class PostgreSQLStrategy extends BaseStrategy {
    constructor() {
        super();
        this._storageDB = null;
        this.READ_WRITE_KEY_TABLE = "KeyValueTable";
    }

    async ensureKeyValueTable(connection) {
        const createTableQuery = `
            CREATE TABLE IF NOT EXISTS "${this.READ_WRITE_KEY_TABLE}" (
                pk TEXT PRIMARY KEY,
                data JSONB,
                __timestamp BIGINT
            );
        `;
        await this.executeQuery(connection, createTableQuery);
    }

    async ensureCollectionsTable(connection) {
        const query = `
        CREATE TABLE IF NOT EXISTS collections (
            name VARCHAR(255) PRIMARY KEY,
            indices JSONB
        );
    `;
        try {
            await this.executeQuery(connection, query);
        } catch (error) {
            console.error('Error creating collections table:', error);
            throw error;
        }
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
        return {
            query: `SELECT name FROM collections WHERE name IS NOT NULL AND name != ''`,
            params: []
        };
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
        const result = {message: "Refresh completed"};
        if (typeof callback === 'function') {
            callback(null, result);
        }
        return result;
    }

    async refreshAsync(connection) {
        return {message: "Refresh completed"};
    }

    async saveDatabase(connection, callback) {
        try {
            // PostgreSQL auto-commits, so we just return a success message
            const result = {message: "Database saved"};
            if (typeof callback === 'function') {
                callback(null, result);
            }
            return result;
        } catch (error) {
            if (typeof callback === 'function') {
                callback(error);
            }
            throw error;
        }
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

    filter(tableName, conditions, sort = {}, max = null) {
        try {
            let query = `
            SELECT pk, data, __timestamp 
            FROM "${tableName}"`;

            // Convert the conditions to PostgreSQL syntax
            if (conditions && conditions.length > 0) {
                const whereClause = this.convertConditionsToLokiQuery(conditions);
                if (whereClause) {
                    query += ` WHERE ${whereClause}`;
                }
            }

            // Handle sorting
            const sortField = sort.field || '__timestamp';
            const direction = (sort.direction || 'ASC').toUpperCase();
            query += ` ORDER BY ${sortField === '__timestamp' ? '__timestamp' : `(data->>'${sortField}')`} ${direction}`;

            // Handle limit
            if (max) {
                query += ` LIMIT ${max}`;
            }

            if (process.env.DEBUG) {
                console.log('Generated filter query:', query);
                console.log('With conditions:', conditions);
            }

            return query;
        } catch (err) {
            throw new Error(`Error building filter query: ${err.message}`);
        }
    }

    convertConditionsToLokiQuery(conditions) {
        if (!conditions || !Array.isArray(conditions) || conditions.length === 0) {
            return '';
        }

        try {
            const andConditions = conditions.map(condition => {
                if (typeof condition !== 'string') {
                    throw new Error('Invalid condition format');
                }

                const parts = condition.trim().split(/\s+/);
                if (parts.length !== 3) {
                    throw new Error(`Invalid condition structure: ${condition}`);
                }

                const [field, operator, value] = parts;
                // Ensure numeric comparison for score field
                return `(data->>'${field}')::numeric ${operator} ${value}`;
            });

            return andConditions.join(' AND ');
        } catch (err) {
            throw new Error(`Error processing filter conditions: ${err.message}`);
        }
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
            INSERT INTO "${this.READ_WRITE_KEY_TABLE}" (pk, data, __timestamp)
            VALUES ($1, $2::jsonb, $3)
            ON CONFLICT (pk) DO UPDATE 
            SET data = $2::jsonb, 
                __timestamp = $3
            RETURNING data;
            `,
            params: []
        };
    }

    readKey(tableName) {
        return {
            query: `SELECT data, __timestamp FROM "${this.READ_WRITE_KEY_TABLE}" WHERE pk = $1`,
            params: []
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
            if (!result?.rows) {
                console.log('No rows in collection result:', result);
                return [];
            }
            return result.rows.map(row => row.name || '').filter(Boolean);
        } catch (e) {
            console.error('Error parsing collections:', e);
            return [];
        }
    }

    parseInsertResult(result, pk, record) {
        if (!result?.rows?.[0]) {
            console.error('Insert result:', result);
            throw new Error('Insert operation failed to return result');
        }

        const row = result.rows[0];

        // Ensure data is properly parsed
        let parsedData = row.data;
        if (typeof parsedData === 'string') {
            try {
                parsedData = JSON.parse(parsedData);
            } catch (e) {
                console.error('Error parsing data:', e);
                parsedData = record; // Fallback to original record
            }
        }

        return {
            ...parsedData,
            pk: row.pk || pk,
            __timestamp: parseInt(row.__timestamp) || Date.now()
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
        try {
            if (process.env.DEBUG) {
                console.log('=== Execute Query Debug ===');
                console.log('Query:', query);
                console.log('Params:', params);
                console.log('========================');
            }

            let queryText = '';
            let queryParams = params;

            if (typeof query === 'string') {
                queryText = query;
            } else if (query && typeof query === 'object') {
                queryText = query.query;
                if (query.params && (!params || !params.length)) {
                    queryParams = query.params;
                }
            }

            if (!queryText) {
                throw new Error('Query string is required');
            }

            // Execute the query using the pool directly
            const result = await connection.query(queryText, queryParams);

            // Clean the result before returning
            return JSON.parse(JSON.stringify(result));
        } catch (error) {
            const serializableError = new Error(error.message);
            serializableError.code = error.code;
            serializableError.type = 'DatabaseError';
            throw serializableError;
        }
    }

    async executeTransaction(connection, queries) {
        if (process.env.DEBUG) {
            console.log('=== Execute Transaction Debug ===');
            console.log('Connection:', connection ? 'exists' : 'null');
            console.log('Connection type:', connection ? Object.getPrototypeOf(connection).constructor.name : 'N/A');
            console.log('Connection methods:', connection ? Object.getOwnPropertyNames(Object.getPrototypeOf(connection)) : 'N/A');
            console.log('Queries:', queries);
            console.log('==============================');
        }

        try {
            // For PostgreSQL Pool, we need to acquire a client first
            const client = await connection.connect();

            if (process.env.DEBUG) {
                console.log('Client acquired:', client ? 'yes' : 'no');
                console.log('Client type:', client ? Object.getPrototypeOf(client).constructor.name : 'N/A');
            }

            try {
                await client.query('BEGIN');
                const results = [];

                for (const queryData of queries) {
                    if (process.env.DEBUG) {
                        console.log('Processing query:', queryData);
                    }

                    let queryText = '';
                    let params = [];

                    if (typeof queryData === 'string') {
                        queryText = queryData;
                    } else if (queryData && typeof queryData === 'object') {
                        queryText = queryData.query;
                        params = queryData.params || [];
                    } else {
                        throw new Error('Invalid query format');
                    }

                    if (!queryText) {
                        throw new Error('Query string is required');
                    }

                    if (process.env.DEBUG) {
                        console.log('Executing transaction query:', queryText);
                        console.log('With params:', params);
                    }

                    const result = await client.query(queryText, params);
                    results.push(result);
                }

                await client.query('COMMIT');
                return results;
            } catch (err) {
                console.error('Transaction error:', err);
                await client.query('ROLLBACK');
                throw err;
            } finally {
                if (process.env.DEBUG) {
                    console.log('Releasing client');
                }
                client.release();
            }
        } catch (err) {
            console.error('Transaction setup error:', err);
            throw err;
        }
    }
}

module.exports = PostgreSQLStrategy;