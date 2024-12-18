// strategies/PostgreSQLStrategy.js
const BaseStrategy = require('./BaseStrategy');
const crypto = require('crypto');

class PostgreSQLStrategy extends BaseStrategy {
    constructor() {
        super();
        this._storageDB = null;
        this.READ_WRITE_KEY_TABLE = "KeyValueTable";
    }

    async cleanupDatabase(connection) {
        console.log('DEBUG: Starting database cleanup');

        // Get all tables
        const query = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        AND table_name NOT LIKE 'pg_%'
    `;

        const result = await this.executeQuery(connection, query);
        const tables = result.rows.map(row => row.table_name);

        // Drop each table
        for (const table of tables) {
            console.log('DEBUG: Dropping table:', table);
            await this.executeQuery(connection, `DROP TABLE IF EXISTS "${table}" CASCADE`);
        }

        console.log('DEBUG: Database cleanup completed');
    }

    async createDatabase(connection) {
        try {
            // Clean up any existing tables first
            await this.cleanupDatabase(connection);
            return {success: true, message: "Database reset and ready"};
        } catch (error) {
            console.error('Error in createDatabase:', error);
            throw error;
        }
    }

    // Database schema operations
    async ensureKeyValueTable(connection) {
        const query = `
            CREATE TABLE IF NOT EXISTS "${this.READ_WRITE_KEY_TABLE}" (
                pk TEXT PRIMARY KEY,
                data JSONB,
                __timestamp BIGINT
            );
        `;
        await this.executeQuery(connection, query);
    }

    async createCollection(connection, tableName, indicesList) {
        if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
            throw new Error(`Invalid table name: ${tableName}`);
        }

        const query = `
        CREATE TABLE IF NOT EXISTS "${tableName}" (
            pk TEXT PRIMARY KEY,
            data JSONB,
            __timestamp BIGINT
        );
    `;

        return await this.executeQuery(connection, query);
    }

    async removeCollection(connection, tableName) {
        const query = `DROP TABLE IF EXISTS "${tableName}"`;
        return await this.executeQuery(connection, query);
    }

    async removeCollectionAsync(connection, tableName) {
        const query = `DROP TABLE IF EXISTS "${tableName}"`;
        return await this.executeQuery(connection, query);
    }

    async addIndex(connection, tableName, property) {
        const query = `CREATE INDEX IF NOT EXISTS "${tableName}_${property}" ON "${tableName}" ((data ->>'${property}'));`;
        return await this.executeQuery(connection, query);
    }

    // Collection information
    async getCollections(connection) {
        const query = `
        SELECT table_name as name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        AND table_name != 'KeyValueTable'
    `;
        const result = await this.executeQuery(connection, query);
        return result.rows.map(row => row.name);
    }

    async count(connection, tableName) {
        const query = `SELECT COUNT(*) as count FROM "${tableName}"`;
        const result = await this.executeQuery(connection, query);
        return parseInt(result.rows[0].count);
    }

    // Database state management
    async close(connection) {
        try {
            if (connection && !connection.ended) {
                await connection.end();
            }
        } catch (error) {
            if (!error.message.includes('Cannot use a pool after calling end')) {
                throw error;
            }
        }
    }

    async refresh(connection) {
        return {message: "Refresh completed"};
    }

    async refreshAsync(connection) {
        return {message: "Refresh completed"};
    }

    async saveDatabase(connection) {
        return {message: "Database saved"};
    }

    // Record operations
    async insertRecord(connection, tableName, pk, record) {
        const query = `
            INSERT INTO "${tableName}" (pk, data, __timestamp)
            VALUES ($1, $2::jsonb, $3)
            RETURNING *
        `;
        const timestamp = Date.now();
        const result = await this.executeQuery(connection, query, [pk, JSON.stringify(record), timestamp]);

        if (!result?.rows?.[0]) {
            throw new Error('Insert operation failed to return result');
        }

        const row = result.rows[0];
        return {
            ...row.data,
            pk: row.pk,
            __timestamp: parseInt(row.__timestamp) || timestamp
        };
    }

    async updateRecord(connection, tableName, pk, record) {
        const query = `
            UPDATE "${tableName}"
            SET data = $2::jsonb,
                __timestamp = $3
            WHERE pk = $1
                RETURNING pk
                , data
                , __timestamp
        `;
        const timestamp = Date.now();
        const result = await this.executeQuery(connection, query, [pk, JSON.stringify(record), timestamp]);

        if (!result?.rows?.[0]) return null;
        const row = result.rows[0];
        return {
            ...row.data,
            pk: row.pk,
            __timestamp: row.__timestamp
        };
    }

    async deleteRecord(connection, tableName, pk) {
        const query = `
            DELETE FROM "${tableName}"
            WHERE pk = $1
            RETURNING pk, data, __timestamp
        `;
        const result = await this.executeQuery(connection, query, [pk]);

        if (!result?.rows?.[0]) return null;
        return {
            pk: result.rows[0].pk,
            data: result.rows[0].data,
            __timestamp: result.rows[0].__timestamp
        };
    }

    async getRecord(connection, tableName, pk) {
        const query = `SELECT data, __timestamp FROM "${tableName}" WHERE pk = $1`;
        const result = await this.executeQuery(connection, query, [pk]);

        if (!result?.rows?.[0]) return null;
        return result.rows[0].data;
    }

    async getOneRecord(connection, tableName) {
        const query = `SELECT data, __timestamp FROM "${tableName}" LIMIT 1`;
        const result = await this.executeQuery(connection, query);

        if (!result?.rows?.[0]) return null;
        return result.rows[0].data;
    }

    async getAllRecords(connection, tableName) {
        const query = `SELECT pk, data, __timestamp FROM "${tableName}"`;
        const result = await this.executeQuery(connection, query);

        return result.rows.map(row => ({
            ...row.data,
            pk: row.pk,
            __timestamp: row.__timestamp
        }));
    }

    async filter(connection, tableName, conditions = [], sort = 'asc', max = null) {
        // Handle string condition by converting to array
        if (typeof conditions === "string") {
            conditions = [conditions];
        }

        // Handle when conditions is a function (callback)
        if (typeof conditions === "function") {
            max = Infinity;
            sort = "asc";
            conditions = [];
        }

        // Handle when sort is a function (callback)
        if (typeof sort === "function") {
            max = Infinity;
            sort = "asc";
        }

        // Handle when max is a function (callback)
        if (typeof max === "function") {
            max = Infinity;
        }

        // Set max to Infinity if not provided
        if (!max) {
            max = Infinity;
        }

        let query = `
        SELECT pk, data, __timestamp 
        FROM "${tableName}"
        `;

        if (conditions && conditions.length > 0) {
            const whereClause = this._convertToSQLQuery(conditions);
            if (whereClause) {
                query += ` WHERE ${whereClause}`;
            }
        }

        query += ` ORDER BY __timestamp ${sort.toUpperCase()}`;

        if (max && max !== Infinity) {
            query += ` LIMIT ${max}`;
        }

        const result = await this.executeQuery(connection, query);

        return result.rows.map(row => ({
            ...row.data,
            pk: row.pk,
            __timestamp: row.__timestamp
        }));
    }

    // Queue operations
    async addInQueue(connection, queueName, object, ensureUniqueness = false) {
        const hash = crypto.createHash('sha256').update(JSON.stringify(object)).digest('hex');
        let pk = hash;

        if (ensureUniqueness) {
            const random = crypto.randomBytes(5).toString('base64');
            pk = `${hash}_${Date.now()}_${random}`;
        }

        const query = `
            INSERT INTO "${queueName}" (pk, data, __timestamp)
            VALUES ($1, $2::jsonb, $3)
            RETURNING *
        `;

        await this.executeQuery(connection, query, [pk, JSON.stringify(object), Date.now()]);
        return pk;
    }

    async queueSize(connection, queueName) {
        const query = `SELECT COUNT(*)::int as count FROM "${queueName}"`;
        const result = await this.executeQuery(connection, query);
        return parseInt(result.rows[0].count, 10) || 0;
    }

    async listQueue(connection, queueName, sortAfterInsertTime = 'asc', onlyFirstN = null) {
        const query = `
            SELECT pk, data, __timestamp
            FROM "${queueName}"
            ORDER BY __timestamp ${sortAfterInsertTime.toUpperCase()}
            ${onlyFirstN ? `LIMIT ${onlyFirstN}` : ''}
        `;

        const result = await this.executeQuery(connection, query);
        return result.rows.map(row => row.pk);
    }

    async getObjectFromQueue(connection, queueName, hash) {
        const query = `SELECT data, __timestamp FROM "${queueName}" WHERE pk = $1`;
        const result = await this.executeQuery(connection, query, [hash]);

        if (!result?.rows?.[0]) return null;
        return result.rows[0].data;
    }

    async deleteObjectFromQueue(connection, queueName, hash) {
        const query = `
            DELETE FROM "${queueName}"
            WHERE pk = $1
            RETURNING pk, data, __timestamp
        `;

        const result = await this.executeQuery(connection, query, [hash]);
        if (!result?.rows?.[0]) return null;
        return result.rows[0].data;
    }

    // Key-value operations
    async writeKey(connection, key, value) {
        const query = `
            INSERT INTO "${this.READ_WRITE_KEY_TABLE}" (pk, data, __timestamp)
            VALUES ($1, $2::jsonb, $3)
            ON CONFLICT (pk) DO UPDATE 
            SET data = $2::jsonb, 
                __timestamp = $3
            RETURNING data;
        `;

        const result = await this.executeQuery(connection, query, [key, value, Date.now()]);
        if (!result?.rows?.[0]?.data) return null;
        return result.rows[0].data;
    }

    async readKey(connection, key) {
        const query = `SELECT data, __timestamp FROM "${this.READ_WRITE_KEY_TABLE}" WHERE pk = $1`;
        const result = await this.executeQuery(connection, query, [key]);

        if (!result?.rows?.[0]?.data) return null;
        return result.rows[0].data;
    }

    // Helper methods
    _convertToSQLQuery(conditions) {
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

                let [field, operator, value] = parts;

                // Normalize operator
                operator = operator.replace('==', '=');

                // Handle IS NULL and IS NOT NULL
                if (value.toLowerCase() === 'null') {
                    return `data->>'${field}' IS NULL`;
                }

                // Handle LIKE/ILIKE operator
                if (operator.toLowerCase() === 'like') {
                    // Handle a regex pattern for word boundary
                    if (value.includes('\\b')) {
                        // Convert \btest\w* to proper PostgreSQL regex
                        value = value.replace(/\\b/g, '\\m');  // \m for word boundary in PostgreSQL
                        value = value.replace(/\\w\*/g, '[a-zA-Z0-9_]*');  // \w* to PostgreSQL pattern
                        // Escape single quotes and wrap in quotes
                        value = `'${value.replace(/'/g, "''")}'`;
                    } else {
                        value = `'${value.replace(/'/g, "''")}'`;
                    }
                    return `data->>'${field}' ~ ${value}`;  // Using ~ for regex match
                }

                // Handle numeric comparisons
                const numericValue = parseFloat(value);
                if (!isNaN(numericValue)) {
                    // Cast both sides to numeric for comparison
                    return `(data->>'${field}')::numeric ${operator} ${numericValue}::numeric`;
                }

                // Handle boolean values
                if (value.toLowerCase() === 'true' || value.toLowerCase() === 'false') {
                    return `(data->>'${field}')::boolean = ${value.toLowerCase()}`;
                }

                // For string values (handle quotes)
                // If value is not wrapped in quotes, add them
                if (!value.startsWith("'") && !value.startsWith('"')) {
                    value = `'${value}'`;
                }
                return `data->>'${field}' ${operator} ${value}`;
            });

            return andConditions.join(' AND ');
        } catch (err) {
            throw new Error(`Error processing filter conditions: ${err.message}`);
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

            const result = await connection.query(queryText, queryParams);
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
            console.log('Queries:', queries);
            console.log('==============================');
        }

        const client = await connection.connect();

        try {
            await client.query('BEGIN');
            const results = [];

            for (const queryData of queries) {
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
            client.release();
        }
    }
}

module.exports = PostgreSQLStrategy;