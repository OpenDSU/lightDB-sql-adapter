// strategies/PostgreSQLStrategy.js
const BaseStrategy = require('./BaseStrategy');
const crypto = require('crypto');

class PostgreSQLStrategy extends BaseStrategy {
    constructor() {
        super();
        this._storageDB = null;
        this.READ_WRITE_KEY_TABLE = "KeyValueTable";
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

    async createCollection(connection, tableName, indicesList) {
        if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
            throw new Error(`Invalid table name: ${tableName}`);
        }

        const queries = [
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

        return await this.executeTransaction(connection, queries);
    }

    async removeCollection(connection, tableName) {
        const queries = [
            {
                query: `DROP TABLE IF EXISTS "${tableName}"`
            },
            {
                query: `DELETE FROM collections WHERE name = $1`,
                params: [tableName]
            }
        ];
        return await this.executeTransaction(connection, queries);
    }

    async removeCollectionAsync(connection, tableName) {
        const queries = [
            {
                query: `DROP TABLE IF EXISTS "${tableName}"`
            },
            {
                query: `DELETE FROM collections WHERE name = $1`,
                params: [tableName]
            }
        ];
        return await this.executeTransaction(connection, queries);
    }

    async addIndex(connection, tableName, property) {
        const query = `CREATE INDEX IF NOT EXISTS "${tableName}_${property}" ON "${tableName}" ((data ->>'${property}'));`;
        return await this.executeQuery(connection, query);
    }

    // Collection information
    async getCollections(connection) {
        const query = `SELECT name FROM collections WHERE name IS NOT NULL AND name != ''`;
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
        let query = `
            SELECT pk, data, __timestamp 
            FROM "${tableName}"
        `;

        if (conditions && conditions.length > 0) {
            const whereClause = this._convertConditionsToLokiQuery(conditions);
            if (whereClause) {
                query += ` WHERE ${whereClause}`;
            }
        }

        query += ` ORDER BY __timestamp ${sort.toUpperCase()}`;

        if (max) {
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
    _convertConditionsToLokiQuery(conditions) {
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
                return `(data->>'${field}')::numeric ${operator} ${value}`;
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