const {Pool} = require('pg');
const crypto = require('crypto');

// class SQLAdapter (Type - connection registry for connectivity - to make in separate file) {
class PostgreSQLDb {
    constructor(connectionString, autoSaveInterval) {
        this.pool = new Pool({ connectionString });
        // this.connection = new connectionRegistry.Type;
        // Verify if "query" or other commands are general used for db or specific.
        this.logger = console;

        // Constants
        this.READ_WRITE_KEY_TABLE = "KeyValueTable";

        // Initialize schema immediately and store the promise
        this.initPromise = this.initializeSchema().catch(err => {
            this.logger.error('Failed to initialize PostgreSQL schema', err);
            throw err;
        });
    }

    async initializeSchema() {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');

            // Create collections table
            await client.query(`
                CREATE TABLE IF NOT EXISTS collections (
                    name VARCHAR(255) PRIMARY KEY,
                    indices JSONB
                );
            `);

            // Create key-value table
            await client.query(`
                CREATE TABLE IF NOT EXISTS "${this.READ_WRITE_KEY_TABLE}" (
                    pk TEXT PRIMARY KEY,
                    data JSONB,
                    __timestamp BIGINT
                );
            `);

            await client.query('COMMIT');
        } catch (err) {
            await client.query('ROLLBACK');
            throw err;
        } finally {
            client.release();
        }
    }

    async ensureCollectionTable(tableName) {
        const client = await this.pool.connect();
        try {
            await client.query(`
                CREATE TABLE IF NOT EXISTS "${tableName}" (
                    pk TEXT PRIMARY KEY,
                    data JSONB,
                    __timestamp BIGINT
                );
            `);
        } finally {
            client.release();
        }
    }

    async close() {
        await this.pool.end();
    }

    refresh(callback) {
        callback();
    }

    saveDatabase(callback) {
        callback(undefined, {message: "Database saved"});
    }

    count(tableName, callback) {
        this.pool.query(`SELECT COUNT(*) FROM "${tableName}"`)
            .then(result => callback(null, parseInt(result.rows[0].count)))
            .catch(callback);
    }

    getCollections(callback) {
        this.pool.query('SELECT name FROM collections')
            .then(result => callback(undefined, result.rows.map(row => row.name)))
            .catch(callback);
    }

    async createCollection(tableName, indicesList, callback) {
        if (typeof indicesList === "function") {
            callback = indicesList;
            indicesList = undefined;
        }

        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            await this.ensureCollectionTable(tableName);

            await client.query(
                'INSERT INTO collections (name, indices) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET indices = $2',
                [tableName, JSON.stringify(indicesList || [])]
            );

            if (indicesList && Array.isArray(indicesList)) {
                for (const index of indicesList) {
                    await client.query(`
                        CREATE INDEX IF NOT EXISTS "${tableName}_${index}"
                        ON "${tableName}" ((data ->>'${index}'));
                    `);
                }
            }

            await client.query('COMMIT');
            callback(undefined, {message: `Collection ${tableName} created`});
        } catch (err) {
            await client.query('ROLLBACK');
            callback(err);
        } finally {
            client.release();
        }
    }

    removeCollection(collectionName, callback) {
        this.pool.query(`DROP TABLE IF EXISTS "${collectionName}";`)
            .then(() => this.pool.query('DELETE FROM collections WHERE name = $1', [collectionName]))
            .then(() => callback())
            .catch(callback);
    }

    insertRecord(tableName, pk, record, callback) {
        this.ensureCollectionTable(tableName)
            .then(() => this.pool.query(
                `SELECT pk FROM "${tableName}" WHERE pk = $1`,
                [pk]
            ))
            .then(exists => {
                if (exists.rows.length > 0) {
                    throw new Error(`A record with pk ${pk} already exists in ${tableName}`);
                }

                const timestamp = Date.now();
                return this.pool.query(
                    `INSERT INTO "${tableName}" (pk, data, __timestamp) VALUES ($1, $2, $3) RETURNING *`,
                    [pk, record, timestamp]
                );
            })
            .then(result => callback(null, {...result.rows[0].data, pk}))
            .catch(callback);
    }

    updateRecord(tableName, pk, record, callback) {
        const timestamp = Date.now();
        this.pool.query(
            `UPDATE "${tableName}"
             SET data = $1,
                 __timestamp = $2
             WHERE pk = $3 RETURNING *`,
            [record, timestamp, pk]
        )
            .then(result => {
                if (result.rows.length === 0 && record.__fallbackToInsert) {
                    delete record.__fallbackToInsert;
                    return this.insertRecord(tableName, pk, record, callback);
                }
                callback(null, {...record, pk, __timestamp: timestamp});
            })
            .catch(callback);
    }

    deleteRecord(tableName, pk, callback) {
        this.pool.query(
            `DELETE FROM "${tableName}" WHERE pk = $1 RETURNING *`,
            [pk]
        )
            .then(result => {
                if (result.rows.length === 0) {
                    return callback(undefined, {pk});
                }
                callback(null, {...result.rows[0].data, pk});
            })
            .catch(callback);
    }

    getRecord(tableName, pk, callback) {
        this.pool.query(
            `SELECT data, __timestamp FROM "${tableName}" WHERE pk = $1`,
            [pk]
        )
            .then(result => {
                if (result.rows.length === 0) {
                    return callback(null, null);
                }
                callback(null, {
                    ...result.rows[0].data,
                    pk,
                    __timestamp: result.rows[0].__timestamp
                });
            })
            .catch(callback);
    }

    filter(tableName, filterConditions, sort, max, callback) {
        if (typeof filterConditions === "function") {
            callback = filterConditions;
            filterConditions = undefined;
            sort = "asc";
            max = Infinity;
        }

        let query = `SELECT pk, data, __timestamp FROM "${tableName}"`;
        const params = [];
        let paramIndex = 1;

        if (filterConditions && filterConditions.length) {
            const conditions = Array.isArray(filterConditions) ? filterConditions : [filterConditions];
            query += ' WHERE ' + conditions.map(condition => {
                const [field, operator, value] = condition.split(/\s+/);
                params.push(value.replace(/['"]/g, ''));
                return `(data->>'${field}')::numeric ${operator} $${paramIndex++}`;
            }).join(' AND ');
        }

        // Fix the ORDER BY clause to use the JSONB field
        const sortField = filterConditions?.[0]?.split(' ')?.[0] || '__timestamp';
        if (sortField === '__timestamp') {
            query += ` ORDER BY __timestamp ${sort === 'desc' ? 'DESC' : 'ASC'}`;
        } else {
            query += ` ORDER BY (data->>'${sortField}')::numeric ${sort === 'desc' ? 'DESC' : 'ASC'}`;
        }

        if (max && max !== Infinity) {
            query += ` LIMIT ${max}`;
        }

        this.pool.query(query, params)
            .then(result => {
                const records = result.rows.map(row => ({
                    ...row.data,
                    pk: row.pk,
                    __timestamp: row.__timestamp
                }));
                callback(null, records);
            })
            .catch(callback);
    }

    getAllRecords(tableName, callback) {
        this.pool.query(`SELECT pk, data, __timestamp FROM "${tableName}"`)
            .then(result => {
                const records = result.rows.map(row => ({
                    ...row.data,
                    pk: row.pk,
                    __timestamp: row.__timestamp
                }));
                callback(null, records);
            })
            .catch(callback);
    }

    // Queue operations
    addInQueue(queueName, object, ensureUniqueness, callback) {
        if (typeof ensureUniqueness === "function") {
            callback = ensureUniqueness;
            ensureUniqueness = false;
        }

        const hash = crypto.createHash('sha256').update(JSON.stringify(object)).digest('hex');
        let pk = hash;

        if (ensureUniqueness) {
            const random = crypto.randomBytes(5).toString('base64');
            pk = `${hash}_${Date.now()}_${random}`;
        }

        this.insertRecord(queueName, pk, object, (err) => callback(err, pk));
    }

    queueSize(queueName, callback) {
        this.count(queueName, callback);
    }

    listQueue(queueName, sortAfterInsertTime, onlyFirstN, callback) {
        if (typeof sortAfterInsertTime === "function") {
            callback = sortAfterInsertTime;
            sortAfterInsertTime = "asc";
            onlyFirstN = undefined;
        }

        let query = `SELECT pk FROM "${queueName}" ORDER BY __timestamp ${
            sortAfterInsertTime === "desc" ? "DESC" : "ASC"
        }`;

        if (onlyFirstN) {
            query += ` LIMIT ${onlyFirstN}`;
        }

        this.pool.query(query)
            .then(result => callback(null, result.rows.map(row => row.pk)))
            .catch(err => {
                if (err.code === '42P01') { // table does not exist
                    callback(undefined, []);
                    return;
                }
                callback(err);
            });
    }

    getObjectFromQueue(queueName, hash, callback) {
        return this.getRecord(queueName, hash, callback);
    }

    deleteObjectFromQueue(queueName, hash, callback) {
        return this.deleteRecord(queueName, hash, callback);
    }

    // Key-Value operations
    async writeKey(key, value, callback) {
        try {
            // Wait for initialization to complete
            await this.initPromise;

            let valueObject = {
                type: typeof value,
                value: value
            };

            if (Buffer.isBuffer(value)) {
                valueObject = {
                    type: "buffer",
                    value: value.toString()
                };
            } else if (value !== null && typeof value === "object") {
                valueObject = {
                    type: "object",
                    value: JSON.stringify(value)
                };
            }

            // Update if exists, insert if not
            const timestamp = Date.now();
            const result = await this.pool.query(
                `INSERT INTO "${this.READ_WRITE_KEY_TABLE}" (pk, data, __timestamp)
                VALUES ($1, $2, $3)
                ON CONFLICT (pk) DO UPDATE 
                SET data = $2, __timestamp = $3
                RETURNING data`,
                [key, valueObject, timestamp]
            );

            callback(null, result.rows[0].data);
        } catch (err) {
            callback(err);
        }
    }

    async readKey(key, callback) {
        try {
            // Wait for initialization to complete
            await this.initPromise;

            const result = await this.pool.query(
                `SELECT data FROM "${this.READ_WRITE_KEY_TABLE}" WHERE pk = $1`,
                [key]
            );

            if (result.rows.length === 0) {
                callback(null, null);
                return;
            }

            callback(null, result.rows[0].data);
        } catch (err) {
            callback(err);
        }
    }
}

module.exports = PostgreSQLDb;