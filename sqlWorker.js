// sqlWorker.js
const {parentPort} = require('worker_threads');
const {StrategyFactory} = require('./strategyFactory');
const ConnectionRegistry = require('./connectionRegistry');
const crypto = require('crypto');

let connection = null;
let strategy = null;

parentPort.on('message', async (message) => {
    try {
        const {type, method, params} = message;

        if (!connection) {
            connection = await ConnectionRegistry.createConnection(type);
            strategy = StrategyFactory.createStrategy(type);
            await initializeSchema();
        }

        const result = await executeMethod(method, params);
        const serializedResult = serializeResult(result);
        parentPort.postMessage({success: true, result: serializedResult});
    } catch (error) {
        console.error('Worker error:', error);
        parentPort.postMessage({
            success: false,
            error: error.message || String(error),
            stack: error.stack
        });
    }
});

async function initializeSchema() {
    const READ_WRITE_KEY_TABLE = "KeyValueTable";
    try {
        // Create collection table
        await strategy.executeQuery(connection, strategy.createCollectionsTable());

        // Create key-value table
        await strategy.executeQuery(connection, strategy.createKeyValueTable(READ_WRITE_KEY_TABLE));
    } catch (error) {
        // Ignore duplicate table errors
        if (!error.message.includes('duplicate key value violates unique constraint')) {
            throw error;
        }
    }
}

async function executeMethod(method, params) {
    switch (method) {
        case 'executeQuery': {
            const [queryMethod, queryParams] = params;

            // For methods that need table creation
            if (['insertRecord', 'filter', 'queueSize', 'listQueue'].includes(queryMethod)) {
                const tableName = queryParams[0];
                await ensureTableExists(tableName);
            }

            // Handle collection operations
            if (queryMethod === 'createCollection') {
                const [tableName, indicesList] = queryParams;
                const queries = strategy[queryMethod](tableName, indicesList);
                return await strategy.executeTransaction(connection, queries);
            }

            if (queryMethod === 'getCollections') {
                const query = strategy[queryMethod]();
                return await strategy.executeQuery(connection, query.query, query.params || []);
            }

            // Handle record operations
            if (['insertRecord', 'updateRecord'].includes(queryMethod)) {
                const query = strategy[queryMethod](queryParams[0]);
                const [tableName, pk, recordData, timestamp] = queryParams;
                return await strategy.executeQuery(connection, query, [pk, recordData, timestamp]);
            }

            if (queryMethod === 'deleteRecord') {
                const [tableName, pk] = queryParams;
                const query = strategy[queryMethod](tableName);
                return await strategy.executeQuery(connection, query.query, [pk]);
            }

            if (queryMethod === 'getRecord') {
                const [tableName, pk] = queryParams;
                const query = strategy[queryMethod](tableName);
                return await strategy.executeQuery(connection, query.query, [pk]);
            }

            // Handle filter operations
            if (queryMethod === 'filter') {
                const [tableName, conditions, sort, max] = queryParams;
                await ensureTableExists(tableName);

                // Convert conditions to WHERE clause
                const whereClause = conditions && conditions.length > 0
                    ? strategy.convertConditionsToLokiQuery(conditions)
                    : '';

                // Get complete query
                const query = strategy.filter(tableName, whereClause, sort, max);

                // Execute the complete query
                if (typeof query === 'object' && query.query) {
                    return await strategy.executeQuery(connection, query.query, query.params || []);
                }
                return await strategy.executeQuery(connection, query, []);
            }

            // Handle queue operations
            if (queryMethod === 'queueSize' || queryMethod === 'listQueue') {
                const tableName = queryParams[0];
                await ensureTableExists(tableName);
            }
            if (queryMethod === 'queueSize') {
                const query = strategy.queueSize(queryParams[0]);
                return await strategy.executeQuery(connection, query.query, query.params || []);
            }

            if (queryMethod === 'listQueue') {
                const [queueName, sortAfterInsertTime, onlyFirstN] = queryParams;
                const query = strategy.listQueue(queueName, sortAfterInsertTime, onlyFirstN);
                return await strategy.executeQuery(connection, query.query, query.params || []);
            }

            if (queryMethod === 'getObjectFromQueue') {
                const [queueName, hash] = queryParams;
                const query = strategy.getObjectFromQueue(queueName);
                return await strategy.executeQuery(connection, query.query, [hash]);
            }

            if (queryMethod === 'deleteObjectFromQueue') {
                const [queueName, hash] = queryParams;
                const query = strategy.deleteObjectFromQueue(queueName);
                return await strategy.executeQuery(connection, query.query, [hash]);
            }

            // Handle key-value operations
            if (queryMethod === 'writeKey') {
                const [tableName, key, value, timestamp] = queryParams;
                const query = strategy[queryMethod](tableName);
                return await strategy.executeQuery(connection, query.query, [key, value, timestamp]);
            }

            if (queryMethod === 'readKey') {
                const [tableName, key] = queryParams;
                const query = strategy[queryMethod](tableName);
                return await strategy.executeQuery(connection, query.query, [key]);
            }

            // Default query execution
            const query = strategy[queryMethod](...queryParams);
            if (typeof query === 'object' && query.query) {
                return await strategy.executeQuery(connection, query.query, query.params || []);
            }
            return await strategy.executeQuery(connection, query, queryParams.slice(1));
        }

        case 'executeTransaction': {
            const [transactionMethod, transactionParams] = params;
            const queries = strategy[transactionMethod](...transactionParams);
            return await strategy.executeTransaction(connection, queries);
        }

        case 'addInQueue': {
            const [queueName, object, ensureUniqueness] = params;
            // First create table
            await strategy.executeQuery(connection, strategy.createKeyValueTable(queueName));

            const hash = crypto.createHash('sha256').update(JSON.stringify(object)).digest('hex');
            const pk = ensureUniqueness ? `${hash}_${Date.now()}_${crypto.randomBytes(5).toString('hex')}` : hash;

            const query = strategy.insertRecord(queueName);
            await strategy.executeQuery(
                connection,
                query,
                [pk, JSON.stringify(object), Date.now()]
            );
            return pk;
        }

        case 'close':
            if (connection) {
                await strategy.closeConnection(connection);
                connection = null;
                strategy = null;
            }
            return true;

        case 'parseResult': {
            const [parseMethod, result] = params;
            return strategy[parseMethod](result);
        }

        default:
            throw new Error(`Unknown method: ${method}`);
    }
}

function serializeResult(result) {
    if (!result) return null;

    // Handle pg.Result objects
    if (result.rows) {
        return {
            rows: result.rows,
            rowCount: result.rowCount,
            command: result.command
        };
    }

    // Handle arrays
    if (Array.isArray(result)) {
        return result.map(serializeResult);
    }

    // Handle other objects
    if (typeof result === 'object') {
        const serialized = {};
        for (const [key, value] of Object.entries(result)) {
            if (typeof value === 'function') continue;
            if (value && typeof value === 'object') {
                serialized[key] = serializeResult(value);
            } else {
                serialized[key] = value;
            }
        }
        return serialized;
    }

    return result;
}

async function ensureTableExists(tableName) {
    try {
        await strategy.executeQuery(connection, strategy.createKeyValueTable(tableName));
    } catch (error) {
        if (!error.message.includes('duplicate key value violates unique constraint')) {
            throw error;
        }
    }
}

parentPort.once('close', async () => {
    if (connection) {
        try {
            await strategy.closeConnection(connection);
            connection = null;
            strategy = null;
        } catch (error) {
            // Ignore connection already closed errors
            if (!error.message.includes('Cannot use a pool after calling end')) {
                console.error('Error closing connection:', error);
            }
        }
    }
});