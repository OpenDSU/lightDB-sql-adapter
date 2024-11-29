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

    // Create collection table
    await strategy.executeQuery(connection, strategy.createCollectionsTable());

    // Create key-value table
    await strategy.executeQuery(connection, strategy.createKeyValueTable(READ_WRITE_KEY_TABLE));
}

async function executeMethod(method, params) {
    switch (method) {
        case 'executeQuery': {
            const [queryMethod, queryParams] = params;

            // For methods that need table creation
            if (['insertRecord', 'filter', 'queueSize', 'listQueue'].includes(queryMethod)) {
                const tableName = queryParams[0];
                await strategy.ensureTableExists(connection, tableName);
            }

            // Special handling for filter operations
            if (queryMethod === 'filter') {
                const [tableName, conditions, sort, max] = queryParams;
                let conditionsQuery = '';
                if (conditions && conditions.length > 0) {
                    conditionsQuery = strategy.convertConditionsToLokiQuery(conditions);
                }

                return await strategy.executeQuery(
                    connection,
                    strategy.filter(tableName, conditionsQuery, sort, max)
                );
            }

            // Handle record operations
            if (['insertRecord', 'updateRecord'].includes(queryMethod)) {
                const query = strategy[queryMethod](...queryParams);
                const [tableName, pk, recordData, timestamp] = queryParams;
                const data = typeof recordData === 'string' ? recordData : JSON.stringify(recordData);
                return await strategy.executeQuery(connection, query, [pk, data, timestamp]);
            }

            // Handle queue operations
            if (queryMethod === 'queueSize') {
                const query = strategy.queueSize(...queryParams);
                return await strategy.executeQuery(connection, query);
            }

            if (queryMethod === 'listQueue') {
                const query = strategy.listQueue(...queryParams);
                return await strategy.executeQuery(connection, query);
            }

            // Handle key-value operations
            if (['writeKey', 'readKey'].includes(queryMethod)) {
                const query = strategy[queryMethod](...queryParams);
                if (queryMethod === 'writeKey') {
                    const [tableName, key, value, timestamp] = queryParams;
                    return await strategy.executeQuery(connection, query, [key, value, timestamp]);
                }
                const [tableName, key] = queryParams;
                return await strategy.executeQuery(connection, query, [key]);
            }

            // Default query execution
            const query = strategy[queryMethod](...queryParams);
            return await strategy.executeQuery(
                connection,
                query,
                Array.isArray(queryParams.slice(1)) ? queryParams.slice(1) : []
            );
        }
        case 'executeTransaction': {
            const [transactionMethod, transactionParams] = params;
            const queries = strategy[transactionMethod](...transactionParams);
            return await strategy.executeTransaction(connection, queries);
        }
        case 'addInQueue': {
            const [queueName, object, ensureUniqueness] = params;
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
            await strategy.closeConnection(connection);
            connection = null;
            strategy = null;
            return true;
        case 'parseResult': {
            const [parseMethod, result] = params;
            return strategy[parseMethod](result);
        }
        case 'queueSize': {
            const [queueName] = params;
            return await strategy.queueSize(connection, queueName);
        }
        case 'listQueue': {
            const [queueName, sortAfterInsertTime, onlyFirstN] = params;
            return await strategy.listQueue(connection, queueName, sortAfterInsertTime, onlyFirstN);
        }
        case 'writeKey': {
            const [tableName, key, value] = params;
            const valueObject = {
                type: typeof value,
                value: value
            };
            const query = strategy.writeKey(tableName);
            return await strategy.executeQuery(connection, query, [key, JSON.stringify(valueObject), Date.now()]);
        }
        case 'readKey': {
            const [tableName, key] = params;
            const query = strategy.readKey(tableName);
            return await strategy.executeQuery(connection, query, [key]);
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