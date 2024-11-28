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
            const query = strategy[queryMethod](...queryParams);

            if (Array.isArray(query)) {
                return await strategy.executeTransaction(connection, query);
            }

            if (['insertRecord', 'updateRecord'].includes(queryMethod)) {
                const [tableName, pk, data] = queryParams;
                const result = await strategy.executeQuery(
                    connection,
                    query,
                    [pk, JSON.stringify(data), Date.now()]
                );
                return await strategy.parseInsertResult(result);
            }

            if (queryMethod === 'deleteRecord') {
                const [tableName, pk] = queryParams;
                const result = await strategy.executeQuery(connection, query, [pk]);
                return strategy.parseDeleteResult(result);
            }

            if (queryMethod === 'writeKey') {
                const [tableName, key, value] = queryParams;
                const result = await strategy.executeQuery(
                    connection,
                    query,
                    [key, JSON.stringify(value), Date.now()]
                );
                return strategy.parseWriteKeyResult(result);
            }

            if (queryMethod === 'readKey') {
                const [tableName, key] = queryParams;
                const result = await strategy.executeQuery(connection, query, [key]);
                return strategy.parseReadKeyResult(result);
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