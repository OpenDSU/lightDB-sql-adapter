// workerScript.js
const worker_threads = "worker_threads";
const {parentPort, isMainThread} = require(worker_threads);
const {StrategyFactory} = require("./strategyFactory");
const ConnectionRegistry = require('./connectionRegistry');
const constants = require("./constants");

let pool = null;
let strategy = null;

if (!isMainThread) {
    parentPort.postMessage("ready");

    async function initializePool(config) {
        let type = constants.TYPES.POSTGRESQL;
        if (config && config.type) {
            type = config.type;
        }

        if (!config){
            config = ConnectionRegistry.DEFAULT_CONFIGS[type.toLowerCase()]
        }

        if (!pool) {
            console.log('DEBUG: Initializing pool with type:', type);
            pool = await ConnectionRegistry.createConnection(type, config);
            strategy = StrategyFactory.createStrategy(type);
            console.log('DEBUG: Strategy created:', strategy.constructor.name);
        }
        return pool;
    }

    parentPort.on("message", async (taskData) => {
        console.log('DEBUG: Received task:', taskData);
        const {taskName, args} = taskData;
        let result = null;
        let error = null;

        try {
            if (!pool) {
                await initializePool(taskData.workerData.config, taskData.workerData.type);
            }

            console.log('DEBUG: Executing strategy method:', taskName, 'with args:', args);
            // Verify the strategy has the method
            if (typeof strategy[taskName] !== 'function') {
                throw new Error(`Method ${taskName} not implemented`);
            }

            result = await strategy[taskName](pool, ...args);
            result = JSON.parse(JSON.stringify(result));

            parentPort.postMessage({
                success: true,
                result
            });
        } catch (err) {
            console.error('DEBUG: Error executing task:', err);
            error = {
                message: err.message,
                code: err.code,
                type: err.type || 'DatabaseError'
            };
            parentPort.postMessage({
                success: false,
                error
            });
        }
    });

    process.on("uncaughtException", (error) => {
        console.error("[SQL Worker] uncaughtException inside worker:", error);
        parentPort.postMessage({
            success: false,
            error: {
                message: error.message,
                code: error.code,
                type: 'UncaughtException'
            }
        });
    });
}