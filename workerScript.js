// workerScript.js
const worker_threads = "worker_threads";
const {parentPort, isMainThread} = require(worker_threads);
const {StrategyFactory} = require("./strategyFactory");
const ConnectionRegistry = require('./connectionRegistry');

let pool = null;
let strategy = null;

if (!isMainThread) {
    parentPort.postMessage("ready");

    async function initializePool(config, type) {
        if (!pool) {
            pool = await ConnectionRegistry.createConnection(type, config);
            strategy = StrategyFactory.createStrategy(type);
            await strategy.ensureCollectionsTable(pool);
        }
        return pool;
    }

    parentPort.on("message", async (taskData) => {
        const {taskName, args} = taskData;
        let result = null;
        let error = null;

        try {
            if (!pool) {
                await initializePool(taskData.workerData.config, taskData.workerData.type);
            }

            // Call the strategy method directly
            result = await strategy[taskName](pool, ...args);
            result = JSON.parse(JSON.stringify(result));

            parentPort.postMessage({
                success: true,
                result
            });
        } catch (err) {
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