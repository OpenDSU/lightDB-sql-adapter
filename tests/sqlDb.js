const getTestDb = require("../../loki-enclave-facade/tests/test-util").getTestDb;

function getSQLDB(config) {
    const SQLAdapter = require("../sqlAdapter");

    return new SQLAdapter(config);
}

const adapter = getSQLDB();
getTestDb(adapter, 'sql');