const getTestDb = require("../../loki-enclave-facade/tests/test-util").getTestDb;

function getSQLDB() {
    const SQLAdapter = require("../sqlAdapter");

    return new SQLAdapter(undefined,"postgresql");
}

const adapter = getSQLDB();
getTestDb(adapter, 'sql');