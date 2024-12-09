const SQLAdapter = require("../sqlAdapter");
const getTestDb = require("./../opendsu-sdk/modules/loki-enclave-facade/tests/test-util").getTestDb;

function getSQLDB() {
    const ConnectionRegistry = require('./../connectionRegistry');

    return new SQLAdapter(ConnectionRegistry.POSTGRESQL);
}

const adapter = getSQLDB();
getTestDb(adapter, 'sql');