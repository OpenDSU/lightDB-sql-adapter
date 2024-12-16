const SqlAdapter = require("./sqlAdapter");

const createSQLAdapterInstance = (config, type) => {
    return new SqlAdapter(config, type);
}

module.exports = {
    createSQLAdapterInstance,
}
