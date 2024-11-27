// strategyFactory.js
const BaseStrategy = require('./strategies/baseStrategy');
const PostgreSQLStrategy = require('.//strategies/postgreSQLStrategy');
const MySQLStrategy = require('./strategies/mySQLStrategy');
const SQLServerStrategy = require('./strategies/sqlServerStrategy');

class StrategyFactory {
    static createStrategy(type) {
        switch (type.toLowerCase()) {
            case 'postgresql':
                return new PostgreSQLStrategy();
            case 'mysql':
                return new MySQLStrategy();
            case 'sqlserver':
                return new SQLServerStrategy();
            default:
                throw new Error(`Unsupported database type: ${type}`);
        }
    }
}

module.exports = {
    BaseStrategy,
    PostgreSQLStrategy,
    MySQLStrategy,
    SQLServerStrategy,
    StrategyFactory
};