// strategyFactory.js
const BaseStrategy = require('./strategies/baseStrategy');
const PostgreSQLStrategy = require('./strategies/postgreSQLStrategy');

class StrategyFactory {
    static createStrategy(type) {
        switch (type.toLowerCase()) {
            case 'postgresql':
                return new PostgreSQLStrategy();
            default:
                throw new Error(`Unsupported database type: ${type}`);
        }
    }
}

module.exports = {
    BaseStrategy,
    PostgreSQLStrategy,
    StrategyFactory
};