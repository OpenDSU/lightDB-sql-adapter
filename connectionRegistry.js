// connectionRegistry.js
const {Pool} = require('pg');

class ConnectionRegistry {
    static get POSTGRESQL() {
        return 'postgresql';
    }

    static get MYSQL() {
        return 'mysql';
    }

    static get SQLSERVER() {
        return 'sqlserver';
    }

    // Default configurations
    static get DEFAULT_CONFIGS() {
        return {
            postgresql: {
                user: 'postgres',
                password: 'password',
                host: 'localhost',
                database: 'postgres',
                port: 5432,
                max: 20,  // Max number of clients in the pool
                idleTimeoutMillis: 30000,  // Close idle clients after 30 seconds
                ssl: false
            },
            mysql: {
                host: 'localhost',
                user: 'root',
                password: 'password',
                database: 'test',
                port: 3306,
                waitForConnections: true,
                connectionLimit: 20,
                queueLimit: 0
            },
            sqlserver: {
                server: 'localhost',
                user: 'sa',
                password: 'Password123!',
                database: 'master',
                port: 1433,
                pool: {
                    max: 20,
                    min: 0,
                    idleTimeoutMillis: 30000
                },
                options: {
                    encrypt: false,
                    trustServerCertificate: true,
                    enableArithAbort: true
                }
            }
        };
    }

    static async createConnection(type, customConfig = null) {
        const dbType = type.toLowerCase();
        const config = customConfig || this.DEFAULT_CONFIGS[dbType];

        if (!config) {
            throw new Error(`No configuration found for database type: ${type}`);
        }

        // Validate required configuration fields
        if (!config.user || typeof config.user !== 'string') {
            throw new Error('Database user must be a string');
        }
        if (!config.password || typeof config.password !== 'string') {
            throw new Error('Database password must be a string');
        }

        try {
            switch (dbType) {
                case 'postgresql':
                    // Create a pool with proper configuration
                    const pool = new Pool({
                        ...config,
                        max: config.max || 20,
                        idleTimeoutMillis: config.idleTimeoutMillis || 30000
                    });
                    // Test the connection
                    await pool.query('SELECT 1');
                    return pool;

                default:
                    throw new Error(`Unsupported database type: ${type}`);
            }
        } catch (error) {
            console.error('Connection error details:', {
                type: dbType,
                host: config.host,
                port: config.port,
                database: config.database,
                user: config.user,
                error: error.message
            });
            throw new Error(`Failed to connect to ${type}: ${error.message}`);
        }
    }

    static async testConnection(type, customConfig = null) {
        let connection = null;
        try {
            connection = await this.createConnection(type, customConfig);
            return true;
        } catch (error) {
            console.error(`Failed to connect to ${type}:`, error.message);
            return false;
        } finally {
            if (connection) {
                switch (type.toLowerCase()) {
                    case 'postgresql':
                }
            }
        }
    }
}

module.exports = ConnectionRegistry;