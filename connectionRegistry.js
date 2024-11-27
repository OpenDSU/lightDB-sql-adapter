// connectionRegistry.js
const {Pool} = require('pg');
const mysql = require('mysql2/promise');
const sql = require('mssql');

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
                idleTimeoutMillis: 30000  // Close idle clients after 30 seconds
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

        let connection = null;
        try {
            switch (dbType) {
                case 'postgresql':
                    connection = new Pool(config);
                    // Test the connection
                    await connection.query('SELECT 1');
                    break;

                case 'mysql':
                    connection = await mysql.createPool(config);
                    // Test the connection
                    await connection.query('SELECT 1');
                    break;

                case 'sqlserver':
                    connection = await sql.connect(config);
                    // Test the connection
                    await connection.request().query('SELECT 1');
                    break;

                default:
                    throw new Error(`Unsupported database type: ${type}`);
            }
            return connection;
        } catch (error) {
            if (connection) {
                switch (dbType) {
                    case 'postgresql':
                    case 'mysql':
                        await connection.end();
                        break;
                    case 'sqlserver':
                        await connection.close();
                        break;
                }
            }
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
                    case 'mysql':
                        await connection.end();
                        break;
                    case 'sqlserver':
                        await connection.close();
                        break;
                }
            }
        }
    }
}

module.exports = ConnectionRegistry;