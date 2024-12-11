require('../opendsu-sdk/builds/output/testsRuntime');
const doubleCheck = require("double-check");
const assert = doubleCheck.assert;
const path = require('path');
const os = require('os');
const fs = require('fs');
const LightDBServer = require('../LightDBServer');


assert.callback("LightDB Server Tests", async (testFinished) => {
    doubleCheck.createTestFolder("testFolder", async (err, folder) => {
        // Server configuration
        const serverConfig = {
            lightDBStorage: folder,
            lightDBPort: 8081,
            host: 'localhost'
        };

        // Embedded PostgreSQL configuration
        const sqlConfig = {
            type: "postgresql",
            postgresql: {
                user: "postgres",
                password: "password",
                host: "localhost",
                database: "postgres",
                port: 5432,
                max: 20,
                idleTimeoutMillis: 30000
            }
        };
        let server;
        const testPort = serverConfig.lightDBPort;
        const testDbName = 'testdb';

        try {
            // Start server
            server = await new Promise((resolve, reject) => {
                new LightDBServer(serverConfig, sqlConfig, (err, serverInstance) => {
                    if (err) {
                        console.error('Server initialization error:', err);
                        return reject(err);
                    }
                    resolve(serverInstance);
                });
            });

            // Test database creation
            const createResponse = await fetch(`http://localhost:${testPort}/createDatabase/${testDbName}`, {
                method: 'PUT'
            });

            assert.true(createResponse.status === 201, 'Database creation should return 201');

            // Test table creation
            const createTableCmd = {
                commandName: 'execute',
                params: [
                    $$.SYSTEM_IDENTIFIER,
                    `CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    value INTEGER
                )`
                ]
            };

            const createTableResponse = await fetch(`http://localhost:${testPort}/executeCommand/${testDbName}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    command: JSON.stringify(createTableCmd),
                    signature: $$.SYSTEM_DID_DOCUMENT.sign(JSON.stringify(createTableCmd))
                })
            });

            assert.true(createTableResponse.status === 200, 'Table creation should succeed');

            // Test data insertion
            const insertCmd = {
                commandName: 'execute',
                params: [
                    $$.SYSTEM_IDENTIFIER,
                    'INSERT INTO test_table (name, value) VALUES ($1, $2) RETURNING *',
                    ['test_item', 42]
                ]
            };

            const insertResponse = await fetch(`http://localhost:${testPort}/executeCommand/${testDbName}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    command: JSON.stringify(insertCmd),
                    signature: $$.SYSTEM_DID_DOCUMENT.sign(JSON.stringify(insertCmd))
                })
            });

            assert.true(insertResponse.status === 200, 'Data insertion should succeed');

            // Test data query
            const queryCmd = {
                commandName: 'execute',
                params: [
                    $$.SYSTEM_IDENTIFIER,
                    'SELECT * FROM test_table WHERE name = $1',
                    ['test_item']
                ]
            };

            const queryResponse = await fetch(`http://localhost:${testPort}/executeCommand/${testDbName}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    command: JSON.stringify(queryCmd),
                    signature: $$.SYSTEM_DID_DOCUMENT.sign(JSON.stringify(queryCmd))
                })
            });

            assert.true(queryResponse.status === 200, 'Data query should succeed');

            const result = await queryResponse.json();
            assert.true(Array.isArray(result), 'Result should be an array');
            assert.true(result.length === 1, 'Should have one result');
            assert.true(result[0].name === 'test_item', 'Should have correct name');
            assert.true(result[0].value === 42, 'Should have correct value');

            // Test error handling
            const invalidCmd = {
                commandName: 'execute',
                params: [
                    $$.SYSTEM_IDENTIFIER,
                    'INVALID SQL STATEMENT'
                ]
            };

            const errorResponse = await fetch(`http://localhost:${testPort}/executeCommand/${testDbName}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    command: JSON.stringify(invalidCmd),
                    signature: $$.SYSTEM_DID_DOCUMENT.sign(JSON.stringify(invalidCmd))
                })
            });

            assert.true(errorResponse.status === 500, 'Invalid SQL should return 500');

        } catch (error) {
            console.error('Test error:', error);
            throw error;
        } finally {
            // Cleanup
            if (server && server.close) {
                await new Promise(resolve => server.close(resolve));
            }
            fs.rmSync(folder, {recursive: true, force: true});
            testFinished();
        }
    });
}, 20000);