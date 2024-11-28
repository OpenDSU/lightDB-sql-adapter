// tests/test-imports.js
const assert = require('assert');
const SQLAdapter = require('../sqlAdapter');
const SQLWorkerAdapter = require('../sqlWorkerAdapter');

describe('Import Tests', () => {
    it('should properly import SQLWorkerAdapter', () => {
        console.log('SQLWorkerAdapter:', SQLWorkerAdapter);
        assert(typeof SQLWorkerAdapter === 'function', 'SQLWorkerAdapter should be a constructor');
    });

    it('should properly import SQLAdapter', () => {
        console.log('SQLAdapter:', SQLAdapter);
        assert(typeof SQLAdapter === 'function', 'SQLAdapter should be a constructor');
    });

    it('should be able to instantiate SQLWorkerAdapter', () => {
        const worker = new SQLWorkerAdapter('postgresql');
        assert(worker instanceof SQLWorkerAdapter, 'Should create SQLWorkerAdapter instance');
    });
});