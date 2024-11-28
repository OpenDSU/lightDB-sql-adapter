// sqlWorkerAdapter.js
const {Worker} = require('worker_threads');
const path = require('path');
const EventEmitter = require('events');

class SQLWorkerAdapter extends EventEmitter {
    constructor(type) {
        super();
        this.type = type;
        this.worker = new Worker(path.join(__dirname, 'sqlWorker.js'));
        this.worker.on('error', error => this.emit('error', error));
        this.worker.on('exit', code => this.emit('exit', code));
    }

    async executeWorkerTask(method, params) {
        return new Promise((resolve, reject) => {
            this.worker.postMessage({type: this.type, method, params});

            this.worker.once('message', response => {
                if (response.success) {
                    resolve(response.result);
                } else {
                    const error = new Error(response.error);
                    error.stack = response.stack;
                    reject(error);
                }
            });
        });
    }

    async close() {
        try {
            await this.executeWorkerTask('close');
        } finally {
            await this.worker.terminate();
        }
    }
}

module.exports = SQLWorkerAdapter;