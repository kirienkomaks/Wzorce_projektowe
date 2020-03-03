const { Pool, Client } = require('pg') // Biblioteka do postgres
const Sequelize = require('sequelize');
const fs = require('fs'); // operacje na plikach

//Interceptor, przekazuje żądania z biblioteki
class FilterManager {
    constructor(sequelize) {
        sequelize.filterManager = this;
    }

    //podłączenie filtra (loadBalancera)
    attach(filter) {
        this.filter = filter;
    }
    process(defaultConnection, parameters, query) {
        return this.filter.process(parameters, query);
    }
    processInitializationQueries(connectionConfig, query) {
        return this.filter.sendToAll(null, query);
    }
}

//Stany load balancera - są używane w polu state w klasie LoadBalancer
//Aby zmienić obecny LoadBalancerState, wystarczy zrobić context.state = new ...
class NoTransaction {
    nonModifyingQuery(context, parameters, query) {
        context.connectionManager.chooseDatabase();
        return context.send(parameters, query);
    }
    modifyingQuery(context, parameters, query) {
        return context.sendToAll(parameters, query);
    }
    startTransaction(context, parameters, query) {
        context.state = new OneDatabase();
        context.incrementTransactionNesting();
        context.addToQueue(parameters, query);
        context.connectionManager.chooseDatabase();
        return context.send(parameters, query);
    }
    commit(context, parameters, query) {
        return returnErrorMessage("Commit while no transaction");
    }
    rollback(context, parameters, query) {
        return returnErrorMessage("Rollback while no transaction");
    }
}

class OneDatabase {
    nonModifyingQuery(context, parameters, query) {
        return context.send(parameters, query);
    }
    modifyingQuery(context, parameters, query) {
        context.state = new MultipleDatabases();
        context.sendAndClearQueue();
        return context.sendToAll(parameters, query);
    }
    startTransaction(context, parameters, query) {
        context.incrementTransactionNesting();
        context.addToQueue(parameters, query);
        return context.send(parameters, query);
    }
    commit(context, parameters, query) {
        context.decrementTransactionNesting();
        if (context.nesting == 0) {
            context.clearQueue();
            context.state = new NoTransaction();
        } else {
            context.addToQueue(parameters, query);
        }
        return context.send(parameters, query);
    }
    rollback(context, parameters, query) {
        context.clearQueue();
        context.state = new NoTransaction();
        return context.send(parameters, query);
    }
}

class MultipleDatabases {
    nonModifyingQuery(context, parameters, query) {
        return context.send(parameters, query);
    }
    modifyingQuery(context, parameters, query) {
        return context.sendToAll(parameters, query);
    }
    startTransaction(context, parameters, query) {
        context.incrementTransactionNesting();
        return context.sendToAll(parameters, query);
    }
    commit(context, parameters, query) {
        context.decrementTransactionNesting();
        if (context.nesting == 0) {
            context.clearQueue();
            context.state = new NoTransaction();
        }
        return context.sendToAll(parameters, query);   
    }
    rollback(context, parameters, query) {
        context.clearQueue();
        context.state = new NoTransaction();
        return context.sendToAll(parameters, query);   
    }
}

//Strategie wysyłania zapytań
class SingleDatabase {
    send(connectionManager, query, parameters) {
        console.log('LoadBalancer - executing on database number ' + connectionManager.currentDatabaseIndex + ': ' + query)
        var connection = connectionManager.getConnection(connectionManager.currentDatabaseIndex);
        return parameters && parameters.length
            ? new Promise((resolve, reject) => connection.query(query, parameters, (error, result) => error ? reject(error) : resolve(result)))
            : new Promise((resolve, reject) => connection.query(query, (error, result) => error ? reject(error) : resolve(result)));
    }
}

class AllDatabases {
    send(connectionManager, query, parameters) {
        console.log('LoadBalancer - executing on all databases: ' + query)
        var promises = [];
        for (var i = 0; i < connectionManager.activeConnections.length; i++) {
            var connection = connectionManager.getConnection(i);
            var promise = parameters && parameters.length
                ? new Promise((resolve, reject) => connection.query(query, parameters, (error, result) => error ? reject(error) : resolve(result)))
                : new Promise((resolve, reject) => connection.query(query, (error, result) => error ? reject(error) : resolve(result)));
            promises.push(promise);
        };
        connectionManager.storeToAllInactive(parameters, query);
        return Promise.all(promises).then((result) => result[0]);
    }
}

class AllExceptOneDatabase {
    send(connectionManager, query, parameters) {
        console.log('LoadBalancer - executing on all databases except number ' + connectionManager.currentDatabaseIndex + ': ' + query)
        var promises = [];
        for (var i = 0; i < connectionManager.activeConnections.length; i++) {
            if (i == connectionManager.currentDatabaseIndex) {
                continue;
            }
            var connection = connectionManager.getConnection(i);
            console.log("AllExceptOneDatabase " + i + " " + query)
            var promise = parameters && parameters.length
                ? new Promise((resolve, reject) => connection.query(query, parameters, (error, result) => error ? reject(error) : resolve(result)))
                : new Promise((resolve, reject) => connection.query(query, (error, result) => error ? reject(error) : resolve(result)));
            promises.push(promise);
        };
        connectionManager.storeToAllInactive(parameters, query);
        return Promise.all(promises).then((result) => result[0]);
    }
}

//Zarządza połączeniami z wieloma bazami danych
class ConnectionManager {
    constructor(databaseConfigs) {
        this.connections = [];
        this.activeConnections = [];
        this.inactiveConnections = [];
        this.currentDatabaseIndex = 0;
        this.num_connections = 0;
        this.databaseConfigs = databaseConfigs
        var connect_promises = [];
        databaseConfigs.forEach(element => {
            var client = new Client(element);
            var index = this.num_connections;
            connect_promises.push(client.connect().then(() => {
                this.activeConnections.push(index);
                console.log(`Connected to database ${index}`);
            }).catch(err => {
                this.inactiveConnections.push(index);
                console.log(`Could not connect to database ${index}: ${err}`);
            }));
            this.connections.push(client);
            this.num_connections++;
        });
        this.storedQueries = this.getQueuesFromFile();
        if (!(this.storedQueries && this.storedQueries.length == this.num_connections)) {
            this.storedQueries = [];
            for (var i = 0; i < this.num_connections; ++i) {
                this.storedQueries.push([]);
            }
        }
        setInterval(this.writeQueuesToFile.bind(this), 1000);
        this.dateChecked = Date.now();
        this.initialization = Promise.allSettled(connect_promises).then(async () => {
            var promises = [];
            for (var i = 0; i < this.num_connections; i++) {
                promises.push(this.catchUpInactive(i));
            }
            await Promise.allSettled(promises);
            this.inactiveChanges = true;
        })
    }
    async waitForInitialization() {
        if (this.initialization) {
            await this.initialization;
        }
    }
    tryReconnectInactive() {
        var promises = [];
        for (const i of this.inactiveConnections) {
            this.connections[i] = new Client(this.databaseConfigs[i]);
            promises.push(this.connections[i].connect().then(() => {
                console.log(`Reconnected with ${i}`);
                this.catchUpInactive(i).then(() => {
                    return i;
                });
            }).catch(err => {
                console.log(`Didn't reconnect with with ${i}: ${err}`);
            }));
        }
        return Promise.allSettled(promises).then(values => {
            for (const result of values) {
                if (result.statis == "fulfilled") {
                    console.log(`status fulfilled of reconnecting, index: ${result.value}`);
                    for(var i = this.inactiveConnections.length - 1; i >= 0; i--) {
                        if(this.inactiveConnections[i] === result.value) {
                            array.splice(i, 1);
                        }
                    }
                    this.activeConnections.push(result.value);
                } else {
                    console.log(`status: ${result.status}`);
                }
            }
        });
    }
    storeToAllInactive(parameters, query) {
        for (const i of this.inactiveConnections) {
            this.storedQueries[i].push({parameters: parameters, query: query});
        }
        this.inactiveChanges = true;
    }
    async catchUpInactive(index) {
        while (this.storedQueries[index].length > 0) {
            var connection = this.connections[index];
            var query = this.storedQueries[index][0].query;
            var parameters = this.storedQueries[index][0].parameters;
            if (parameters && parameters.length) {
                await connection.query(query, parameters)
            } else {
                await connection.query(query);
            }
            this.storedQueries[index].shift();
        }
        this.inactiveChanges = true;
    }
    //zwraca połączenie z bazą danych na podstawie indeksu
    getConnection(index) {
        return this.connections[this.activeConnections[index]];
    }
    //Wybiera (zmienia) obecną bazę danych do któej wysyłamy
    chooseDatabase() {
        this.currentDatabaseIndex++;
        if (this.currentDatabaseIndex >= this.activeConnections.length) {
            this.currentDatabaseIndex = 0;
        }
    }
    //Wybiera strategię wysyłania query
    setStrategy(strategy) {
        this.strategy = strategy;
    }
    //wysyła query
    send(parameters, query) {
        return this.waitForInitialization().then(() => {
            return this.strategy.send(this, query, parameters);
        });
    }
    //Zapisuje kolejki niekatywnych do pliku
    writeQueuesToFile() {
        if (this.inactiveChanges) {
            var obj = {}
            obj.queues = this.storedQueries;
            var str = JSON.stringify(obj);
            console.log(this.storedQueries);
            fs.writeFile('queries.json', str, err => {
                if (err) {
                    console.log("Error while writing to file: " + err);
                }
            })
            this.inactiveChanges = false;
        }
    }
    //Wczyctje kolejki nieaktywnych z pliku
    getQueuesFromFile() {
        try {
            return JSON.parse(fs.readFileSync('queries.json')).queues;
        } catch(err) {
            console.log("Error while reading file: " + err);
        }
    }
}

//Główna klasa, dostaje żądania, analizuje je i wysyła w odpowiednie miejsce
class LoadBalancer {
    constructor(database_configs) {
        this.state = new NoTransaction();
        this.connectionManager = new ConnectionManager(database_configs);
        this.nesting = 0;
        this.queryQueue = [];
        this.dateChecked = Date.now();
    }
    process(parameters, query) {
        var dateNow = Date.now()
        if (dateNow - this.dateChecked > 1000) {
            this.dateChecked = dateNow;
            return this.connectionManager.tryReconnectInactive().then(() => this.chooseTypeAndSend(parameters, query));
        } else {
            return this.chooseTypeAndSend(parameters, query);
        }
    }
    chooseTypeAndSend(parameters, query) {
        if (query.startsWith("START TRANSACTION")) {
            return this.state.startTransaction(this, parameters, query);
        } else if (query.startsWith("COMMIT")) {
            return this.state.commit(this, parameters, query);
        } else if (query.startsWith("ROLLBACK")) {
            return this.state.rollback(this, parameters, query);
        } else if (query.startsWith("SELECT")) {
            return this.state.nonModifyingQuery(this, parameters, query);
        } else {
            return this.state.modifyingQuery(this, parameters, query);
        }
    }
    send(parameters, query) {
        this.connectionManager.setStrategy(new SingleDatabase());
        return this.connectionManager.send( parameters, query);
    }
    sendToAll(parameters, query) {
        this.connectionManager.setStrategy(new AllDatabases());
        return this.connectionManager.send(parameters, query);
    }
    sendToAllExceptOne(parameters, query) {
        this.connectionManager.setStrategy(new AllExceptOneDatabase());
        return this.connectionManager.send(parameters, query);
    }
    rollbackAll() {
        this.sendToAll(null, "ROLLBACK;");
    }
    returnErrorMessage(message) {
        return new Promise((resolve, reject) => {
            reject(message);
          });
    }
    clearQueue() {
        this.queryQueue = [];
        this.nesting = 0;
    }
    addToQueue(parameters, query) {
        this.queryQueue.push({parameters: parameters, query: query});
    }
    sendAndClearQueue() {
        while (this.queryQueue.length > 0) {
            var elem = this.queryQueue.shift();
            this.sendToAllExceptOne(elem.parameters, elem.query);
        }
    }
    decrementTransactionNesting() {
        this.nesting--;
    }
    incrementTransactionNesting() {
        this.nesting++;
    }
}

module.exports = {FilterManager, LoadBalancer};
