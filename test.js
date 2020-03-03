// Inicjalizacja bibliotek

const Sequelize = require('sequelize'); //Biblioteka mapowania obiektowo-relacyjnego
const {FilterManager, LoadBalancer} = require('./load-balancer');

// Inicjalizacja bazy danych do mapowania obiektowo-relacyjnego
const db = new Sequelize('dummy', 'postgres', '1234', {
    host: 'localhost',
    dialect: 'postgres',
    logging: false,
    pool: {
        max: 1,
        min: 0,
        acquire: 30000,
        idle: 10000
      }
  });

// Konfiguracje pozostałych baz
db_configs = [
    {
        database: 'baza1',
        user: 'postgres',
        password: '1234',
        host: 'localhost',
    },
    {
        database: 'baza2',
        user: 'postgres',
        password: '1234',
        host: 'localhost',
    },
    {
        database: 'baza3',
        user: 'postgres',
        password: '1234',
        host: 'localhost',
    }
]

// Wstawienie filterManagera do bazy
filterManager = new FilterManager(db);
var lb = new LoadBalancer(db_configs);
filterManager.attach(lb);


// Połączenie z bazą danych
db.authenticate()
  .then(() => console.log('connected'))
  .catch(err => console.log('Error while connection to database: ' + err))

// Określenie rodzaju obiektu testowego i jakieś operacje
// W bazie danych wymagana jest tabela "testobject"  z polami:
// id (serial), test_string (varchar), createdAt (date), updatedAt (date)
const TestObjects = db.define('testobject', {
    test_string: {
        type: Sequelize.STRING
    }
})

TestObjects.findOrCreate({where: {test_string: Math.random().toString(36).substr(2, 5)}})

TestObjects.findAll()
    .then(obj => {
        console.log(`Wczytano ${obj.length} obiekty z db`)
        for (var i = 0; i < obj.length; i++) {
            console.log(`testobjects[${i}].test_string: ` + obj[i].test_string)
        }
    })
    .catch(err => console.log('Error: ' + err))
    

const Testobject2s = db.define('testobject2s', {
    value: {
        type: Sequelize.INTEGER
    }
})
    
for (var i = 0; i < 4; i++) {
    Testobject2s.findAll()
    .then(obj => {
        console.log(`Wczytano ${obj.length} obiekty z db`)
        for (var i = 0; i < obj.length; i++) {
            console.log(`testobject2s[${i}].value: ` + obj[i].value)
        }
    })
    .catch(err => console.log('Error: ' + err))
}


setTimeout(() => {
    TestObjects.findOrCreate({where: {test_string: Math.random().toString(36).substr(2, 5)}})
}, 20000);
