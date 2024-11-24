// Creating a database and initializing a collection for cats in MongoDB
// use cats_db;
db = db.getSiblingDB('cats_db');

// Creating the 'cats' collection
db.createCollection('cats');

// Adding initial documents to the 'cats' collection
db.cats.insertMany([
    {
        "name": "barsik",
        "age": 3,
        "features": ["ходить в капці", "дає себе гладити", "рудий"]
    },
    {
        "name": "murzik",
        "age": 5,
        "features": ["любить гратися", "чорний", "дружелюбний"]
    },
    {
        "name": "simba",
        "age": 2,
        "features": ["сірий", "активний", "любить ласощі"]
    }
]);

print('Database initialization for cats_db completed successfully.');
