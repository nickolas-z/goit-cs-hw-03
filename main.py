from pymongo import MongoClient
from bson.objectid import ObjectId
import pymongo.errors
from DatabaseManager import DatabaseManager, DatabaseType

# Ініціалізація бази даних
DB_PARAMS = {"host": "localhost", "port": "27017", "dbname": "cats_db"}
SCRIPT_PATH = "cats_script.js"  # Це шлях до скрипта, що містить необхідні ініціалізаційні команди для бази даних

DatabaseManager.initialize_database(DatabaseType.MONGODB, DB_PARAMS, SCRIPT_PATH)


# Додати запис у колекцію
def create_cat(name, age, features):
    try:
        cat = {"name": name, "age": age, "features": features}
        cats_collection.insert_one(cat)
        print(f"Кіт з іменем '{name}' успішно доданий.")
    except pymongo.errors.PyMongoError as e:
        print(f"Помилка при додаванні кота: {e}")


# Виведення всіх записів із колекції
def read_all_cats():
    try:
        cats = cats_collection.find()
        for cat in cats:
            print(cat)
    except pymongo.errors.PyMongoError as e:
        print(f"Помилка при читанні записів: {e}")


# Виведення інформації про кота за ім'ям
def read_cat_by_name(name):
    try:
        cat = cats_collection.find_one({"name": name})
        if cat:
            print(cat)
        else:
            print(f"Кіт з іменем '{name}' не знайдений.")
    except pymongo.errors.PyMongoError as e:
        print(f"Помилка при читанні запису: {e}")


# Оновлення віку кота за ім'ям
def update_cat_age(name, new_age):
    try:
        result = cats_collection.update_one({"name": name}, {"$set": {"age": new_age}})
        if result.matched_count > 0:
            print(f"Вік кота з іменем '{name}' успішно оновлено.")
        else:
            print(f"Кіт з іменем '{name}' не знайдений.")
    except pymongo.errors.PyMongoError as e:
        print(f"Помилка при оновленні запису: {e}")


# Додавання нової характеристики до списку features за ім'ям
def add_feature_to_cat(name, feature):
    try:
        result = cats_collection.update_one(
            {"name": name}, {"$addToSet": {"features": feature}}
        )
        if result.matched_count > 0:
            print(f"Характеристика '{feature}' успішно додана коту з іменем '{name}'.")
        else:
            print(f"Кіт з іменем '{name}' не знайдений.")
    except pymongo.errors.PyMongoError as e:
        print(f"Помилка при оновленні запису: {e}")


# Видалення запису за ім'ям
def delete_cat_by_name(name):
    try:
        result = cats_collection.delete_one({"name": name})
        if result.deleted_count > 0:
            print(f"Кіт з іменем '{name}' успішно видалений.")
        else:
            print(f"Кіт з іменем '{name}' не знайдений.")
    except pymongo.errors.PyMongoError as e:
        print(f"Помилка при видаленні запису: {e}")


# Видалення всіх записів із колекції
def delete_all_cats():
    try:
        result = cats_collection.delete_many({})
        print(f"Успішно видалено {result.deleted_count} записів із колекції.")
    except pymongo.errors.PyMongoError as e:
        print(f"Помилка при видаленні записів: {e}")


# Основний блок для тестування функцій, викликайте тут необхідні методи
def main():
    create_cat("barsik", 3, ["ходить в капці", "дає себе гладити", "рудий"])
    read_all_cats()
    read_cat_by_name("barsik")
    update_cat_age("barsik", 4)
    add_feature_to_cat("barsik", "любить їсти")
    delete_cat_by_name("barsik")
    delete_all_cats()


if __name__ == "__main__":
    main()
