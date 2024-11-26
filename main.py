from pymongo import MongoClient
from DatabaseManager import DatabaseManager, DatabaseType
from CatManager import CatManager


def demonstrate_all_functions(db_params):
    """Demonstrate all functions"""
    client = MongoClient(f"mongodb://{db_params['host']}:{db_params['port']}/")
    db = client[db_params["dbname"]]
    cats_collection = db["test_collection"]
    cat_manager = CatManager(cats_collection)

    print("Initial state of the collection:")
    cat_manager.read_all_cats()

    print("\n1. Creating a new cat:")
    cat_manager.create_cat("barsik", 3, ["ходить в капці", "дає себе гладити", "рудий"])
    cat_manager.read_all_cats()

    print("\n2. Reading cat by name:")
    cat_manager.read_cat_by_name("barsik")

    print("\n3. Updating age of cat:")
    cat_manager.update_cat_age("barsik", 4)
    cat_manager.read_all_cats()

    print("\n4. Adding feature to cat:")
    cat_manager.add_feature_to_cat("barsik", "ненажера")
    cat_manager.read_all_cats()

    print("\n5. Deleting cat by name:")
    cat_manager.delete_cat_by_name("barsik")
    cat_manager.read_all_cats()

    print("\n6. Deleting all cats:")
    cat_manager.delete_all_cats()
    cat_manager.read_all_cats()

    client.close()


def main():
    """Main block for testing functions, demonstrating CRUD operations"""
    demonstrate_all_functions()


if __name__ == "__main__":

    DB_PARAMS = {"host": "localhost", "port": "27017", "dbname": "cats_db"}
    SCRIPT_PATH = "cats_db.json"

    if DatabaseManager.initialize_database(
        DatabaseType.MONGODB, DB_PARAMS, SCRIPT_PATH
    ):
        print("Database initialization complete.")
        demonstrate_all_functions(DB_PARAMS)
        # Optionally stop the container
        response = (
            input("Do you want to stop the container? (yes/no): ").strip().lower()
        )
        if response == "yes":
            remove = (
                input("Do you want to remove the container? (yes/no): ").strip().lower()
                == "yes"
            )
            initializer = DatabaseManager.get_initializer(
                DatabaseType.MONGODB, DB_PARAMS
            )
            initializer.stop_container(remove)
