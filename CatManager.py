from bson.objectid import ObjectId
import pymongo.errors


class CatManager:
    def __init__(self, cats_collection):
        self.cats_collection = cats_collection

    def create_cat(self, name, age, features):
        """Add a new record to the collection"""
        try:
            cat = {"_id": ObjectId(), "name": name, "age": age, "features": features}
            self.cats_collection.insert_one(cat)
            print(f"Cat named '{name}' successfully added.")
        except pymongo.errors.PyMongoError as e:
            print(f"Error adding cat: {e}")

    def read_all_cats(self):
        """Read all records from the collection"""
        try:
            cats = self.cats_collection.find()
            for cat in cats:
                print(cat)
        except pymongo.errors.PyMongoError as e:
            print(f"Error reading records: {e}")

    def read_cat_by_name(self, name):
        """Read a record from the collection by name"""
        try:
            cat = self.cats_collection.find_one({"name": name})
            if cat:
                print(cat)
            else:
                print(f"Cat named '{name}' not found.")
        except pymongo.errors.PyMongoError as e:
            print(f"Error reading record: {e}")

    def update_cat_age(self, name, new_age):
        """Update the age of a cat by name"""
        try:
            result = self.cats_collection.update_one(
                {"name": name}, {"$set": {"age": new_age}}
            )
            if result.matched_count > 0:
                print(f"Age of cat named '{name}' successfully updated.")
            else:
                print(f"Cat named '{name}' not found.")
        except pymongo.errors.PyMongoError as e:
            print(f"Error updating record: {e}")

    def add_feature_to_cat(self, name, feature):
        """Add a new feature to the features list by name"""
        try:
            result = self.cats_collection.update_one(
                {"name": name}, {"$addToSet": {"features": feature}}
            )
            if result.matched_count > 0:
                print(f"Feature '{feature}' successfully added to cat named '{name}'.")
            else:
                print(f"Cat named '{name}' not found.")
        except pymongo.errors.PyMongoError as e:
            print(f"Error updating record: {e}")

    def delete_cat_by_name(self, name):
        """Delete a record from the collection by name"""
        try:
            result = self.cats_collection.delete_one({"name": name})
            if result.deleted_count > 0:
                print(f"Cat named '{name}' successfully deleted.")
            else:
                print(f"Cat named '{name}' not found.")
        except pymongo.errors.PyMongoError as e:
            print(f"Error deleting record: {e}")

    def delete_all_cats(self):
        """Delete all records from the collection"""
        try:
            result = self.cats_collection.delete_many({})
            print(
                f"Successfully deleted {result.deleted_count} records from the collection."
            )
        except pymongo.errors.PyMongoError as e:
            print(f"Error deleting records: {e}")


if __name__ == "__main__":
    pass
