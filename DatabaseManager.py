from abc import ABC, abstractmethod
import time
import docker
import psycopg2
import json
from pymongo import MongoClient
from bson import ObjectId
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Dict, Any
from enum import Enum


class DatabaseType(Enum):
    POSTGRESQL = "postgresql"
    MONGODB = "mongodb"


class DatabaseInitializerInterface(ABC):
    @abstractmethod
    def start_container(self) -> bool:
        pass

    @abstractmethod
    def check_database_exists(self) -> bool:
        pass

    @abstractmethod
    def drop_database(self) -> None:
        pass

    @abstractmethod
    def create_database(self) -> None:
        pass

    @abstractmethod
    def execute_script(self, script_path: str) -> None:
        pass

    @abstractmethod
    def stop_container(self, remove: bool = False) -> None:
        pass


class PostgresDatabaseInitializer(DatabaseInitializerInterface):
    def __init__(self, db_params: Dict[str, Any]) -> None:
        self.initial_params = {
            "user": db_params.get("user"),
            "password": db_params.get("password"),
            "host": db_params.get("host"),
            "port": db_params.get("port"),
        }
        self.db_params = {
            **self.initial_params,
            "dbname": db_params.get("dbname"),
        }
        self.container_name = "my_postgres_container"
        self.container_port = 5432

    def start_container(self) -> bool:
        client = docker.from_env()

        try:
            container = client.containers.get(self.container_name)
            if container.status != "running":
                print("PostgreSQL container exists but is not running. Starting container...")
                container.start()
            else:
                # print("PostgreSQL container is already running.")
                container.reload()
                return container.status == "running"
        except docker.errors.NotFound:
            print("PostgreSQL container not found. Creating and starting a new container...")
            container = client.containers.run(
                "postgres:latest",
                detach=True,
                ports={f"{self.container_port}/tcp": self.initial_params["port"]},
                environment={
                    "POSTGRES_USER": self.initial_params["user"],
                    "POSTGRES_PASSWORD": self.initial_params["password"],
                },
                name=self.container_name,
            )

        print("Waiting for PostgreSQL database to start...")
        time.sleep(10)

        container.reload()
        return container.status == "running"

    def check_database_exists(self) -> bool:
        conn_params = self.db_params.copy()
        conn_params["dbname"] = "postgres"

        try:
            conn = psycopg2.connect(**conn_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.db_params["dbname"],),
            )
            exists = cursor.fetchone() is not None

            cursor.close()
            conn.close()
            return exists
        except Exception as e:
            print(f"An error occurred while checking the PostgreSQL database: {e}")
            return False

    def drop_database(self) -> None:
        conn_params = self.db_params.copy()
        conn_params["dbname"] = "postgres"

        try:
            conn = psycopg2.connect(**conn_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            cursor.execute(f"DROP DATABASE IF EXISTS {self.db_params['dbname']}")
            print(f"PostgreSQL database '{self.db_params['dbname']}' dropped successfully.")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"An error occurred while dropping the PostgreSQL database: {e}")

    def create_database(self) -> None:
        conn_params = self.db_params.copy()
        conn_params["dbname"] = "postgres"

        try:
            conn = psycopg2.connect(**conn_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            cursor.execute(f"CREATE DATABASE {self.db_params['dbname']}")
            print(f"PostgreSQL database '{self.db_params['dbname']}' created successfully.")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"An error occurred while creating the PostgreSQL database: {e}")

    def execute_script(self, script_path: str) -> None:
        if not script_path:
            print("No script path provided. Skipping script execution.")
            return

        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            with open(script_path, "r") as sql_file:
                sql_script = sql_file.read()

            cursor.execute(sql_script)
            conn.commit()
            print("PostgreSQL script executed successfully.")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"An error occurred while executing the PostgreSQL script: {e}")

    def stop_container(self, remove: bool = False) -> None:
        client = docker.from_env()

        try:
            container = client.containers.get(self.container_name)
            if container.status == "running":
                print("Stopping the PostgreSQL container...")
                container.stop()
                print("PostgreSQL container stopped successfully.")
            else:
                print("PostgreSQL container is not running.")
            if remove:
                print("Removing the PostgreSQL container...")
                container.remove()
                print("PostgreSQL container removed successfully.")
        except docker.errors.NotFound:
            print("PostgreSQL container not found.")


class MongoDBInitializer(DatabaseInitializerInterface):
    def __init__(self, db_params: Dict[str, Any]):
        self.initial_params = {
            "host": db_params.get("host"),
            "port": db_params.get("port"),
        }
        self.db_name = db_params.get("dbname")
        self.container_name = "my_mongodb_container"
        self.container_port = 27017

    def start_container(self) -> bool:
        client = docker.from_env()

        try:
            container = client.containers.get(self.container_name)
            if container.status != "running":
                print("MongoDB container exists but is not running. Starting container...")
                container.start()
            else:
                # print("MongoDB container is already running.")
                container.reload()
                return container.status == "running"
        except docker.errors.NotFound:
            print("MongoDB container not found. Creating and starting a new container...")
            container = client.containers.run(
                "mongo:latest",
                detach=True,
                ports={f"{self.container_port}/tcp": self.initial_params["port"]},
                name=self.container_name,
            )

        print("Waiting for MongoDB to start...")
        time.sleep(10)

        container.reload()
        return container.status == "running"

    def check_database_exists(self) -> bool:
        try:
            client = MongoClient(
                f"mongodb://{self.initial_params['host']}:{self.initial_params['port']}/"
            )
            database_names = client.list_database_names()
            return self.db_name in database_names
        except Exception as e:
            print(f"An error occurred while checking the MongoDB database: {e}")
            return False

    def drop_database(self) -> None:
        try:
            client = MongoClient(
                f"mongodb://{self.initial_params['host']}:{self.initial_params['port']}/"
            )
            client.drop_database(self.db_name)
            print(f"MongoDB database '{self.db_name}' dropped successfully.")
        except Exception as e:
            print(f"An error occurred while dropping the MongoDB database: {e}")

    def create_database(self) -> None:
        try:
            client = MongoClient(
                f"mongodb://{self.initial_params['host']}:{self.initial_params['port']}/"
            )
            db = client[self.db_name]
            db.create_collection("test_collection")
            print(f"MongoDB database '{self.db_name}' created successfully.")
        except Exception as e:
            print(f"An error occurred while creating the MongoDB database: {e}")

    def execute_script(self, script_path: str) -> None:
        if not script_path:
            print("No script path provided. Skipping script execution.")
            return

        try:
            with open(script_path, "r") as file:
                data = json.load(file)

            if isinstance(data, dict):
                data = [data]

            client = MongoClient(
                f"mongodb://{self.initial_params['host']}:{self.initial_params['port']}/"
            )

            if isinstance(data, list):
                try:
                    for doc in data:
                        if "_id" not in doc:
                            doc["_id"] = ObjectId()
                        else:
                            doc["_id"] = ObjectId(doc["_id"])
                    client[self.db_name]["test_collection"].insert_many(data, ordered=False)
                    print("MongoDB script executed successfully.")
                except Exception as e:
                    print(f"An error occurred while inserting documents: {e}")
            else:
                print("Unknown data format. Skipping import.")

        except Exception as e:
            print(f"An error occurred while executing the MongoDB script: {e}")
            return

    def stop_container(self, remove: bool = False) -> None:
        client = docker.from_env()

        try:
            container = client.containers.get(self.container_name)
            if container.status == "running":
                print("Stopping the MongoDB container...")
                container.stop()
                print("MongoDB container stopped successfully.")
            else:
                print("MongoDB container is not running.")
            if remove:
                print("Removing the MongoDB container...")
                container.remove()
                print("MongoDB container removed successfully.")
        except docker.errors.NotFound:
            print("MongoDB container not found.")


class DatabaseManager:
    @staticmethod
    def get_initializer(
        db_type: DatabaseType, db_params: Dict[str, Any]
    ) -> DatabaseInitializerInterface:
        if db_type == DatabaseType.POSTGRESQL:
            return PostgresDatabaseInitializer(db_params)
        elif db_type == DatabaseType.MONGODB:
            return MongoDBInitializer(db_params)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    @staticmethod
    def initialize_database(
        db_type: DatabaseType, db_params: Dict[str, Any], script_path: str
    ) -> bool:
        initializer = DatabaseManager.get_initializer(db_type, db_params)

        if initializer.start_container():
            print(f"{db_type.value} container is running.")

            if initializer.check_database_exists():
                response = (
                    input(
                        f"Database '{db_params.get('dbname')}' already exists. Do you want to drop it and recreate? (yes/no): "
                    )
                    .strip()
                    .lower()
                )
                if response == "yes":
                    initializer.drop_database()
                    initializer.create_database()
            else:
                initializer.create_database()

            initializer.execute_script(script_path)
            return True
        else:
            print(f"Failed to start {db_type.value} container.")
            return False


def main():
    db_params_pg = {
        "user": "postgres",
        "password": "mysecretpassword",
        "host": "localhost",
        "port": "5432",
        "dbname": "task_management",
    }
    db_params_mn = {"host": "localhost", "port": "27017", "dbname": "cats_db"}

    print("Available database types:")
    for db_type in DatabaseType:
        print(f"- {db_type.value}")

    db_choice = input("Choose database type: ").strip().lower()
    try:
        db_type = DatabaseType(db_choice)
    except ValueError:
        print(f"Invalid database type: {db_choice}")
        return

    script_path = (
        input("Enter the path to the script file (leave empty for no script): ").strip()
    )

    db_params = db_params_pg if db_type == DatabaseType.POSTGRESQL else db_params_mn

    if DatabaseManager.initialize_database(db_type, db_params, script_path):
        print("Database initialization complete.")

        response = (
            input("Do you want to stop the container? (yes/no): ").strip().lower()
        )
        if response == "yes":
            remove = (
                input("Do you want to remove the container? (yes/no): ").strip().lower()
                == "yes"
            )
            initializer = DatabaseManager.get_initializer(db_type, db_params)
            initializer.stop_container(remove)


if __name__ == "__main__":
    main()
