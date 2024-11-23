import time
import docker
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Dict, Any

INITIAL_DB_PARAMS = {
    "user": "postgres",
    "password": "mysecretpassword",
    "host": "localhost",
    "port": "5432"
}

# Database connection parameters for working with task_management database
DB_PARAMS = {
    **INITIAL_DB_PARAMS,
    "dbname": "task_management"
}

class DatabaseInitializer:
    @staticmethod
    def start_postgres_container(db_params: Dict[str, Any]) -> bool:
        """
        Start or create a PostgreSQL Docker container.
        params:
            db_params: Dictionary with database connection parameters
        returns:
            bool: True if the container is running, False otherwise
        """
        client = docker.from_env()

        try:
            container = client.containers.get("my_postgres_container")
            if container.status != "running":
                print("Container exists but is not running. Starting container...")
                container.start()
            else:
                print("Container is already running.")
                container.reload()
                return container.status == "running"
        except docker.errors.NotFound:
            print("Container not found. Creating and starting a new container...")
            container = client.containers.run(
                "postgres:latest",
                detach=True,
                ports={"5432/tcp": db_params["port"]},
                environment={
                    "POSTGRES_USER": db_params["user"],
                    "POSTGRES_PASSWORD": db_params["password"],
                },
                name="my_postgres_container",
            )

        print("Waiting for PostgreSQL database to start...")
        time.sleep(10)

        # Check if the container is running
        container.reload()
        return container.status == "running"

    @staticmethod
    def check_database_exists(db_params: Dict[str, Any]) -> bool:
        """
        Check if a database exists.
        params:
            db_params: Dictionary with database connection parameters
        returns:
                bool: True if the database exists, False otherwise
        """
        conn_params = db_params.copy()
        conn_params["dbname"] = "postgres"  # Connect to the default postgres database

        try:
            conn = psycopg2.connect(**conn_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_params["dbname"],))
            exists = cursor.fetchone() is not None

            cursor.close()
            conn.close()
            return exists
        except Exception as e:
            print(f"An error occurred while checking the database: {e}")
            return False

    @staticmethod
    def drop_database(db_params: Dict[str, Any])-> None:
        """
        Drop an existing database.
        params:
            db_params: Dictionary with database connection parameters
        return:
            None
        """
        conn_params = db_params.copy()
        conn_params["dbname"] = "postgres"  # Connect to the default postgres database

        try:
            conn = psycopg2.connect(**conn_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            cursor.execute(f"DROP DATABASE IF EXISTS {db_params['dbname']}")
            print(f"Database '{db_params['dbname']}' dropped successfully.")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"An error occurred while dropping the database: {e}")

    @staticmethod
    def create_database(db_params: Dict[str, Any])-> None:
        """
        Create a new database.
        params:
            db_params: Dictionary with database connection parameters
        return:
            None
        """
        conn_params = db_params.copy()
        conn_params["dbname"] = "postgres"  # Connect to the default postgres database

        try:
            conn = psycopg2.connect(**conn_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            cursor.execute(f"CREATE DATABASE {db_params['dbname']}")
            print(f"Database '{db_params['dbname']}' created successfully.")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"An error occurred while creating the database: {e}")

    @staticmethod
    def execute_sql_script(db_params: Dict[str, Any], sql_file_path: str)-> None:
        """
        Execute an SQL script on the specified database.
        params:
            db_params: Dictionary with database connection parameters
            sql_file_path: Path to the SQL file
        return:
            None
        """
        try:
            conn = psycopg2.connect(**db_params)
            cursor = conn.cursor()

            with open(sql_file_path, 'r') as sql_file:
                sql_script = sql_file.read()

            cursor.execute(sql_script)
            conn.commit()
            print("SQL script executed successfully.")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"An error occurred while executing the SQL script: {e}")

    @staticmethod
    def initialize_database(sql_file_path: str)-> bool:
        """
        Initialize the database: check, drop (if needed), create, and execute SQL.
        params:
            sql_file_path: Path to the SQL file
        return:
            None
        """
        if DatabaseInitializer.start_postgres_container(INITIAL_DB_PARAMS):
            print("PostgreSQL container is running.")

            if DatabaseInitializer.check_database_exists(DB_PARAMS):
                response = input(f"Database '{DB_PARAMS['dbname']}' already exists. Do you want to drop it and recreate? (yes/no): ").strip().lower()
                if response == "yes":
                    DatabaseInitializer.drop_database(DB_PARAMS)
                    DatabaseInitializer.create_database(DB_PARAMS)
            else:
                DatabaseInitializer.create_database(DB_PARAMS)

            DatabaseInitializer.execute_sql_script(DB_PARAMS, sql_file_path)
            return True
        else:
            print("Failed to start PostgreSQL container.")
            return False

    @staticmethod
    def stop_postgres_container(remove=False) -> None:
        """
        Stop the PostgreSQL Docker container.
        params:
            remove: bool: True to remove the container, False to stop only
        return:
            None
        """
        client = docker.from_env()

        try:
            container = client.containers.get("my_postgres_container")
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

if __name__ == "__main__":
    if DatabaseInitializer.initialize_database("task_management_backup.sql"):
        print("Database initialization complete.")
        # DatabaseInitializer.stop_postgres_container(remove=True)

