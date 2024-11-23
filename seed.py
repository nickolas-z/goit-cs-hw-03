import docker
import time
import psycopg2
from faker import Faker
import random
from psycopg2 import Error
from typing import List, Dict, Any
import os

fake = Faker("uk_UA")

# Database connection parameters for initial connection
DB_PARAMS = {
    "dbname": "task_management",
    "user": "postgres",
    "password": "mysecretpassword",
    "host": "localhost",
    "port": "5432",
}



SQL_SCRIPT = """
-- Drop database if exists (be careful with this in production!)
DROP DATABASE IF EXISTS task_management;

-- Create database
CREATE DATABASE task_management;

-- Connect to the database
-- This command should be executed separately, not within the script

-- Create users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    fullname VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
);

-- Create status table
CREATE TABLE status (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

-- Create tasks table
CREATE TABLE tasks (
    id SERIAL PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    description TEXT,
    status_id INTEGER REFERENCES status(id),
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial status values
INSERT INTO status (name) VALUES 
    ('new'),
    ('in progress'),
    ('completed');
"""


class DatabaseInitializer:
    @staticmethod
    def init_database():
        """Initialize database from SQL script"""
        try:
            # Connect to PostgreSQL server (without selecting a database)
            conn = psycopg2.connect(**INITIAL_DB_PARAMS)
            conn.autocommit = True
            cursor = conn.cursor()

            print("Creating database...")
            cursor.execute(SQL_SCRIPT.split(';')[0] + ';')  # Execute the first part to create the database
            conn.close()  # Close initial connection

            # Connect to the newly created database
            conn = psycopg2.connect(**DB_PARAMS)
            conn.autocommit = True
            cursor = conn.cursor()

            # Execute the rest of the script
            for command in SQL_SCRIPT.split(';')[1:]:
                if command.strip():
                    cursor.execute(command + ';')
            print("Database initialized successfully!")

        except psycopg2.Error as e:
            print(f"Database error: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise
        finally:
            if "conn" in locals():
                cursor.close()
                conn.close()
                print("Initial database connection closed.")


class TaskManager:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_PARAMS)
        self.cursor = self.conn.cursor()

    def __del__(self):
        if hasattr(self, "conn"):
            self.cursor.close()
            self.conn.close()

    def create_users(self, num_users: int = 10) -> List[int]:
        """Create fake users and return their IDs"""
        user_ids = []
        for _ in range(num_users):
            fullname = fake.name()
            email = f"{fake.user_name()}@{fake.domain_name()}"
            self.cursor.execute(
                "INSERT INTO users (fullname, email) VALUES (%s, %s) RETURNING id",
                (fullname, email),
            )
            user_ids.append(self.cursor.fetchone()[0])
        self.conn.commit()
        return user_ids

    def create_tasks(self, user_ids: List[int], num_tasks: int = 30) -> None:
        """Create fake tasks for users"""
        status_ids = [1, 2, 3]  # new, in progress, completed

        for _ in range(num_tasks):
            title = fake.sentence(nb_words=4)
            description = fake.text() if random.random() > 0.2 else None
            status_id = random.choice(status_ids)
            user_id = random.choice(user_ids)

            self.cursor.execute(
                """
                INSERT INTO tasks (title, description, status_id, user_id)
                VALUES (%s, %s, %s, %s)
                """,
                (title, description, status_id, user_id),
            )
        self.conn.commit()

    def get_user_tasks(self, user_id: int) -> List[Dict[str, Any]]:
        """Get all tasks for a specific user"""
        self.cursor.execute(
            """
            SELECT t.id, t.title, t.description, s.name as status
            FROM tasks t
            JOIN status s ON t.status_id = s.id
            WHERE t.user_id = %s
        """,
            (user_id,),
        )
        return self._fetch_all_as_dicts()

    def get_tasks_by_status(self, status_name: str) -> List[Dict[str, Any]]:
        """Select tasks with specific status"""
        self.cursor.execute(
            """
            SELECT *
            FROM tasks
            WHERE status_id = (SELECT id FROM status WHERE name = %s)
        """,
            (status_name,),
        )
        return self._fetch_all_as_dicts()

    def update_task_status(self, task_id: int, new_status: str) -> bool:
        """Update task status"""
        try:
            self.cursor.execute(
                """
                UPDATE tasks
                SET status_id = (SELECT id FROM status WHERE name = %s)
                WHERE id = %s
            """,
                (new_status, task_id),
            )
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error updating task status: {e}")
            return False

    def get_users_without_tasks(self) -> List[Dict[str, Any]]:
        """Get users with no tasks"""
        self.cursor.execute(
            """
            SELECT *
            FROM users u
            WHERE u.id NOT IN (SELECT DISTINCT user_id FROM tasks)
        """
        )
        return self._fetch_all_as_dicts()

    def add_task(self, title: str, description: str, status: str, user_id: int) -> bool:
        """Add new task for specific user"""
        try:
            self.cursor.execute(
                """
                INSERT INTO tasks (title, description, status_id, user_id)
                VALUES (%s, %s, (SELECT id FROM status WHERE name = %s), %s)
            """,
                (title, description, status, user_id),
            )
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error adding task: {e}")
            return False

    def get_incomplete_tasks(self) -> List[Dict[str, Any]]:
        """Get all incomplete tasks"""
        self.cursor.execute(
            """
            SELECT t.*, s.name as status
            FROM tasks t
            JOIN status s ON t.status_id = s.id
            WHERE s.name != 'completed'
        """
        )
        return self._fetch_all_as_dicts()

    def delete_task(self, task_id: int) -> bool:
        """Delete specific task"""
        try:
            self.cursor.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error deleting task: {e}")
            return False

    def find_users_by_email(self, email_pattern: str) -> List[Dict[str, Any]]:
        """Find users by email pattern"""
        self.cursor.execute(
            """
            SELECT *
            FROM users
            WHERE email LIKE %s
        """,
            (f"%{email_pattern}%",),
        )
        return self._fetch_all_as_dicts()

    def update_user_name(self, user_id: int, new_name: str) -> bool:
        """Update user's name"""
        try:
            self.cursor.execute(
                """
                UPDATE users
                SET fullname = %s
                WHERE id = %s
            """,
                (new_name, user_id),
            )
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error updating user name: {e}")
            return False

    def get_tasks_by_status_count(self) -> List[Dict[str, Any]]:
        """Count tasks by status"""
        self.cursor.execute(
            """
            SELECT s.name, COUNT(t.id) as task_count
            FROM status s
            LEFT JOIN tasks t ON s.id = t.status_id
            GROUP BY s.name
            ORDER BY s.name
        """
        )
        return self._fetch_all_as_dicts()

    def get_tasks_by_email_domain(self, domain: str) -> List[Dict[str, Any]]:
        """Get tasks for users with specific email domain"""
        self.cursor.execute(
            """
            SELECT t.*, u.email
            FROM tasks t
            JOIN users u ON t.user_id = u.id
            WHERE u.email LIKE %s
        """,
            (f"%@{domain}",),
        )
        return self._fetch_all_as_dicts()

    def get_tasks_without_description(self) -> List[Dict[str, Any]]:
        """Get tasks without description"""
        self.cursor.execute(
            """
            SELECT *
            FROM tasks
            WHERE description IS NULL
        """
        )
        return self._fetch_all_as_dicts()

    def get_in_progress_tasks_with_users(self) -> List[Dict[str, Any]]:
        """Get users and their in-progress tasks"""
        self.cursor.execute(
            """
            SELECT u.fullname, t.title, t.description
            FROM users u
            JOIN tasks t ON u.id = t.user_id
            JOIN status s ON t.status_id = s.id
            WHERE s.name = 'in progress'
        """
        )
        return self._fetch_all_as_dicts()

    def get_user_task_counts(self) -> List[Dict[str, Any]]:
        """Get users and their task counts"""
        self.cursor.execute(
            """
            SELECT u.fullname, COUNT(t.id) as task_count
            FROM users u
            LEFT JOIN tasks t ON u.id = t.user_id
            GROUP BY u.id, u.fullname
            ORDER BY task_count DESC
        """
        )
        return self._fetch_all_as_dicts()

    def _fetch_all_as_dicts(self) -> List[Dict[str, Any]]:
        """Convert query results to list of dictionaries"""
        columns = [desc[0] for desc in self.cursor.description]
        results = self.cursor.fetchall()
        return [dict(zip(columns, row)) for row in results]


def demonstrate_all_functions():
    """Function to demonstrate all TaskManager functionality"""
    try:
        # First, initialize the database
        print("Initializing database...")
        DatabaseInitializer.init_database()

        print("\nStarting TaskManager demonstration...")
        tm = TaskManager()

        # 1. Create users and tasks
        print("\n1. Creating users and tasks...")
        user_ids = tm.create_users(5)
        tm.create_tasks(user_ids, 15)
        print(f"Created {len(user_ids)} users and 15 tasks")

        # 2. Get tasks for first user
        print("\n2. Getting tasks for first user:")
        user_tasks = tm.get_user_tasks(user_ids[0])
        print(f"Found {len(user_tasks)} tasks for user {user_ids[0]}")
        for task in user_tasks:
            print(f"- {task['title']} ({task['status']})")

        # 3. Get tasks by status
        print("\n3. Getting 'new' tasks:")
        new_tasks = tm.get_tasks_by_status("new")
        print(f"Found {len(new_tasks)} new tasks")

        # 4. Update task status
        if new_tasks:
            print("\n4. Updating first 'new' task to 'in progress':")
            success = tm.update_task_status(new_tasks[0]["id"], "in progress")
            print(f"Status update {'successful' if success else 'failed'}")

        # 5. Get users without tasks
        print("\n5. Finding users without tasks:")
        users_no_tasks = tm.get_users_without_tasks()
        print(f"Found {len(users_no_tasks)} users without tasks")

        # 6. Add new task
        print("\n6. Adding new task:")
        success = tm.add_task("Test Task", "Test Description", "new", user_ids[0])
        print(f"Task creation {'successful' if success else 'failed'}")

        # 7. Get incomplete tasks
        print("\n7. Getting incomplete tasks:")
        incomplete = tm.get_incomplete_tasks()
        print(f"Found {len(incomplete)} incomplete tasks")

        # 8. Finding users by email domain
        print("\n8. Finding users by email domain:")
        users = tm.find_users_by_email("gmail.com")
        print(f"Found {len(users)} users with gmail.com")

        # 9. Update user name
        if user_ids:
            print("\n9. Updating user name:")
            success = tm.update_user_name(user_ids[0], "New Test Name")
            print(f"Name update {'successful' if success else 'failed'}")

        # 10. Get task counts by status
        print("\n10. Getting task counts by status:")
        status_counts = tm.get_tasks_by_status_count()
        for status in status_counts:
            print(f"- {status['name']}: {status['task_count']} tasks")

        # 11. Get tasks without description
        print("\n11. Getting tasks without description:")
        no_desc_tasks = tm.get_tasks_without_description()
        print(f"Found {len(no_desc_tasks)} tasks without description")

        # 12. Get in-progress tasks with users
        print("\n12. Getting in-progress tasks with users:")
        in_progress = tm.get_in_progress_tasks_with_users()
        print(f"Found {len(in_progress)} in-progress tasks")

        # 13. Get user task counts
        print("\n13. Getting user task counts:")
        user_counts = tm.get_user_task_counts()
        for user in user_counts:
            print(f"- {user['fullname']}: {user['task_count']} tasks")

        print("\nDemonstration completed successfully!")

    except Exception as e:
        print(f"An error occurred during demonstration: {e}")
    finally:
        if "tm" in locals():
            del tm

def start_postgres_container(db_params)->None:
    client = docker.from_env()

    # Check if the container already exists
    try:
        container = client.containers.get("my_postgres_container")
        if container.status != "running":
            print("Container exists but is not running. Starting container...")
            container.start()
        else:
            print("Container is already running.")
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

    # Wait for the database to start
    print("Waiting for PostgreSQL database to start...")
    time.sleep(10)  # Usually enough time, but can be increased if needed

    # Connect to PostgreSQL server and create the database
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=db_params["user"],
            password=db_params["password"],
            host=db_params["host"],
            port=db_params["port"],
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Create the new database
        cursor.execute(f"CREATE DATABASE {db_params['dbname']}")
        print(f"Database '{db_params['dbname']}' created successfully!")

    except Exception as e:
        print(f"Error connecting to or creating the database: {e}")

    finally:
        # Close cursor and connection
        if "cursor" in locals():
            cursor.close()
        if "conn" in locals():
            conn.close()


def main():
    try:
        demonstrate_all_functions()
    except Exception as e:
        print(f"Failed to run demonstration: {e}")
        exit(1)


if __name__ == "__main__":
    start_postgres_container(DB_PARAMS)
    # main()
    # Доступ до контейнера через команду
    # container.stop()  # Зупинка контейнера після виконання завдань, якщо потрібно
