import psycopg2
from faker import Faker
from typing import List, Dict, Any
import random

fake = Faker("uk_UA")

class TaskManager:
    def __init__(self, db_params: Dict[str, Any])-> None:
        """
        Initialize TaskManager object.
        params:
            db_params: Dictionary with database connection parameters
        return:
            None
        """
        self.conn = psycopg2.connect(**db_params)
        self.cursor = self.conn.cursor()

    def __del__(self):
        """Close database connection"""
        if hasattr(self, "conn"):
            self.cursor.close()
            self.conn.close()

    def create_users(self, num_users: int = 10) -> List[int]:
        """
        Create fake users and return their IDs
        params:
            num_users: Number of users to create
        return:
            List of user IDs
        """
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
        """
        Create fake tasks for users
        params:
            user_ids: List of user IDs
            num_tasks: Number of tasks to create
        return:
            None
        """
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
        """Get all tasks for a specific user
        params:
            user_id: User ID
        return:
                List of tasks as dictionaries
        """
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
        """
        Select tasks with specific status
        params:
            status_name: Status name
        return:
            List of tasks as dictionaries
        """
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
        """
        Update task status
        params:
            task_id: Task ID
            new_status: New status name
        return:
                bool: True if update was successful, False otherwise
        """
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
        """
        Get users with no tasks
        return:
            List of users as dictionaries
        """
        self.cursor.execute(
            """
            SELECT *
            FROM users u
            WHERE u.id NOT IN (SELECT DISTINCT user_id FROM tasks)
        """
        )
        return self._fetch_all_as_dicts()

    def add_task(self, title: str, description: str, status: str, user_id: int) -> bool:
        """
        Add new task for specific user
        params:
            title: Task title
            description: Task description
            status: Task status
            user_id: User ID
        return:
            bool: True if task was added successfully, False otherwise
        """
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
        """
        Get all incomplete tasks
        return:
            List of tasks as dictionaries
        """
        self.cursor.execute(
            """
            SELECT t.*, s.name as status
            FROM tasks t
            JOIN status s ON t.status_id = s.id
            WHERE s.name != %s
        """,
            ('completed',),
        )
        return self._fetch_all_as_dicts()

    def delete_task(self, task_id: int) -> bool:
        """
        Delete specific task
        params:
            task_id: Task ID
        return:
            bool: True if task was deleted successfully, False otherwise
        """
        try:
            self.cursor.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error deleting task: {e}")
            return False

    def find_users_by_email(self, email_pattern: str) -> List[Dict[str, Any]]:
        """
        Find users by email pattern
        params:
            email_pattern: Email pattern
        return:
                List of users as dictionaries
        """
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
        """
        Update user's name
        params:
            user_id: User ID
            new_name: New name
        return:
                bool: True if update was successful, False otherwise
        """
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
        """
        Count tasks by status
        return:
            List of dictionaries with status name and task count
        """
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
        """
        Get tasks for users with specific email domain
        params:
            domain: Email domain
        return:
                List of tasks as dictionaries
        """
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
        """
        Get tasks without description
        return:
            List of tasks as dictionaries
        """
        self.cursor.execute(
            """
            SELECT *
            FROM tasks
            WHERE description IS NULL
        """
        )
        return self._fetch_all_as_dicts()

    def get_in_progress_tasks_with_users(self) -> List[Dict[str, Any]]:
        """
        Get users and their in-progress tasks
        return:
            List of dictionaries with user name, task title, and task description
        """
        self.cursor.execute(
            """
            SELECT u.fullname, t.title, t.description
            FROM users u
            JOIN tasks t ON u.id = t.user_id
            JOIN status s ON t.status_id = s.id
            WHERE s.name = %s
        """,
            ('in progress',),
        )
        return self._fetch_all_as_dicts()

    def get_user_task_counts(self) -> List[Dict[str, Any]]:
        """
        Get users and their task counts
        return:
            List of dictionaries with user name and task count"""
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
        """
        Convert query results to list of dictionaries
        return:
            List of dictionaries
        """
        columns = [desc[0] for desc in self.cursor.description]
        results = self.cursor.fetchall()
        return [dict(zip(columns, row)) for row in results]

if __name__ == "__main__":
    pass
