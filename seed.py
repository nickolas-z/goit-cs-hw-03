from DatabaseManager import DatabaseManager
from DatabaseManager import DatabaseType
from TaskManager import TaskManager

DB_PARAMS = {
    "user": "postgres",
    "password": "mysecretpassword",
    "host": "localhost",
    "port": "5432",
    "dbname": "task_management",
}

def demonstrate_all_functions(db_params: dict = DB_PARAMS)-> None:
    """
    Function to demonstrate all TaskManager functionality
    params:
        db_params: Dictionary with database connection parameters
    return:
        None
    """

    try:
        print("\nStarting TaskManager demonstration...")
        tm = TaskManager(db_params)

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
        if user_ids:
            print("\nUpdating email for first user to 'new_email@gmail.com':")
            success = tm.update_user_email(user_ids[0], "new_email@gmail.com")
            print(f"Email update {'successful' if success else 'failed'}")
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

        # 14. Delete a specific task
        if new_tasks:
            print("\n14. Deleting first 'new' task:")
            success = tm.delete_task(new_tasks[0]["id"])
            print(f"Task deletion {'successful' if success else 'failed'}")

        # 15. Delete a specific user with cascade deletion
        if user_ids:
            print("\n15. Deleting first user with cascade deletion:")
            user_info = tm.get_user_info(user_ids[0])
            print(f"User info before deletion: {user_info}")
            success = tm.delete_user(user_ids[0])
            print(f"User deletion {'successful' if success else 'failed'}")
            # user_info = tm.get_user_info(user_ids[0])
            # print(f"User info after deletion: {user_info}")

        print("\nDemonstration completed successfully!")

    except Exception as e:
        print(f"An error occurred during demonstration: {e}")
    finally:
        if "tm" in locals():
            del tm

if __name__ == "__main__":
    if DatabaseManager.initialize_database(DatabaseType.POSTGRESQL, DB_PARAMS, "task_management_backup.sql"):
        print("Database initialization complete.")
        demonstrate_all_functions()
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
                DatabaseType.POSTGRESQL, DB_PARAMS
            )
            initializer.stop_container(remove)

