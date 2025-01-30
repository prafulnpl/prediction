import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Load environment variables from .env
print("Loading environment variables...")
load_dotenv()

def create_connection():
    try:
        # Read credentials from .env file
        host = os.getenv("DB_HOST")
        database = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        port = os.getenv("DB_PORT")

        # Debug: Print the credentials to verify they are loaded correctly
        print(f"Debug Info - Host: {host}, Database: {database}, User: {user}, Port: {port}, Password: {'Set' if password else 'Not Set'}")

        # Check if all credentials are available
        if not all([host, database, user, port]):
            print("Error: Missing required database credentials. Check your .env file.")
            return None, None

        # Establish connection to PostgreSQL
        print("Attempting to establish connection to the database...")
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cursor = connection.cursor()
        print("Database connection established successfully!")
        return connection, cursor

    except Exception as error:
        print(f"Error connecting to the database: {error}")
        return None, None

def close_connection(connection, cursor):
    # Debug: Check if connection and cursor are valid before closing
    if cursor:
        print("Closing the cursor...")
        cursor.close()
    if connection:
        print("Closing the database connection...")
        connection.close()
    print("Database connection closed.")

# Main function to run the test
if __name__ == "__main__":
    print("Starting the connection test...")
    connection, cursor = create_connection()
    if connection:
        print("Connection test successful!")
        close_connection(connection, cursor)
    else:
        print("Connection failed.")


    print(f"Connected to database: {os.getenv('DB_NAME')}")
