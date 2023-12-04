# Import the necessary functions from postgres_functions.py
from postgres_functions import connect_to_postgres, create_tables_from_sql_file, insert_data_into_table
import pandas as pd

def main():
    # PostgreSQL connection parameters
    dbname = "slack_feature"
    user = "postgres"
    password = "postgres1234"
    host = "localhost"
    port = "5432"

    # Path to the SQL file containing table creation queries
    sql_file_path = "slack_feature.sql"

    # Connect to PostgreSQL
    connection, cursor = connect_to_postgres(dbname, user, password, host, port)

    try:
        # Create tables from the SQL file
        create_tables_from_sql_file(connection, cursor, sql_file_path)

        # Commit the changes
        connection.commit()

        print("Tables created successfully!")

        # Insert data into the 'users' table
        users_data = "../data/users.csv"
        insert_data_into_table(connection, cursor, "users", pd.read_csv(users_data).to_dict('records'))

        # Insert data into the 'channels' table
        channels_data = "../data/channels.csv"
        insert_data_into_table(connection, cursor, "channels", pd.read_csv(channels_data).to_dict('records'))

        # Insert data into the 'channel_members' table
        channel_members = "../data/channel_members.csv"
        insert_data_into_table(connection, cursor, "channel_members", pd.read_csv(channel_members).to_dict('records'))

        # Insert data into the 'messages' table
        messages_data_file = "../data/messages.csv"
        insert_data_into_table(connection, cursor, "messages", pd.read_csv(messages_data_file).to_dict('records'))

        print("Data inserted successfully!")

    except Exception as e:
        # Rollback the transaction in case of an error
        connection.rollback()
        print(f"Error: {e}")

    finally:
        # Close the connection
        connection.close()

if __name__ == "__main__":
    main()
