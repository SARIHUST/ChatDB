import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import re

def read_csv_file(file_path):
    """Read the CSV file and return a DataFrame."""
    return pd.read_csv(file_path)

def create_mysql_database(db_name, user, password, host='localhost'):
    """Connect to MySQL server and create a new database."""
    conn = mysql.connector.connect(user=user, password=password, host=host)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    conn.close()
    print(f"Database '{db_name}' is ready.")

def create_table_and_insert_data(df, db_name, table_name, user, password, host='localhost'):
    """Create a table in MySQL database from the DataFrame structure and insert data."""
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db_name}")
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data inserted into '{table_name}' table in '{db_name}' database.")

# Define query templates and natural language patterns
query_templates = {
    "basic": r"list|show all (\w+)",
    "where": r"(find|get|show) (\w+) where (\w+) (is|=|contains|>|<) (.+)",
    "aggregation": r"(count|sum|average|avg|min|max) of (\w+)",
    "group_by_aggregation": r"(sum|average|count|avg|min|max) (\w+) by (\w+)",
    "group_by_having": r"(find|get|show) (\w+) with (sum|average|count|min|max) (\w+) greater than (\d+)",
    "join": r"(find|get|show) (\w+\.\w+) and (\w+\.\w+) where (\w+\.\w+) = (\w+\.\w+)",
    "order_by": r"(list|show) (\w+) ordered by (\w+) (asc|desc)?"
}

def generate_query(nl_query, table_name):
    """Generate SQL query based on natural language input."""
    nl_query = nl_query.lower()
    for query_type, pattern in query_templates.items():
        match = re.search(pattern, nl_query)
        if match:
            if query_type == "basic":
                column = match.group(1)
                return f"SELECT {column} FROM {table_name};"
            elif query_type == "where":
                column, condition_column, operator, value = match.groups()[1:]
                if operator == "contains":
                    return f"SELECT {column} FROM {table_name} WHERE {condition_column} LIKE '%{value}%';"
                return f"SELECT {column} FROM {table_name} WHERE {condition_column} {operator} {value};"
            elif query_type == "aggregation":
                agg_func, column = match.groups()
                return f"SELECT {agg_func.upper()}({column}) FROM {table_name};"
            elif query_type == "group_by_aggregation":
                agg_func, column, group_column = match.groups()
                return f"SELECT {group_column}, {agg_func.upper()}({column}) FROM {table_name} GROUP BY {group_column};"
            elif query_type == "group_by_having":
                column, agg_func, condition_column, threshold = match.groups()[1:]
                return f"SELECT {condition_column}, {agg_func.upper()}({column}) AS total FROM {table_name} GROUP BY {condition_column} HAVING total > {threshold};"
            elif query_type == "join":
                column1, column2, join_condition1, join_condition2 = match.groups()
                table1, table2 = column1.split(".")[0], column2.split(".")[0]
                return f"SELECT {column1}, {column2} FROM {table1} JOIN {table2} ON {join_condition1} = {join_condition2};"
            elif query_type == "order_by":
                column, order_column, order = match.groups()[1:]
                order = order or "ASC"
                return f"SELECT {column} FROM {table_name} ORDER BY {order_column} {order.upper()};"
    return "I couldn't understand your query."

def execute_query(query, db_name, user, password, host='localhost'):
    """Execute the generated SQL query and return the result."""
    conn = mysql.connector.connect(user=user, password=password, host=host, database=db_name)
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    return pd.DataFrame(result, columns=columns)

def main():
    # Prompt for CSV file path and database/table details
    file_path = input("Please provide the path to your CSV file: ")
    db_name = input("Enter a name for the MySQL database: ")
    table_name = input("Enter a name for the table to create from the CSV file: ")
    user = input("Enter your MySQL username: ")
    password = input("Enter your MySQL password: ")
    host = input("Enter your MySQL host (default is 'localhost'): ") or 'localhost'

    # Step 1: Read the CSV file
    print("Loading the CSV file data...")
    df = read_csv_file(file_path)

    # Step 2: Create the MySQL database
    print("Setting up the MySQL database...")
    create_mysql_database(db_name, user, password, host)

    # Step 3: Create table and insert data into MySQL
    print("Creating table and inserting data into MySQL...")
    create_table_and_insert_data(df, db_name, table_name, user, password, host)

    print("\nThe database is ready! You can now ask questions about your data.")
    print("Type 'exit' to end the session.")

    # Chat loop
    while True:
        nl_query = input("\nHow can I help you? ")
        if nl_query.lower() == "exit":
            print("Goodbye!")
            break

        # Generate SQL query based on natural language
        sql_query = generate_query(nl_query, table_name)
        if "I couldn't understand" in sql_query:
            print(sql_query)
            continue

        print(f"SQL Query generated: {sql_query}")

        # Execute the SQL query and display the result
        try:
            result_df = execute_query(sql_query, db_name, user, password, host)
            if not result_df.empty:
                print(result_df)
            else:
                print("No results found.")
        except Exception as e:
            print(f"An error occurred while executing the query: {e}")

if __name__ == "__main__":
    main()
