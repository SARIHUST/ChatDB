import pandas as pd
import mysql.connector
import re
import random

# MySQL connection details
MYSQL_HOST = "localhost"
MYSQL_USER = "dsci551"
MYSQL_PASSWORD = "password"
# MYSQL_DATABASE = "551project"
# Connect to MySQL database
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            # database=MYSQL_DATABASE
        )
        print("Connected to MySQL database.")
        return conn
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None
# Function to list all databases
def list_databases(conn):
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES;")
    databases = [db[0] for db in cursor.fetchall()]
    return databases

# Function to select a database
def select_database(conn, database_name):
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE {database_name};")
        # print(f"Switched to database: {database_name}")
        return True
    except Exception as e:
        print(f"Error selecting database {database_name}: {e}")
        return False

# Function to list all tables in the current database
def list_tables(conn):
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]
    return tables

# Function to get metadata for a table
def get_table_metadata(conn, table_name):
    cursor = conn.cursor()
    try:
        cursor.execute(f"DESCRIBE {table_name};")
        metadata = cursor.fetchall()
        return metadata
    except Exception as e:
        print(f"Error describing table {table_name}: {e}")
        return None

# Function to get sample data from a table
def get_sample_data(conn, table_name, limit=5):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        return column_names, rows
    except Exception as e:
        print(f"Error fetching sample data from table {table_name}: {e}")
        return None, None
# Retrieve all table names from the database
def get_all_tables(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES;")
        tables = [table[0] for table in cursor.fetchall()]
        print("Available tables:", tables)
        return tables
    except Exception as e:
        print(f"Error retrieving tables: {e}")
        return []
    
def get_table_metadata(conn, table_name):
    cursor = conn.cursor()
    try:
        cursor.execute(f"DESCRIBE {table_name};")
        columns = cursor.fetchall()
        return columns
    except mysql.connector.Error as e:
        print(f"Error describing table {table_name}: {e}")
        return None
# Upload CSV to MySQL and create a table
def upload_csv_to_mysql(file_path):
    conn = connect_to_mysql()
    if not conn:
        return None, None

    table_name = file_path.split("/")[-1].replace(".csv", "")
    df = pd.read_csv(file_path)

    cursor = conn.cursor()

    # Create table query
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for col in df.columns:
        create_table_query += f"{col} VARCHAR(255), "
    create_table_query = create_table_query.rstrip(", ") + ");"

    try:
        cursor.execute(create_table_query)
        conn.commit()

        # Insert data into the table
        for _, row in df.iterrows():
            columns = ", ".join(row.index)
            placeholders = ", ".join(["%s"] * len(row))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
            cursor.execute(insert_query, tuple(row.values))
        conn.commit()
        print(f"Uploaded {file_path} as table '{table_name}'.")
        return conn, table_name
    except Exception as e:
        print(f"Error uploading CSV: {e}")
        return None, None

# Query templates
query_templates = {
    "basic": r"(list|show) all ((?:\w+\s*,?\s*)+)",
    "where": r"(find|get|show)\s+((?:\w+\s+and\s+)*\w+)\s+where\s+(\w+)\s+(is|=|contains|>|<)\s+(.+)",
    "aggregation": r"(count|sum|average|avg|min|max) of ((?:\w+\s+and\s+)*\w+)",
    "group_by_aggregation": r"(sum|average|count|avg|min|max) ((?:\w+\s+and\s+)*\w+) by (\w+)",
    "group_by_having": r"(find|get|show) ((?:\w+\s+and\s+)*\w+) with (sum|average|count|min|max) (\w+) greater than (\d+)",
    "join": r"(find|get|show) ((?:\w+\.\w+\s+and\s+)*\w+\.\w+) and ((?:\w+\.\w+\s+and\s+)*\w+\.\w+) where (\w+\.\w+) = (\w+\.\w+)",
    "order_by": r"(list|show) ((?:\w+\s+and\s+)*\w+) ordered by (\w+) (asc|desc)?"
}



# Parse user input
def parse_input(user_input,conn):
    
    if user_input.startswith("upload "):
        match = re.match(r"upload (.+\.csv)", user_input)
        if match:
            return "upload", match.group(1)
    elif "sample" in user_input.lower():
            if "basic" in user_input.lower():
                return "sample", "basic"
            elif "where" in user_input.lower():
                return "sample", "where"
            elif "aggregation" in user_input.lower():
                return "sample", "aggregation" 
            elif "group by" in user_input.lower():
                return "sample", "group by" 
            elif "order by" in user_input.lower():
                return "sample", "order by"
            elif "join" in user_input.lower():
                return "sample","join"
            return "sample", None
    elif user_input.lower() == "list databases":
        databases = list_databases(conn)
        if databases:
            return "list_databases", databases
        return "error", "No databases found."

    elif user_input.lower().startswith("use database "):
        match = re.match(r"use database (\w+)", user_input, re.IGNORECASE)
        if match:
            db_name = match.group(1)
            if select_database(conn, db_name):
                return "use_database", f"Switched to database: {db_name}"
            else:
                return "error", f"Failed to switch to database: {db_name}"

    elif user_input.lower() == "list tables":
        tables = list_tables(conn)
        if tables:
            return "list_tables", tables
        return "error", "No tables found in the current database."

    elif user_input.lower().startswith("introduce "):
        match = re.match(r"introduce (\w+)", user_input, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            metadata = get_table_metadata(conn, table_name)
            if metadata:
                result = f"Table: {table_name}\nColumns:\n"
                for column in metadata:
                    col_name = column[0]
                    col_type = column[1]
                    result += f"  - {col_name} ({col_type})\n"
                # Add sample data
                column_names, rows = get_sample_data(conn, table_name)
                if column_names and rows:
                    result += "\nSample Data:\n"
                    result += "\t".join(column_names) + "\n"
                    for row in rows:
                        result += "\t".join(map(str, row)) + "\n"
                return "introduce", result
            return "error", f"Table '{table_name}' does not exist or cannot be described."
    else:
        
        for query_type, pattern in query_templates.items(): 
            if re.search(pattern, user_input, re.IGNORECASE):
                return query_type, user_input
            
    return None, None

# Fetch columns for a specific table
def get_table_columns(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table_name};")
    return [column[0] for column in cursor.fetchall()]
def get_sample_rows(conn, table_name, limit=5):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
    return cursor.fetchall(), [desc[0] for desc in cursor.description]

# Generate random queries
def generate_sample_queries(conn, query_type=None, num_queries=5):
    tables = get_all_tables(conn)
    if not tables:
        return []

    queries = []
    for _ in range(num_queries):
        table = random.choice(tables)
        columns = get_table_columns(conn, table)
        if not columns:
            continue

        if query_type == "basic" or (query_type is None and random.random() < 0.2):
            # Basic query
            col = random.choice(columns)
            query = f"SELECT {col} FROM {table} LIMIT 10;"

        elif query_type == "where" or (query_type is None and random.random() < 0.3):
            # Query with WHERE clause
            col = random.choice(columns)
            sample_rows, column_names = get_sample_rows(conn, table, limit=5)
            if sample_rows:
                random_row = random.choice(sample_rows)
                condition_value = random_row[column_names.index(col)] if col in column_names else None
                if condition_value is not None:
                    query = f"SELECT * FROM {table} WHERE {col} = '{condition_value}' LIMIT 10;"
                else:
                    query = f"SELECT * FROM {table} WHERE {col} IS NOT NULL LIMIT 10;"
            else:
                query = f"SELECT * FROM {table} WHERE {col} IS NOT NULL LIMIT 10;"

        elif query_type == "aggregation" or (query_type is None and random.random() < 0.2):
            # Aggregation query
            col = random.choice(columns)
            agg_func = random.choice(["SUM", "COUNT", "AVG", "MAX", "MIN"])
            query = f"SELECT {agg_func}({col}) FROM {table};"

        elif query_type == "group by" or (query_type is None and random.random() < 0.2):
            # Group by query
            if len(columns) > 1:
                col1, col2 = random.sample(columns, 2)
                query = f"SELECT {col1}, COUNT({col2}) FROM {table} GROUP BY {col1} LIMIT 10;"
            else:
                query = f"SELECT * FROM {table} LIMIT 10;"

        elif query_type == "order by" or (query_type is None and random.random() < 0.1):
            # Order by query
            col = random.choice(columns)
            order = random.choice(["ASC", "DESC"])
            query = f"SELECT * FROM {table} ORDER BY {col} {order} LIMIT 10;"

        elif query_type == "join" or (query_type is None and random.random() < 0.1):
            # Join query (requires at least 2 tables)
            if len(tables) > 1:
                table2 = random.choice([t for t in tables if t != table])
                columns2 = get_table_columns(conn, table2)
                if columns2:
                    col1 = random.choice(columns)
                    col2 = random.choice(columns2)
                    query = f"SELECT {table}.{col1}, {table2}.{col2} FROM {table} JOIN {table2} ON {table}.{col1} = {table2}.{col2} LIMIT 10;"
                else:
                    query = f"SELECT * FROM {table} LIMIT 10;"
            else:
                query = f"SELECT * FROM {table} LIMIT 10;"

        else:
            continue

        # Add the generated query to the list
        queries.append(query)
    return queries

# Process queries based on type
def process_query(query_type, user_input, table_name):
    match = None
    if query_type == "basic":
        match = re.search(query_templates["basic"], user_input, re.IGNORECASE)
        column = match.group(1)
        sql = f"SELECT {column} FROM {table_name};"
    elif query_type == "where":
        match = re.search(query_templates["where"], user_input, re.IGNORECASE)
        column, condition_column, operator, value = match.group(2), match.group(3), match.group(4), match.group(5)
        operator = "=" if operator == "is" else operator  # Replace "is" with "="
        sql = f"SELECT {column} FROM {table_name} WHERE {condition_column} {operator} '{value}';"
    elif query_type == "aggregation":
        match = re.search(query_templates["aggregation"], user_input, re.IGNORECASE)
        func, column = match.group(1), match.group(2)
        sql = f"SELECT {func.upper()}({column}) FROM {table_name};"
    elif query_type == "group_by_aggregation":
        match = re.search(query_templates["group_by_aggregation"], user_input, re.IGNORECASE)
        func, column, group_by_column = match.group(1), match.group(2), match.group(3)
        sql = f"SELECT {group_by_column}, {func.upper()}({column}) FROM {table_name} GROUP BY {group_by_column};"
    elif query_type == "group_by_having":
        match = re.search(query_templates["group_by_having"], user_input, re.IGNORECASE)
        column, func, agg_column, value = match.group(2), match.group(3), match.group(4), match.group(5)
        sql = f"""
            SELECT {column}, {func.upper()}({agg_column}) AS agg_value 
            FROM {table_name} 
            GROUP BY {column} 
            HAVING agg_value > {value};
        """
    elif query_type == "join":
        match = re.search(query_templates["join"], user_input, re.IGNORECASE)
        col1, col2, condition1, condition2 = match.group(2), match.group(3), match.group(4), match.group(5)
        table1, column1 = col1.split(".")
        table2, column2 = col2.split(".")
        sql = f"""
            SELECT {col1}, {col2} 
            FROM {table1} 
            JOIN {table2} ON {condition1} = {condition2};
        """
    elif query_type == "order_by":
        print(user_input)
        match = re.search(query_templates["order_by"], user_input, re.IGNORECASE)
        column, order_column, order = match.group(2), match.group(3), match.group(4) or "ASC"
        sql = f"SELECT {column} FROM {table_name} ORDER BY {order_column} {order.upper()};"
    else:
        sql = None
    print(sql)
    return sql

# Execute an SQL query and fetch results
def execute_query(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

# Main chatbot loop
def chatbot():
    print("Hello! I'm your database assistant. You can upload CSV files, ask for sample queries, or ask natural language questions about your data.")
    conn = connect_to_mysql()
    if not conn:
        print("Failed to connect to the database.")
        return
    
    # tables = get_all_tables(conn)  # Get all existing tables in the database
    
    while True:
        user_input = input("\nYou: ")
        action, data = parse_input(user_input,conn)
        
        if action == "upload":
            conn, table_name = upload_csv_to_mysql(data)
            if not conn:
                print("Failed to upload and process the CSV file.")
            else:
                tables = get_all_tables(conn)  # Update table list
        # elif action == "sample_":
        #     if tables:
        #         print("\nSample Queries:")
        #         for table_name in tables:
        #             print(f"Table: {table_name}")
        #             print(f"  SELECT * FROM {table_name} LIMIT 10;")
        #             print(f"  SELECT COUNT(*) FROM {table_name};")
        #             print(f"  SELECT column_name FROM {table_name} WHERE column_name = 'example';")
        #     else:
        #         print("No tables found. Please upload a CSV file or check your database.")
        elif action == "list_databases":
            print("Available Databases:")
            print("\n".join(data))
        elif action == "use_database":
            print(data)
        elif action == "list_tables":
            print("Available Tables:")
            print("\n".join(data))
        elif action == "introduce":
            print(data)
        elif action == "error":
            print(f"Error: {data}")
        elif action ==  "sample":
            print(generate_sample_queries(conn,query_type=data))
        elif action in query_templates.keys():
            if tables:
                table_name = tables[0]  # Default to the first table for simplicity
                sql_query = process_query(action, data, table_name)
                if sql_query:
                    results = execute_query(conn, sql_query)
                    if results:
                        print("\nQuery Results:")
                        for row in results:
                            print(" , ".join(map(str, row)))
                    else:
                        print("No results found or an error occurred.")
                else:
                    print("Could not generate a valid SQL query. Please try again.")
            else:
                print("No tables found. Please upload a CSV file or check your database.")
        else:
            print("Invalid command.")

# Run the chatbot
if __name__ == "__main__":
    chatbot()