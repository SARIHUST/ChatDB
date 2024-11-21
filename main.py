import pandas as pd
import mysql.connector
import re

# MySQL connection details
MYSQL_HOST = "localhost"
MYSQL_USER = "dsci551"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "551project"

# Connect to MySQL database
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        print("Connected to MySQL database.")
        return conn
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# Create table and load CSV into MySQL
def upload_csv_to_mysql(file_path):
    conn = connect_to_mysql()
    if not conn:
        return None, None

    table_name = file_path.split("/")[-1].replace(".csv", "")
    df = pd.read_csv(file_path)

    # Create table query
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for col in df.columns:
        create_table_query += f"{col} VARCHAR(255), "
    create_table_query = create_table_query.rstrip(", ") + ");"

    cursor = conn.cursor()
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
    "basic": r"list|show all (\w+)",
    "where": r"(find|get|show) (\w+) where (\w+) (is|=|contains|>|<) (.+)",
    "aggregation": r"(count|sum|average|avg|min|max) of (\w+)",
    "group_by_aggregation": r"(sum|average|count|avg|min|max) (\w+) by (\w+)",
    "group_by_having": r"(find|get|show) (\w+) with (sum|average|count|min|max) (\w+) greater than (\d+)",
    "join": r"(find|get|show) (\w+\.\w+) and (\w+\.\w+) where (\w+\.\w+) = (\w+\.\w+)",
    "order_by": r"(list|show) (\w+) ordered by (\w+) (asc|desc)?"
}

# Parse user input
def parse_input(user_input):
    if user_input.startswith("upload "):
        match = re.match(r"upload (.+\.csv)", user_input)
        if match:
            return "upload", match.group(1)
    elif user_input.lower() == "give sample queries":
        return "sample_queries", None
    else:
        for query_type, pattern in query_templates.items():
            if re.search(pattern, user_input, re.IGNORECASE):
                return query_type, user_input
    return None, None

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
        print(sql)
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
        match = re.search(query_templates["order_by"], user_input, re.IGNORECASE)
        column, order_column, order = match.group(2), match.group(3), match.group(4) or "ASC"
        sql = f"SELECT {column} FROM {table_name} ORDER BY {order_column} {order.upper()};"
    else:
        sql = None
    
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
    conn = None  # Global connection object for uploaded databases
    table_name = None
    
    while True:
        user_input = input("\nYou: ")
        action, data = parse_input(user_input)
        
        if action == "upload":
            conn, table_name = upload_csv_to_mysql(data)
            if not conn:
                print("Failed to upload and process the CSV file.")
        elif action == "sample_queries":
            if conn and table_name:
                sample_queries = [
                    f"SELECT * FROM {table_name} LIMIT 10;",
                    f"SELECT COUNT(*) FROM {table_name};",
                    f"SELECT column_name FROM {table_name} WHERE column_name = 'example';"
                ]
                print("\nSample Queries:")
                for query in sample_queries:
                    print(query)
            else:
                print("No database uploaded yet. Please upload a CSV file first.")
        elif action in query_templates.keys():
            if conn and table_name:
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
                print("No database uploaded yet. Please upload a CSV file first.")
        else:
            print("Invalid command.")

# Run the chatbot
if __name__ == "__main__":
    chatbot()
