import pandas as pd
import mysql.connector
import re
import random
from prettytable import PrettyTable
import warnings
from cryptography.utils import CryptographyDeprecationWarning
import pdb

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
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
        # print("Connected to MySQL database.")
        return conn
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None
    
def helper():
    help_instructions = [
        "upload csv -> upload your data to the database, e.g., upload courses.csv",
        "list tables -> list the available tables in the database",
        "introduce tablename -> introduce a certain table in the database, e.g., introduce students",
        "sample -> general sample queries",
        "sample basic -> basic 'select from' sample queries",
        "sample distinct -> sample queries utilizing distinct",
        "sample where -> sample queries utilizing where",
        "sample aggregation -> sample queries utilizing aggregation",
        "sample group by -> sample queries utlizing group by",
        "sample order by -> sample queries utilizing order by",
        "sample join -> sample queries utilizing join",
        "ask questions utilizing basic queries, e.g., show all StudentID",
        "ask questions utilizing where queries, e.g., find firstname, lastname where studentid is 1",
        "ask questions utilizing aggregation queries, e.g., maximum of score",
        "ask questions utilizing group by queries, e.g., maximum score by studentId",
        "ask questions utilizing having queries, e.g., find studentId with average score larger than 90",
        "ask questions utilizing order by queries, e.g., show studentId, firstname ordered by firstname in ascending order",
        "ask questions utilizing join queries, e.g., find students: firstname, lastname and enrollment: courseId, score where students.studentId = enrollment.studentId",
    ]

    print("Here are the commands you can input:")
    for i in range(len(help_instructions)):
        instruction = help_instructions[i]
        print(f"\t{i+1}. {instruction}")

# Function to list all tables in the current database
def list_tables(conn):
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]
    return tables

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
        # print("Available tables in the mysql database:", tables)
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
    
def infer_sql_type(data):
    if pd.api.types.is_integer_dtype(data):
        return "INT"
    elif pd.api.types.is_float_dtype(data):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(data):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(data):
        return "DATETIME"
    else:
        return f"VARCHAR({255})"
    
# Upload CSV to MySQL and create a table
def upload_csv_to_mysql(file_path):
    conn = connect_to_mysql()
    if not conn:
        return None, None

    table_name = file_path.split("/")[-1].replace(".csv", "")
    df = pd.read_csv(file_path)

    cursor = conn.cursor()
    columns = []
    # Create table query
    for col in df.columns:
        sql_type = infer_sql_type(df[col])
        columns.append(f"`{col}` {sql_type}")
    create_table_query = f"""
    CREATE TABLE `{table_name}` (
        {', '.join(columns)}
    );
    """

    print(create_table_query)
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
# TODO: modify and -> ,
mysql_query_templates = {
    "basic": r"(list|show|display)\s+all\s+((?:\w+\s*,?\s*)+)",
    "where": r"(find|get|show)\s+((?:\w+\s*,?\s*)+)\s+where\s+(\w+)\s*(is|=|>=|<=|>|<)\s*(.+)",
    "aggregation": r"(count|sum|average|avg|minimum|min|maximum|max)\s+of\s+(\w+)",
    "group_by_aggregation": r"(sum|count|average|avg|minimum|min|maximum|max)\s+(\w+)\s+by\s+(\w+)",
    "group_by_having_1": r"(find|get|show)\s+(\w+)\s+with\s+(sum|average|count|minimum|min|maximum|max)\s+(\w+)\s+(?:greater|larger|bigger)\s+than\s+(\d+)",
    "group_by_having_2": r"(find|get|show)\s+(\w+)\s+with\s+(sum|average|count|minimum|min|maximum|max)\s+(\w+)\s+(?:less|smaller)\s+than\s+(\d+)",
    "group_by_having_3": r"(find|get|show)\s+(\w+)\s+with\s+(sum|average|count|minimum|min|maximum|max)\s+(\w+)\s+equal\s+to\s+(\d+)",
    "join": r"(find)\s+(\w+:\s+(?:\w+\s*(?:,\s*\w+\s*)*?))\s+and\s+(\w+:\s+(?:\w+\s*(?:,\s*\w+\s*)*?))\s+where\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)",
    "order_by": r"(list|show)\s+((?:\w+\s*,\s*)*\w+)\s+ordered\s+by\s+(\w+)\s+in\s+(ascending|descending)\s+order"
}

# Parse user input
def parse_input_mysql(user_input, conn):
    if user_input.startswith("upload "):
        match = re.match(r"upload (.+\.csv)", user_input)
        if match:
            return "upload", match.group(1)
    elif "sample" in user_input.lower():
        if "basic" in user_input.lower():
            return "sample", "basic"
        elif "distinct" in user_input.lower():
            return "sample", "distinct"
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
        elif user_input.lower() == "sample":
            return "sample", None
        return None, None

    elif user_input.lower() == "list tables":
        tables = list_tables(conn)
        if tables:
            return "list_tables", tables
        return "error", "No tables found in the current database."

    elif user_input.lower().startswith("introduce "):
        match = re.match(r"introduce\s+(\w+)", user_input, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            metadata = get_table_metadata(conn, table_name)
            if metadata:
                table = PrettyTable(["ColumnName", "DataType"])
                metadata = [x[:2] for x in metadata]
                table.add_rows(metadata)
                print(f"Table {table_name} info:")
                print(table)
                column_names, rows = get_sample_data(conn, table_name)
                table = PrettyTable(column_names)
                table.add_rows(rows)
                print("\nSample Data:")
                print(table)
                return "introduce", None
            return "error", f"Table '{table_name}' does not exist or cannot be described."
    else:   # NLP
        for query_type, pattern in mysql_query_templates.items():
            # print(query_type)
            if query_type == "where" and "where" not in user_input:
                continue
            if re.search(pattern, user_input, re.IGNORECASE):
                return query_type, user_input
            
    return None, None

# Fetch columns for a specific table
def get_table_columns(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table_name};")
    columns = cursor.fetchall()
    column_types = {column[0]: column[1].lower() for column in columns}
    return [column[0] for column in columns], column_types
def get_sample_rows(conn, table_name, limit=10):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
    return cursor.fetchall(), [desc[0] for desc in cursor.description]

def is_numeric_column(column_name, column_types):
    data_type = column_types.get(column_name, "").lower()
    return any(keyword in data_type for keyword in ["int", "float", "double", "decimal"])

# Define meaningful join relationships -> hand crafted
JOIN_RELATIONSHIPS = {
    "advisors": {
        "advisorId": [("students", "advisorId"), ("instruct", "instructorId")]
    },
    "students": {
        "studentId": [("enrollment", "studentId")],
        "advisorId": [("advisors", "advisorId")]
    },
    "courses": {
        "courseId": [("enrollment", "courseId"), ("instruct", "courseId")]
    },
    "enrollment": {
        "studentId": [("students", "studentId")],
        "courseId": [("courses", "courseId")]
    },
    "instruct": {
        "courseId": [("courses", "courseId")],
        "instructorId": [("advisors", "advisorId")]
    }
}

def generate_sample_queries(conn, query_type=None, num_queries=5):
    tables = get_all_tables(conn)
    if not tables:
        return []

    queries = []
    query_types = [
        "basic", "where", "aggregation", "group by", "order by", "join", "distinct"
    ] if query_type is None else [query_type]

    while len(queries) < num_queries:
        query = None
        table = random.choice(tables)
        columns, data_types = get_table_columns(conn, table)
        if not columns:
            continue

        selected_query_type = random.choice(query_types)
        
        if selected_query_type == "basic":
            # Basic query
            subset_size = random.randint(1, len(columns))
            selected_cols = random.sample(columns, subset_size)
            cols = ", ".join(selected_cols)
            query = f"SELECT {cols} FROM {table} LIMIT 10;"
            
        elif selected_query_type == "distinct":
            # Distinct query
            col = random.choice(columns)
            query = f"SELECT DISTINCT {col} FROM {table} LIMIT 10;"
        
        elif selected_query_type == "where":
            # Query with WHERE clause
            col = random.choice(columns)

            sample_rows, column_names = get_sample_rows(conn, table, limit=10)
            if sample_rows:
                random_row = random.choice(sample_rows)
                condition_value = random_row[column_names.index(col)] if col in column_names else None
                if not is_numeric_column(col,data_types):
                    condition_value = f"'{condition_value}'"
                # check condition_value type and modify the query
                if condition_value is not None:
                    query = f"SELECT * FROM {table} WHERE {col} = {condition_value} LIMIT 10;"
                else:
                    query = f"SELECT * FROM {table} WHERE {col} IS NOT NULL LIMIT 10;"
            else:
                query = f"SELECT * FROM {table} WHERE {col} IS NOT NULL LIMIT 10;"
        
        elif selected_query_type == "aggregation":
            # Aggregation query
            # except count, all others should be numerical
            random_index = random.randint(0, len(columns) - 1)
            col = columns[random_index]
            agg_func = "COUNT"
            if is_numeric_column(col,data_types):
                agg_func = random.choice(["SUM", "COUNT", "AVG", "MAX", "MIN"])
            query = f"SELECT {agg_func}({col}) FROM {table};"

        elif selected_query_type == "group by":
            # Group by query
            # check the type of col2, if numerical we can choose sum, avg, max, min
            if len(columns) > 1:
                col1, col2 = random.sample(columns, 2)
                agg_func = "COUNT"
                if is_numeric_column(col2,data_types):
                    agg_func = random.choice(["SUM", "COUNT", "AVG", "MAX", "MIN"])
                query = f"SELECT {col1}, {agg_func}({col2}) FROM {table} GROUP BY {col1} LIMIT 10;"
            else:
                query = f"SELECT * FROM {table} LIMIT 10;"

        elif selected_query_type == "order by":
            # Order by query
            col = random.choice(columns)
            order = random.choice(["ASC", "DESC"])
            query = f"SELECT * FROM {table} ORDER BY {col} {order} LIMIT 10;"

        elif selected_query_type == "join":
            # Join query (requires at least 2 tables)
            # if len(tables) > 1:
            #     #table2 = random.choice([t for t in tables])
            #     table2 = random.choice([t for t in tables if t != table])
            #     columns2, types = get_table_columns(conn, table2)
            #     if columns2:
            #         col1 = random.choice(columns)
            #         col2 = random.choice(columns2)
            #         query = f"SELECT t1.{col1}, t2.{col2} FROM {table} t1 JOIN {table2} t2 ON t1.{col1} = t2.{col2} LIMIT 10;"
            #     else:
            #         query = f"SELECT * FROM {table} LIMIT 10;"
            # else:
            #     query = f"SELECT * FROM {table} LIMIT 10;"
            # Directly select two tables with meaningful relationships
            joinable_tables = []
            for table1, relationships in JOIN_RELATIONSHIPS.items():
                if table1 in tables:
                    for col1, related in relationships.items():
                        for table2, col2 in related:
                            if table2 in tables:
                                joinable_tables.append((table1, col1, table2, col2))
            
            if not joinable_tables:
                # If no meaningful join relationships, skip this iteration
                continue

            # Choose a meaningful join pair
            table1, col1, table2, col2 = random.choice(joinable_tables)
            columns1, _ = get_table_columns(conn, table1)
            columns2, _ = get_table_columns(conn, table2)

            # Randomly select additional columns for the query
            additional_cols1 = random.sample(columns1, random.randint(1, min(2, len(columns1))))
            additional_cols2 = random.sample(columns2, random.randint(1, min(2, len(columns2))))
            selected_cols1 = ", ".join([f"t1.{col}" for col in additional_cols1])
            selected_cols2 = ", ".join([f"t2.{col}" for col in additional_cols2])

            # Generate the join query
            query = (
                f"SELECT {selected_cols1}, {selected_cols2} "
                f"FROM {table1} t1 "
                f"JOIN {table2} t2 "
                f"ON t1.{col1} = t2.{col2} "
                f"LIMIT 10;"
            )

        if query is not None and query not in queries:
            queries.append(query)

    return queries

# Process queries based on type
def process_query(query_type, user_input, table_name):
    match = None
    if query_type == "basic":
        match = re.search(mysql_query_templates["basic"], user_input, re.IGNORECASE)
        column = match.group(2)
        sql = f"SELECT {column} FROM {table_name};"
    elif query_type == "where":
        match = re.search(mysql_query_templates["where"], user_input, re.IGNORECASE)
        column, condition_column, operator, value = match.group(2), match.group(3), match.group(4), match.group(5)
        operator = "=" if operator == "is" else operator
        if value.isdigit():
            sql = f"SELECT {column} FROM {table_name} WHERE {condition_column} {operator} {int(value)};"
        elif value.replace('.', '').isdigit():
            sql = f"SELECT {column} FROM {table_name} WHERE {condition_column} {operator} {float(value)};"
        else:
            sql = f"SELECT {column} FROM {table_name} WHERE {condition_column} {operator} '{value}';"
    elif query_type == "aggregation":
        match = re.search(mysql_query_templates["aggregation"], user_input, re.IGNORECASE)
        func, column = match.group(1), match.group(2)
        if func == "average":
            func = "avg"
        elif func == "minimum":
            func = "min"
        elif func == "maximum":
            func = "max"
        agg_name = f"{func}_{column}"
        sql = f"SELECT {func.upper()}({column}) AS {agg_name} FROM {table_name};"
    elif query_type == "group_by_aggregation":
        match = re.search(mysql_query_templates["group_by_aggregation"], user_input, re.IGNORECASE)
        func, column, group_by_column = match.group(1), match.group(2), match.group(3)
        if func == "average":
            func = "avg"
        elif func == "minimum":
            func = "min"
        elif func == "maximum":
            func = "max"
        agg_name = f"{func}_{column}"
        sql = f"SELECT {group_by_column}, {func.upper()}({column}) AS {agg_name} FROM {table_name} GROUP BY {group_by_column};"
    elif "group_by_having" in query_type:
        match = re.search(mysql_query_templates[query_type], user_input, re.IGNORECASE)
        group_by_column, func, column, value = match.group(2), match.group(3), match.group(4), match.group(5)
        if func == "average":
            func = "avg"
        elif func == "minimum":
            func = "min"
        elif func == "maximum":
            func = "max"
        agg_name = f"{func}_{column}"
        sql = f"""
            SELECT {group_by_column}, {func.upper()}({column}) AS {agg_name}
            FROM {table_name} 
            GROUP BY {group_by_column} 
            HAVING {agg_name} {'>' if '1' in query_type else ('<' if '2' in query_type else '=')} {value};
        """
    elif query_type == "join":
        match = re.search(mysql_query_templates["join"], user_input, re.IGNORECASE)
        input1, input2, condition1, condition2 = match.group(2), match.group(3), match.group(4), match.group(5)
        table1, columns1 = input1.split(":")
        table2, columns2 = input2.split(":")
        cols1 = columns1.strip().split(', ')
        cols2 = columns2.strip().split(', ')
        t1_cols1 = f"{table1}.{cols1[0]}"
        for i in range(1, len(cols1)):
            t1_cols1 += f", {table1}.{cols1[i]}"
        t2_cols2 = f"{table2}.{cols2[0]}"
        for i in range(1, len(cols2)):
            t2_cols2 += f", {table2}.{cols2[i]}"
        sql = f"""
            SELECT {t1_cols1}, {t2_cols2}
            FROM {table1}
            JOIN {table2} ON {condition1} = {condition2}
        """
    elif query_type == "order_by":
        match = re.search(mysql_query_templates["order_by"], user_input, re.IGNORECASE)
        column, order_column, order = match.group(2), match.group(3), match.group(4) or "ASC"
        if order == "ascending":
            order = "ASC"
        elif order == "descending":
            order = "DESC"
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

def chat_mysql(user_input, conn):
    if user_input.lower() == "help":
        helper()
        return conn
    tables = get_all_tables(conn)
    action, data = parse_input_mysql(user_input, conn)
    if action == "upload":
        conn, table_name = upload_csv_to_mysql(data)
        if not conn:
            print("Failed to upload and process the CSV file.")
        else:
            tables = get_all_tables(conn)  # Update table list
    elif action == "list_tables":
        print("Available Tables:")
        print(data)
        # print("\n".join(data))
    elif action == "introduce":
        # print(data)
        pass
    elif action == "error":
        print(f"Error: {data}")
    elif action ==  "sample":
        sample_queires = generate_sample_queries(conn, query_type=data)
        if len(sample_queires) == 0:
            print("No sample queries generated from your instruction, please try again or use the help command to see instructions")
        else:
            print(f"Here are {len(sample_queires)} sample queries:")
            print("\n".join(sample_queires))
            execute_sample = input(f"\nDo you want to execute these samples?\nPlease enter the index of the sample (1-{len(sample_queires)}) to execute and see result. Enter anything else to ask other questions:")

            while execute_sample in [str(x) for x in range(1, len(sample_queires) + 1)]:
                execute_sample = int(execute_sample)
                sql = sample_queires[execute_sample-1]
                results=execute_query(conn, sql)
                print("\nSelected Query:")
                print(sql)
                columns = sql.split("SELECT ")[1].split(" FROM")[0]
                if columns == "*":
                    table_name = sql.split("FROM ")[1].split(" ")[0]
                    meta_data = get_table_metadata(conn, table_name)
                    columns = [col[0] for col in meta_data]
                else:
                    columns = columns.split(", ")
                columns = [x.strip() for x in columns]
                table = PrettyTable(columns)
                table.add_rows(results)
                print("\nQuery Results:")
                print(table)
                execute_sample = input(f"\nDo you want to execute these samples?\nPlease enter the index of the sample (1-{len(sample_queires)}) to execute and see result. Enter anything else to ask other questions:")
        
    elif action in mysql_query_templates.keys():
        if tables:
            if action == "join":
                table_name = tables[0]  # the tables of join is specified in the input itself, so we don't need to do anything
                sql_query = process_query(action, data, table_name)
                if sql_query:
                    print(f"\nGenerated Query:\n\t{sql_query}")
                    results = execute_query(conn, sql_query)
                    columns = sql_query.split("SELECT ")[1].split(" FROM")[0].split(", ")
                    columns = [x.strip() for x in columns]
                    table = PrettyTable(columns)
                    table.add_rows(results)
                    print("\nQuery Results:")
                    print(table)
                else:
                    print("Could not generate a valid SQL query. Please try again.")
            else:
                table_name = input(f"Please specify the table name you would like to query on among {tables}: " )
                sql_query = process_query(action, data, table_name)
                if sql_query:
                    print(f"Generated Query:\n\t{sql_query}")
                    results = execute_query(conn, sql_query)
                    if results is not None:
                        columns = sql_query.split("SELECT ")[1].split(" FROM")[0].split(", ")
                        columns = [x if ") AS" not in x else x.split("AS ")[1] for x in columns]
                        columns = [x.strip() for x in columns]
                        table = PrettyTable(columns)
                        table.add_rows(results)
                        print("\nQuery Results:")
                        print(table)
                    else:
                        print("An error occurred, please check your input.")
                else:
                    print("Could not generate a valid SQL query. Please try again.")

        else:
            print("No tables found. Please upload a CSV file or check your database.")
    else:
        print("Invalid command.")

    return conn