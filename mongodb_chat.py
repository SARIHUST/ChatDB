import pandas as pd
import os
import re
import random
from pymongo import MongoClient
import json
import warnings
from cryptography.utils import CryptographyDeprecationWarning
import pdb

ATLAS_URI = "mongodb+srv://xyxy:DSCI551@cluster48297.lfyhked.mongodb.net/?retryWrites=true&w=majority&appName=Cluster48297"

def connect_to_mongodb():
    try:
        client = MongoClient(ATLAS_URI)
        # print("Connected to MongoDB Atlas.")
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None
def upload_data_mongodb(client, database_name, file_path):
    collection_name = os.path.splitext(os.path.basename(file_path))[0]
    db = client[database_name]
    collection = db[collection_name]
    try:
        if file_path.endswith(".json"):
            with open(file_path, "r") as file:
                for line in file:
                    line = line.strip()  
                    if not line:  
                        continue
                    try:
                        document = json.loads(line)  
                        collection.insert_one(document)  
                    except json.JSONDecodeError as e:
                        print(f"Invalid JSON object in line: {line}")
                        print(f"Error: {e}")
        elif file_path.endswith(".csv"):
            data = pd.read_csv(file_path)
            collection.insert_many(data.to_dict("records"))
        print(f"Data uploaded to {database_name}.{collection_name} successfully!")
    except Exception as e:
        print(f"Error uploading data: {e}")

def list_collections(client, database_name):
    db = client[database_name]
    return db.list_collection_names()

def get_collection_metadata(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]
    sample_doc = collection.find_one()
    if sample_doc:
        return list(sample_doc.keys()), sample_doc
    return [], None


def generate_sample_queries_for_mongodb(client, database_name, query_type=None,num_queries = 5):
    db = client[database_name]
    collections = db.list_collection_names()
    if not collections:
        print("No collections found in the database.")
        return None
    queries = []
    for _ in range(num_queries):
        if query_type == "basic":
            collection_name = random.choice(collections)
            collection = db[collection_name]
            sample_doc = collection.find_one()
            if sample_doc:
                random_field = random.choice(list(sample_doc.keys()))
                queries.append({"find": collection_name, "projection": {random_field: 1}})

        elif query_type == "where":
            collection_name = random.choice(collections)
            collection = db[collection_name]
            sample_doc = collection.find_one()
            if sample_doc:
                random_field = random.choice(list(sample_doc.keys()))
                random_value = sample_doc[random_field]
                queries.append({"find": collection_name, "filter": {random_field: random_value}})

        elif query_type == "aggregation":
            collection_name = random.choice(collections)
            collection = db[collection_name]
            sample_doc = collection.find_one()
            if sample_doc:
                random_field = random.choice(list(sample_doc.keys()))
                queries.append({
                    "aggregate": collection_name,
                    "pipeline": [{"$group": {"_id": f"${random_field}", "count": {"$sum": 1}}}]
                })

        else:

            collection_name = random.choice(collections)
            queries.append({"find": collection_name, "limit": 10})

    return queries

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
                    result += ", ".join(column_names) + "\n"
                    for row in rows:
                        result += ", ".join(map(str, row)) + "\n"
                return "introduce", result
            return "error", f"Table '{table_name}' does not exist or cannot be described."
    else:   # NLP
        for query_type, pattern in mysql_query_templates.items():
            # print(query_type)
            if query_type == "where" and "where" not in user_input:
                continue
            if re.search(pattern, user_input, re.IGNORECASE):
                return query_type, user_input
            
    return None, None

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

        if query is not None:
            queries.append(query)

    return queries

def chat_mongodb(user_input, client):
    pass
    action, data = parse_input_mysql(user_input, client)
    if action ==  "sample":

        sample_queires = (generate_sample_queries_for_mongodb(client=client,query_type=data,database_name="DSCI551"))
        print("\n".join(sample_queires))