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
DATABASE_NAME = "DSCI551"

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
                data = json.load(file) 
                if isinstance(data, list):
                    collection.insert_many(data) 
                else:
                    collection.insert_one(data)
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
    sample_docs = collection.find().limit(3)
    count = collection.count_documents({})
    if sample_doc:
        return list(sample_doc.keys()), sample_docs, count
    return [], None, 0


def generate_sample_queries_for_mongodb(client, database_name, query_type=None,num_queries=5):
    db = client[database_name]
    collections = db.list_collection_names()
    if not collections:
        print("No collections found in the database.")
        return None
    
    queries = []
    query_types = [
        "find", "projection", "grouping", "lookup", "unwind", "sort", "count"
    ] if query_type is None else [query_type]

    while len(queries) < num_queries:
        query = None
        collection = random.choice(collections)
        sample_doc = db[collection].find_one()

        selected_query_type = random.choice(query_types)

        if selected_query_type == "find":
            valid_fields = {
                k: v for k, v in sample_doc.items() if isinstance(v, (int, float, str))
            }
            if not valid_fields:
                continue
            field = random.choice(list(valid_fields.keys()))
            value = valid_fields[field]
            if isinstance(value, (int, float)): # numeric value
                operator = random.choice(["=", "<", ">", "<=", ">="])
                if operator == "=":
                    query = f"db.{collection}.find({{'{field}': {value}}})"
                elif operator == "<":
                    query = f"db.{collection}.find({{'{field}': {{'$lt': {value}}}}})"
                elif operator == ">":
                    query = f"db.{collection}.find({{'{field}': {{'$gt': {value}}}}})"
                elif operator == "<=":
                    query = f"db.{collection}.find({{'{field}': {{'$lte': {value}}}}})"
                elif operator == ">=":
                    query = f"db.{collection}.find({{'{field}': {{'$gte': {value}}}}})"
            else:
                query = f"db.{collection}.find({{'{field}': '{value}'}})"

        elif selected_query_type == "projection":
            fields = random.sample(list(sample_doc.keys()), min(len(sample_doc), 2))
            projection = ", ".join([f"{field!r}: 1" for field in fields])
            if "_id" not in fields:
                projection += ", '_id': 0"
            query = f"db.{collection}.find({{}}, {{{projection}}})"

        elif selected_query_type == "grouping":
            # Generate a grouping query
            numeric_fields = [k for k, v in sample_doc.items() if isinstance(v, (int, float))]
            all_fields = list(sample_doc.keys())
            
            if numeric_fields and len(all_fields) > 1:
                # Ensure grouping field and calculation field are different
                group_by_field = random.choice(all_fields)
                while group_by_field in numeric_fields:
                    group_by_field = random.choice(all_fields)  # Avoid using the numeric field for grouping
                
                aggregation_field = random.choice(numeric_fields)
                aggregation_function = random.choice(["$avg", "$sum", "$min", "$max", "$stdDevPop", "$stdDevSamp"])
                agg_function = aggregation_function.replace("$", "")
                
                query = (
                    f"db.{collection}.aggregate(["
                    f"{{'$group': {{'_id': '${group_by_field}', "
                    f"'{aggregation_field}_{agg_function}': {{'{aggregation_function}': '${aggregation_field}'}}}}}}"
                    f"])"
                )
            else:
                # Default grouping by any field if no numeric field is found
                group_by_field = random.choice(all_fields)
                query = (
                    f"db.{collection}.aggregate(["
                    f"{{'$group': {{'_id': '${group_by_field}', 'count': {{'$sum': 1}}}}}}"
                    f"])"
                )

        elif selected_query_type == "lookup":
            # Define meaningful relationships between collections
            relationships = {
                "users": [{"from": "reviews", "localField": "_id", "foreignField": "userId"},
                        {"from": "orders", "localField": "_id", "foreignField": "userId"}],
                "reviews": [{"from": "users", "localField": "userId", "foreignField": "_id"},
                            {"from": "products", "localField": "productId", "foreignField": "_id"}],
                "orders": [{"from": "users", "localField": "userId", "foreignField": "_id"}],
                "products": [{"from": "reviews", "localField": "_id", "foreignField": "productId"},
                            {"from": "categories", "localField": "category", "foreignField": "_id"}],
                "categories": [{"from": "products", "localField": "_id", "foreignField": "category"}]
            }

            # Get possible lookups for the current collection
            if collection in relationships:
                possible_lookups = relationships[collection]
                chosen_lookup = random.choice(possible_lookups)
                query = (
                    f"db.{collection}.aggregate(["
                    f"{{'$lookup': {{'from': '{chosen_lookup['from']}', "
                    f"'localField': '{chosen_lookup['localField']}', "
                    f"'foreignField': '{chosen_lookup['foreignField']}', "
                    f"'as': 'joined_data'}}}}"
                    f"])"
                )
            else:
                # Fallback to a generic random lookup if no defined relationship
                other_collection = random.choice([col for col in collections if col != collection])
                query = (
                    f"db.{collection}.aggregate(["
                    f"{{'$lookup': {{'from': '{other_collection}', "
                    f"'localField': '_id', 'foreignField': '_id', 'as': 'joined_data'}}}}"
                    f"])"
                )

        elif selected_query_type == "unwind":
            # Generate an unwind query
            relationships = {
                "users": [{"from": "reviews", "localField": "_id", "foreignField": "userId"},
                        {"from": "orders", "localField": "_id", "foreignField": "userId"}],
                "reviews": [{"from": "users", "localField": "userId", "foreignField": "_id"},
                            {"from": "products", "localField": "productId", "foreignField": "_id"}],
                "orders": [{"from": "users", "localField": "userId", "foreignField": "_id"}],
                "products": [{"from": "reviews", "localField": "_id", "foreignField": "productId"},
                            {"from": "categories", "localField": "category", "foreignField": "_id"}],
                "categories": [{"from": "products", "localField": "_id", "foreignField": "category"}]
            }
            # Get possible lookups for the current collection
            if collection in relationships:
                possible_lookups = relationships[collection]
                chosen_lookup = random.choice(possible_lookups)
                array_field = "joined_data"
                query = (
                    f"db.{collection}.aggregate(["
                    f"{{'$lookup': {{'from': '{chosen_lookup['from']}', "
                    f"'localField': '{chosen_lookup['localField']}', "
                    f"'foreignField': '{chosen_lookup['foreignField']}', "
                    f"'as': 'joined_data'}}}}, "
                    f"{{'$unwind': '${array_field}'}}"
                    f"])"
                )

        elif selected_query_type == "sort":
            # Generate a sort query
            field = random.choice(list(sample_doc.keys()))
            order = random.choice([1, -1])  # Ascending or Descending
            query = f"db.{collection}.aggregate([{{'$sort': {{'{field}': {order}}}}}])"

        elif selected_query_type == "count":
            # Generate a count query with filtering
            possible_fields = list(sample_doc.keys())
            field = random.choice(possible_fields)
            field_value = sample_doc[field]

            # Check the field type and generate a condition
            if isinstance(field_value, (int, float)):  # Numeric field
                operator = random.choice(["$gt", "$lt", "$gte", "$lte", "$eq"])
                value = random.randint(1, 100)  # Generate a random numeric value
                query = f"db.{collection}.count_documents({{'{field}': {{{operator}: {value}}}}})"
            elif isinstance(field_value, str):  # String field
                query = f"db.{collection}.count_documents({{'{field}': '{field_value}'}})"
            else:
                # Fallback to count all if no suitable field found
                query = f"db.{collection}.count_documents({{}})"

        if query is not None and query not in queries:
            queries.append(query)

    return queries

def parse_input_mongodb(user_input, client):
    if user_input.startswith("upload "):
        match = re.match(r"upload (.+\.json)", user_input)
        if match:
            return "upload", match.group(1)
    elif "sample" in user_input.lower():
        if "find" in user_input.lower():
            return "sample", "find"
        elif "projection" in user_input.lower():
            return "sample", "projection"
        elif "grouping" in user_input.lower():
            return "sample", "grouping"
        elif "lookup" in user_input.lower():
            return "sample", "lookup" 
        elif "unwind" in user_input.lower():
            return "sample", "unwind" 
        elif "sort" in user_input.lower():
            return "sample", "sort"
        elif "count" in user_input.lower():
            return "sample","count"
        elif user_input.lower() == "sample":
            return "sample", None
        return None, None

    elif user_input.lower() == "list collections":
        collections = list_collections(client,DATABASE_NAME)
        if collections:
            return "list_collections", collections
        return "error", "No collections found in the current database."

    elif user_input.lower().startswith("introduce "):
        match = re.match(r"introduce (\w+)", user_input, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            metadata = get_collection_metadata(client, DATABASE_NAME,table_name)

            return "introduce", metadata
    else:   # NLP
        for query_type, pattern in mongodb_query_templates.items():
            # print(query_type)
            if query_type == "where" and "where" not in user_input:
                continue
            if re.search(pattern, user_input, re.IGNORECASE):
                return query_type, user_input
            
    return None, None

mongodb_query_templates = {
    "find": r"(find|get|show)\s+all\s+documents\s+where\s+(\w+)\s*(is|=|>=|<=|>|<)\s*(.+)",
    "projection": r"(find|get|show)\s+([\w,\s]+)\s+fields",
    "grouping": r"(sum|average|avg|minimum|min|maximum|max)\s+(\w+)\s+by\s+(\w+)",
    "sort": r"sort\s+documents\s+by\s+(\w+)\s+in\s+(ascending|descending)\s+order",
    "count": r"count\s+documents\s+with\s+(\w+)\s*(is|=|>=|<=|>|<)\s*(.+)",
    "lookup": r"use\s+(\w+)\s+to\s+(lookup|find|search)\s+(\w+)\s+(on|where|with)\s+(\w+)\s+(matches|aligns|equals|=)\s+(\w+)"
}

def process_query(query_type, user_input, collection):
    match = None
    if query_type == "find":
        match = re.search(mongodb_query_templates["find"], user_input, re.IGNORECASE)
        field, operator, value = match.group(2), match.group(3), match.group(4)
        if value.isdigit():
            value = int(value)
        elif value.replace(".", "").isdigit():
            value = float(value)
        if isinstance(value, (int, float)):
            if operator == "=":
                query = f"db.{collection}.find({{'{field}': {value}}})"
            elif operator == "<":
                query = f"db.{collection}.find({{'{field}': {{'$lt': {value}}}}})"
            elif operator == ">":
                query = f"db.{collection}.find({{'{field}': {{'$gt': {value}}}}})"
            elif operator == "<=":
                query = f"db.{collection}.find({{'{field}': {{'$lte': {value}}}}})"
            elif operator == ">=":
                query = f"db.{collection}.find({{'{field}': {{'$gte': {value}}}}})"
        else:
            if operator == "=":
                query = f"db.{collection}.find({{'{field}': '{value}'}})"
            elif operator == "<":
                query = f"db.{collection}.find({{'{field}': {{'$lt': '{value}'}}}})"
            elif operator == ">":
                query = f"db.{collection}.find({{'{field}': {{'$gt': '{value}'}}}})"
            elif operator == "<=":
                query = f"db.{collection}.find({{'{field}': {{'$lte': '{value}'}}}})"
            elif operator == ">=":
                query = f"db.{collection}.find({{'{field}': {{'$gte': '{value}'}}}})"
    
    elif query_type == "projection":
        match = re.search(mongodb_query_templates["projection"], user_input, re.IGNORECASE)
        project_fields= match.group(2)
        projection = ", ".join(f"{x!r}: 1" for x in project_fields.split(", "))
        projection += ", '_id': 0"
        query = f"db.{collection}.find({{}}, {{{projection}}})"

    elif query_type == "grouping":
        match = re.search(mongodb_query_templates["grouping"], user_input, re.IGNORECASE)
        func, field, group_by_field = match.group(1), match.group(2), match.group(3)
        if func == "average" or func == "avg":
            func = "$avg"
        elif func == "minimize" or func == "min":
            func = "$min"
        elif func == "maximize" or func == "max":
            func = "$max"
        elif func == "sum":
            func = "$sum"
        agg_func = func.replace("$", "")
        query = (
            f"db.{collection}.aggregate(["
            f"{{'$group': {{'_id': '${group_by_field}', "
            f"'{field}_{agg_func}': {{'{func}': '${field}'}}}}}}"
            f"])"
        )

    elif query_type == "sort":
        match = re.search(mongodb_query_templates["sort"], user_input, re.IGNORECASE)
        field, order = match.group(1), match.group(2)
        if order == "ascending":
            order = 1
        else:
            order = -1
        query = f"db.{collection}.aggregate([{{'$sort': {{'{field}': {order}}}}}])"

    elif query_type == "count":
        match = re.search(mongodb_query_templates["count"], user_input, re.IGNORECASE)
        field, operator, value = match.group(1), match.group(2), match.group(3)
        if value.isdigit():
            value = int(value)
        elif value.replace(".", "").isdigit():
            value = float(value)
        if isinstance(value, (int, float)):
            if operator == "=":
                query = f"db.{collection}.count_documents({{'{field}': {value}}})"
            elif operator == "<":
                query = f"db.{collection}.count_documents({{'{field}': {{'$lt': {value}}}}})"
            elif operator == ">":
                query = f"db.{collection}.count_documents({{'{field}': {{'$gt': {value}}}}})"
            elif operator == "<=":
                query = f"db.{collection}.count_documents({{'{field}': {{'$lte': {value}}}}})"
            elif operator == ">=":
                query = f"db.{collection}.count_documents({{'{field}': {{'$gte': {value}}}}})"
        else:
            if operator == "=":
                query = f"db.{collection}.count_documents({{'{field}': '{value}'}})"
            elif operator == "<":
                query = f"db.{collection}.count_documents({{'{field}': {{'$lt': '{value}'}}}})"
            elif operator == ">":
                query = f"db.{collection}.count_documents({{'{field}': {{'$gt': '{value}'}}}})"
            elif operator == "<=":
                query = f"db.{collection}.count_documents({{'{field}': {{'$lte': '{value}'}}}})"
            elif operator == ">=":
                query = f"db.{collection}.count_documents({{'{field}': {{'$gte': '{value}'}}}})"

    elif query_type == "lookup":
        match = re.search(mongodb_query_templates["lookup"], user_input, re.IGNORECASE)
        collection, from_collection, local_field, from_field = match.group(1), match.group(3), match.group(5), match.group(7)
        query = (
            f"db.{collection}.aggregate(["
            f"{{'$lookup': {{'from': '{from_collection}', "
            f"'localField': '{local_field}', "
            f"'foreignField': '{from_field}', "
            f"'as': 'joined_data'}}}}"
            f"])"
        )        

    else:
        query = None

    return query

def helper():
    help_instructions = [
        "upload json -> upload your data to the database, e.g., upload input.json",
        "list collections -> list the available collections in the database",
        "introduce collection -> introduce a certain table in the database, e.g., introduce orders",
        "sample -> general sample queries",
        "sample find -> sample queries utilizing find",
        "sample projection -> sample queries utilizing projection",
        "sample grouping -> sample queries utilizing grouping",
        "sample lookup -> sample queries utlizing lookup",
        "sample unwind -> sample queries utilizing unwind",
        "sample sort -> sample queries utilizing sort",
        "sample count -> sample queries utilizing count",
        "ask questions utilizing find queries, e.g., show all documents where price >= 99.1",
        "ask questions utilizing projection queries, e.g., get rating, review fields",
        "ask questions utilizing grouping queries, e.g., sum stock by category",
        "ask questions utilizing sort queries, e.g., sort documents by price in descending order",
        "ask questions utilizing count queries, e.g., count documents with rating <= 3",
        "ask questions utilizing lookup queries, e.g., use users to search reviews on _id = userId",
    ]

    print("Here are the commands you can input:")
    for i in range(len(help_instructions)):
        instruction = help_instructions[i]
        print(f"\t{i+1}. {instruction}")

def process_data(data):
    from bson import ObjectId
    from datetime import datetime
    """Convert a dictionary with non-serializable types into a JSON-serializable format."""
    if isinstance(data, dict):
        return {key: process_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [process_data(item) for item in data]
    elif isinstance(data, ObjectId):
        return str(data)  # Convert ObjectId to string
    elif isinstance(data, datetime):
        return data.isoformat()  # Convert datetime to ISO 8601 string
    else:
        return data

def chat_mongodb(user_input, client):
    db = client[DATABASE_NAME]
    if user_input.lower() == "help":
        helper()
        return
    collections = list_collections(client,DATABASE_NAME)
    action, data = parse_input_mongodb(user_input, client)
    if action == "upload":
        upload_data_mongodb(client,DATABASE_NAME,data)
        print(list_collections(client,DATABASE_NAME))
    elif action == "list_collections":
        print("Available Collections:")
        print(data)
    elif action == "introduce":
        print(f"This collection contains {data[2]} documents\n")
        print("Since MongoDB is a schema-less Nosql database, here are the field names of the first document:\n")
        print(data[0])
        print("\nHere are the first three documents in the collection:\n")
        for doc in data[1]:
            print(json.dumps(doc, indent=4, default=str))
        
    elif action ==  "sample":
        sample_queires = generate_sample_queries_for_mongodb(client=client,query_type=data, database_name=DATABASE_NAME)
        if len(sample_queires) == 0:
            print("No sample queries generated from your instruction, please try again or use the help command to see instructions")
        else:
            print(f"Here are {len(sample_queires)} sample queries:")
            print("\n".join(sample_queires))
            execute_sample = input(f"\nDo you want to execute these samples?\nPlease enter the index of the sample (1-{len(sample_queires)}) to execute and see result. Enter anything else to ask other questions:")

            while execute_sample in [str(x) for x in range(1, len(sample_queires) + 1)]:
                execute_sample = int(execute_sample)
                query = sample_queires[execute_sample-1]
                results = eval(query)
                if not isinstance(results, int):
                    results = list(results)
                results = process_data(results)
                print("\nSelected Query:")
                print(query)
                print("\nQuery Results:")
                print(json.dumps(results, indent=4))
                execute_sample = input(f"\nDo you want to execute these samples?\nPlease enter the index of the sample (1-{len(sample_queires)}) to execute and see result. Enter anything else to ask other questions:")

    elif action in mongodb_query_templates.keys():
        if collections:
            collection_name = None
            if action != "lookup":
                collection_name = input(f"Please specify the table name you would like to query on among {collections}: " )
            mongodb_query = process_query(action, data, collection_name)
            if mongodb_query:
                print(f"\nGenerated Query:\n\t{mongodb_query}")
                results = eval(mongodb_query)
                if not isinstance(results, int):
                    results = list(results)
                results = process_data(results)
                print("\nQuery Results:")
                print(json.dumps(results, indent=4))
            else:
                print("Could not generate a valid MongoDB query. Please try again.")
        else:
            print("No collections found. Please upload a JSON file or check your database.")
    
    else:
        print("Invalid command.")