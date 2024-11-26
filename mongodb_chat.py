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
                pass

        elif selected_query_type == "projection":
            pass

        elif selected_query_type == "grouping":
            pass

        elif selected_query_type == "lookup":
            pass

        elif selected_query_type == "unwind":
            pass

        elif selected_query_type == "sort":
            pass

        elif selected_query_type == "count":
            pass

        if query is not None:
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
    # else:   # NLP
    #     for query_type, pattern in mysql_query_templates.items():
    #         # print(query_type)
    #         if query_type == "where" and "where" not in user_input:
    #             continue
    #         if re.search(pattern, user_input, re.IGNORECASE):
    #             return query_type, user_input
            
    return None, None

def helper():
    print("")

def chat_mongodb(user_input, client):
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
        sample_queires = (generate_sample_queries_for_mongodb(client=client,query_type=data,database_name=DATABASE_NAME))
        print("\n".join(sample_queires))