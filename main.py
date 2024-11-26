from mysql_chat import chat_mysql, connect_to_mysql
from mongodb_chat import chat_mongodb, connect_to_mongodb

# Main chatbot loop
def chatbot():
    global current_database
    print("Hello! I'm your database assistant. You can upload CSV/JSON files, ask for sample queries, or ask natural language questions about your data.")
    print("By default, you are asking questions about mysql. You can enter 'mongodb' to switch to mongodb and 'mysql' to switch back. Type 'help' to see the available commands.")
    current_database = "mysql"
    conn = connect_to_mysql()
    client = connect_to_mongodb()
    if not conn:
        print("Failed to connect to the MySQL database.")
        return
    
    while True:
        user_input = input("\nYou: ")
        if user_input.lower() == "exit":
            print("Goodbye!")
            break
        elif user_input.lower() == "mysql":
            current_database = "mysql"
            print("You are now asking questions about mysql")
            continue
        elif user_input.lower() == "mongodb":
            current_database = "mongodb"
            print("You are now asking questions about mongodb")
            continue
        
        if current_database == "mysql":
            conn = chat_mysql(user_input, conn)
        else:
            chat_mongodb(user_input, client)

# Run the chatbot
if __name__ == "__main__":
    chatbot()