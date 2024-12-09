# ChatDB

Welcome to the repository for the ChatDB course project for USC 2024 Fall DSCI-551 Foundations of Data Management.

## Abstract
The goal of this project is to develop ChatDB, an interactive ChatGPT-like application that assists users in learning how to query data in database systems, including SQL and NoSQL databases. For example, it can suggest sample queries, including ones that include specific language constructs (e.g., group by in SQL), and understand queries in natural language. But unlike ChatGPT, it can also execute the queries in the database systems and display the query results to the users.

## Project Structure

The project structure can be divided into logical components, with `main.py` serving as the central script to initiate the application, and the `data` folder containing separate directories for datasets and collections specific to MySQL and MongoDB.

```
.
├── data
│   ├── mongodb        # Directory to store the dataset collections for MongoDB.
│   └── mysql          # Directory to store the dataset files for MySQL.
├── .gitignore         # Specifies intentionally untracked files to ignore in Git.
├── main.py            # The main entry point of the application.
├── mongodb_chat.py    # Handles MongoDB-related chat functionalities.
├── mysql_chat.py      # Handles MySQL-related chat functionalities.
├── requirements.txt   # Lists the dependencies required to run the project.
└── README.md          # Provides an overview and usage instructions for the project.
```

## Environment

We provide an [environment file](requirements.txt) to install the required libraries to run the ChatDB App.

```shell
pip install -r environment.txt
```

## Usage

This is a command line App, use the following command to enter the interaction section:

```shell
python main.py
```

By default, we use connection details as such:

```python
# MySQL connection details
MYSQL_HOST = "localhost"
MYSQL_USER = "dsci551"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "551project"

# MongoDB connection details
ATLAS_URI = "mongodb+srv://xyxy:DSCI551@cluster48297.lfyhked.mongodb.net/?retryWrites=true&w=majority&appName=Cluster48297"
DATABASE_NAME = "DSCI551"
```

After entering the interaction section, use `mysql` and `mongodb` to choose to the database to interact with. Use `help` to see what kind of instructions the ChatDB App supports (including `sample`, `sample {language construction}`, and natural language question answering).

## Contribution

### Code contributors

- [Hanhui Wang](https://github.com/SARIHUST)
- [Yi Xia](https://github.com/yixia168)

### MongoDB Host (Default)

- [Yi Xia](https://github.com/yixia168)
