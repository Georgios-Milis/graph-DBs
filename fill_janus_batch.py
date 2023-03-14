"""This script can fill JanusGraph, by loading GraphSON files.
We also use it for experimentation and easy access to the database."""
import os
from dotenv import load_dotenv
from connection import JanusGraphConnection


load_dotenv()
URI = os.getenv('JANUSGRAPH_URI')

# Initialize connection to database
scale = 4
connection = JanusGraphConnection(URI, scale)

# connection.clear_database()

# connection.load_graph()

# print(connection.find_paper("1229652591"))
# print(connection.title_of_paper("1229652591"))

print(connection.query("g.V().count()"))
