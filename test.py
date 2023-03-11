import os
from dotenv import load_dotenv

from connection import JanusGraphConnection


load_dotenv()
URI = os.getenv('JANUSGRAPH_URI')

# Initialize connection to database
connection = JanusGraphConnection(URI, 4)
# connection.load_graph()

connection.clear_database()

print(connection.find_paper("1516859612"))
print(connection.title_of_paper("1516859612"))

print(connection.query("g.V().count()"))
# print(connection.query("g.tx().commit()"))
