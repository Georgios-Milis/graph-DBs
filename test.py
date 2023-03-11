import os
from time import sleep
from dotenv import load_dotenv

from connection import JanusGraphConnection


load_dotenv()
URI = os.getenv('JANUSGRAPH_URI')

# Initialize connection to database

scale = 5

print(scale)

connection = JanusGraphConnection(URI, scale)
# connection.clear_database()

#sleep(15)
#connection.load_graph()

print(connection.find_paper("1516859612"))
print(connection.title_of_paper("1516859612"))

print(connection.query("g.V().count()"))
# print(connection.query("g.tx().commit()"))

print()
