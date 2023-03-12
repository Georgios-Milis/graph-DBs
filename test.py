import os
from time import sleep
from dotenv import load_dotenv
import time
from connection import JanusGraphConnection, transact_and_time


load_dotenv()
URI = os.getenv('JANUSGRAPH_URI')

# Initialize connection to database

scale = 5

print(scale)

connection = JanusGraphConnection(URI, scale)




# connection.clear_database()

# sleep(5)

# print(transact_and_time(connection.load_graph))


# print(connection.find_paper("1229652591"))
# print(connection.title_of_paper("1229652591"))
s_time = time.time()
print(connection.query("g.V().count()"))
print(time.time() - s_time)