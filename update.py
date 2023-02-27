import os
import sys
from time import time
from dotenv import load_dotenv

from connection import Connection


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError("Usage: python script.py database-name")

    # Read database name
    database = sys.argv[1]

    # This file path
    path = os.path.dirname(os.path.realpath(__file__))

    LOCAL = False

    if not LOCAL:
        from dotenv import load_dotenv
        load_dotenv()
        URI = os.getenv('NEO4J_URI')
        USERNAME = os.getenv('NEO4J_USERNAME')
        PASSWORD = os.getenv('NEO4J_PASSWORD')
        INSTANCE = os.getenv('AURA_INSTANCENAME')
    else:
        URI = "bolt://localhost:7687"
        USERNAME = "neo4j"
        PASSWORD = "12345678"
        INSTANCE = database

    # Initialize connection to database
    connection = Connection(URI, USERNAME, PASSWORD, INSTANCE)

    start_time = time()
    
    duration = time() - start_time

    # Close
    connection.close()

    # Print so that subprocess.check_output gets the result
    print(duration)
