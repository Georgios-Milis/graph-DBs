import os
import json
import time
from dotenv import load_dotenv
from os.path import join as pjoin

from connection import Connection


if __name__ == "__main__":
    path = os.path.dirname(os.path.realpath(__file__))

    load_dotenv()
    URI = os.getenv('NEO4J_URI')
    USERNAME = os.getenv('NEO4J_USERNAME')
    PASSWORD = os.getenv('NEO4J_PASSWORD')
    INSTANCE = os.getenv('AURA_INSTANCENAME')

    # For local instance
    URI = "bolt://localhost:7687/scale-1"
    USERNAME = "neo4j"
    PASSWORD = "12345678"
    INSTANCE = "scale-1"

    # Initialize connection to database
    connection = Connection(URI, USERNAME, PASSWORD)


    # CREATE
    start_time = time.time()

    datafile = pjoin(path, 'data', 'food_technology.txt')
    with open(datafile, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            data = json.loads(line)
            filtered_data = {k: v for k, v in data.items() if k in ['id', 'title', 'year', 'n_citation']}
            connection.create_paper(filtered_data)

    duration = time.time() - start_time

    # Print so that subprocess.check_output gets the result
    print(duration)

    # Close
    connection.close()
