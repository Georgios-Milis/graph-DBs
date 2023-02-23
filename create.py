import os
import sys
from dotenv import load_dotenv
from os.path import join as pjoin

import data
from connection import Connection, transact_and_time


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError("Usage: python script.py database-name")

    # Read database name
    database = sys.argv[1]

    # This file path
    path = os.path.dirname(os.path.realpath(__file__))
    # Dataset TODO scale 
    datafile = pjoin(path, 'data', 'Magnetism.txt')

    # Config
    LOCAL = True
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

    # Durations dictionary
    durations = {}

    # Load data one at a time, execute transaction and then delete it
    papers = data.get_papers_data(datafile)
    durations.update(transact_and_time(connection.create_papers, papers))
    del papers

    authors = data.get_authors_data(datafile)
    durations.update(transact_and_time(connection.create_authors, authors))
    del authors

    citations = data.get_citations_data(datafile)
    durations.update(transact_and_time(connection.create_references, citations))
    del citations

    authorships = data.get_authorships_data(datafile)
    durations.update(transact_and_time(connection.create_authorships, authorships))
    del authorships

    # Close connection
    connection.close()

    # Print so that subprocess.check_output gets the result
    print(durations)
