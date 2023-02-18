import os
from time import time

from connection import Connection


if __name__ == "__main__":
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
        INSTANCE = "scale-2"

    # Initialize connection to database
    connection = Connection(URI, USERNAME, PASSWORD, INSTANCE)

    data = {
        "papers": [],
        "authors": [],
        "references": [],
        "authorships": []
    }

    for i in range(10000):
        data["papers"].append(
            {
                'id': i,
                'title': "Ontologies in HYDRA - Middleware for Ambient Intelligent Devices.",
                'year': 2009,
                'n_citation': 2
            }
        )

    for i in range(10000):
        data["authors"].append(
            {
                'id': i,
                'name': "Peter Kostelnik",
                'org': 'University of Munich'
            }
        )
    
    for i in range(1000):
        data["references"].append(
            {
                'from': i,
                'to': i + 1
            }
        )
    
    for i in range(1000):
        data["authorships"].append(
            {
                'author': i,
                'paper': i + 5
            }
        )

    connection.clear_database()
    print("Creating papers...")

    start_time = time()
    
    connection.create_papers(data["papers"])
    connection.create_authors(data["authors"])

    connection.find_paper(3)

    connection.create_references(data["references"])
    connection.create_authorships(data["authorships"])

    duration = time() - start_time
    print("Duration:", duration)


    # Close
    connection.close()
