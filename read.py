import os
import time
import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from dotenv import load_dotenv


if __name__ == "__main__":
    start_time = time.time()
    duration = time.time() - start_time

    # Print so that subprocess.check_output gets the result
    print(duration)
    