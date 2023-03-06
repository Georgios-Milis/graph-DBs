import os
import re
import numpy as np
import pandas as pd
from time import time
from dotenv import load_dotenv
from os.path import join as pjoin

import data
from connection import JanusGraphConnection, transact_and_time


# TODO atrosvat: make it like create_janus
# TODO atsorvat: write delete and update janus

# This file path
path = os.path.dirname(os.path.realpath(__file__))

# Config
load_dotenv()
URI = os.getenv('JANUSGRAPH_URI')

connection = JanusGraphConnection(URI)

query = " g.V().has('paper', 'id', 3).values('title')"
print(connection.gremlin_conn.submit(query).next())
