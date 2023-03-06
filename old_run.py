import os
import csv
import json
import subprocess
from os.path import join as pjoin


# This file's absolute path
path = os.path.dirname(os.path.realpath(__file__))

# DBMS names
DBMSs = ['neo4j', 'janus']
# Database names, each one contains data on a different scale 
DBs = ['scale-' + str(i+1) for i in range(6)]

# Scripts to be timed, each one containing CRUD operations
operations = ['create', 'read', 'update', 'delete']

# Time duration of each script execution
duration_dict = {}


# TODO milis: TEMPORARY
DBMSs = DBMSs[:1]
DBs = DBs[:1]
operations = operations[:1]



for dbms in DBMSs:
    for db in DBs:
        for op in operations:
            # Command to execute
            COMMAND = f"python {op}.py {db}"
            # subprocess.check_output requires a list of input tokens
            durations = subprocess.check_output(COMMAND.split()).rstrip()
            # Decode and convert single quotes to double
            durations = durations.decode('utf-8').replace("\'", "\"")
            durations = json.loads(durations)
            # Append to dictionary
            duration_dict.update(durations)

        filename = pjoin(path, 'results', f'{dbms}_{db}.csv')

        with open(filename, 'a+') as fd:
            # Create a CSV dictionary writer and add the student header as field names
            header = duration_dict.keys()
            writer = csv.DictWriter(fd, fieldnames=header)
            # File may be empty
            if not os.stat(filename).st_size:
                writer.writeheader()
            # Write new results
            writer.writerow(duration_dict)
