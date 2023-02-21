import os
import csv
import subprocess


# Database names, each one contains data on a different scale 
dbs = ['scale-' + str(i+1) for i in range(6)]

# Scripts to be timed, each one containing CRUD operations
operations = ['create', 'read', 'update', 'delete']

# Time duration of each script execution
durations = {}

# TODO: get durations as dict

for db in dbs:
    for op in operations:
        # Command to execute
        COMMAND = f"python {op}.py {db}"
        # subprocess.check_output requires a list of input tokens
        duration = subprocess.check_output(COMMAND.split())
        # Convert to float because subprocess returns output as a byte string
        durations[op] = float(duration.strip())

# TODO: figure out a better format to save data
filename = 'neo4j_results.csv'

with open(filename, 'a+') as fd:
    # Create a CSV dictionary writer and add the student header as field names
    writer = csv.DictWriter(fd, fieldnames=operations)
    # File may be empty
    if not os.stat(filename).st_size:
        writer.writeheader()
    # Write new results
    writer.writerow(durations)
