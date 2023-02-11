import os
import csv
import sys
import subprocess


# TODO: scale to different data sizes?
operations = ['create', 'read', 'update', 'delete']

durations = {}

for op in operations:
    # Run CRUD script
    duration = subprocess.check_output([sys.executable, f"{op}.py"])
    # Convert to float because subprocess returns output as a byte string
    durations[op] = float(duration.strip())

    
filename = 'neo4j_results.csv'

with open(filename, 'a+') as fd:
    # Create a CSV dictionary writer and add the student header as field names
    writer = csv.DictWriter(fd, fieldnames=operations)
    # File may be empty
    if not os.stat(filename).st_size:
        writer.writeheader()
    # Write new results
    writer.writerow(durations)
