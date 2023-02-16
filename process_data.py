import os
import json
from collections import Counter
from os.path import join as pjoin


fields_kept = [
    'id',
    'title',
    'authors',
    'year',
    'keywords',
    'fos',
    'references',
    'n_citation'
]

# Filter original raw data - no need to run again
def process_raw(data_file, processed_file, fields_kept=fields_kept):
    with open(processed_file, 'w', encoding='utf-8') as fw:
        with open(data_file, 'r', encoding='utf-8') as fr:
            for i, paper in enumerate(fr):
                # Load paper JSONs line by line
                original = json.loads(paper)
                # Filter JSON to keep the most interesting data
                modified = {k: v for k, v in original.items() if k in fields_kept}
                # Write filtered JSON
                json.dump(modified, fw)
                fw.write('\n')
                # Uncomment this to process the whole dataset
                if i == 100: break

# Paths
path = os.path.dirname(os.path.realpath(__file__))
data_file = pjoin(path, 'data', 'dblp_papers_v11.txt')
processed_file = pjoin(path, 'data', 'dblp_processed.txt') # ~ 1.3M papers
sample_file = pjoin(path, 'data', 'dblp_sample.txt')       # ~ 100 papers
categories = pjoin(path, 'data', 'categories.txt')


# Process by field
all_fields = []

# Split into categories
with open(processed_file, 'r', encoding='utf-8') as f:
    for paper in f:
        data = json.loads(paper)
        try:
            fields = data['fos']
            all_fields += [fos['name'] for fos in fields]
        except KeyError: # Some data is missing
            pass

# Count categories and sort them according to frequency
counts = sorted(Counter(all_fields).items(), key=lambda tup: tup[1], reverse=True)

with open(categories, 'w', encoding='utf-8') as f:
    for category, count in counts:
        f.write(f"{category}, {count}\n")
