import os
import json
from os.path import join as pjoin


# dblp_processed.txt contains ~ 1.3M papers
    
path = os.path.dirname(os.path.realpath(__file__))
data_file = pjoin(path, 'data', 'dblp_papers_v11.txt')
processed_file = pjoin(path, 'data', 'dblp_demo.txt')

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

with open(processed_file, 'w') as fw:
    with open(data_file, 'r') as fr:
        for i, paper in enumerate(fr):
            # Load paper JSONs line by line
            original = json.loads(paper)
            # Filter JSON to keep the most interesting data
            modified = {k: v for k, v in original.items() if k in fields_kept}
            # Write filtered JSON
            json.dump(modified, fw)
            fw.write('\n')
            if i == 100: break
