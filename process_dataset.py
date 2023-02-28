"""
This file contains functions that read the DBLP-Citation-network V11
dataset, and process it in order to separate it into different scale 
subsets. Visit https://www.aminer.org/citation to download the dataset.
"""
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

# Paths
path = os.path.dirname(os.path.realpath(__file__))
data_file = pjoin(path, 'data', 'dblp_papers_v11.txt')
processed_file = pjoin(path, 'data', 'dblp_processed.txt') # ~ 1.3M papers
categories = pjoin(path, 'data', 'categories.txt')


# Filter original raw data
def process_raw(infile, outfile, fields_kept=fields_kept):
    with open(outfile, 'w', encoding='utf-8') as fw:
        with open(infile, 'r', encoding='utf-8') as fr:
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


# Get all categories and their frequency
def write_gategories(infile, outfile=categories):
    all_fields = []
    # Split into categories
    with open(infile, 'r', encoding='utf-8') as f:
        for paper in f:
            data = json.loads(paper)
            try:
                fields = data['fos']
                all_fields += [fos['name'] for fos in fields]
            except KeyError: # Some data is missing
                continue
    # Count categories and sort them according to frequency
    counts = sorted(Counter(all_fields).items(), key=lambda tup: tup[1], reverse=True)
    with open(outfile, 'w', encoding='utf-8') as f:
        for category, count in counts:
            f.write(f"{category}, {count}\n")


# Construct smalled datasets by category
def write_papers_by_category(infile, outfile, category):
    with open(outfile, 'w', encoding='utf-8') as fw:
        with open(infile, 'r', encoding='utf-8') as fr:
            for paper in fr:
                data = json.loads(paper)
                try:
                    for fos in data['fos']:
                        if fos['name'] == category:
                            json.dump(data, fw)
                            fw.write('\n')
                            break
                except KeyError: # Some data is missing
                    continue


if __name__ == "__main__":
    outfile = pjoin(path, 'data', 'scale2_Aeroacoustics.txt')
    write_papers_by_category(processed_file, outfile, 'Aeroacoustics')
