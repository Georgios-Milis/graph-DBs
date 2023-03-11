"""
This file contains functions that read the DBLP-Citation-network V11
dataset, and process it in order to separate it into different scale 
subsets. Visit https://www.aminer.org/citation to download the dataset.
"""
import os
import json
import pickle
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
data_file = pjoin(path, 'dblp_papers_v11.txt')
processed_file = pjoin(path, 'dblp_processed.txt') # ~ 1.3M papers
categories = pjoin(path, 'categories.txt')


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


# Write data in GraphSON format, for loading to JanusGraph
def write_graphson(infile, outfile):
    data = pickle.load(open(infile, 'rb'))

    paper_dict = {}
    author_dict = {}

    # Add unique ID to vertices and edges
    N = 0
    papers_raw = data['papers']
    papers = []
    for i, paper in enumerate(papers_raw, N):
        paper_dict[paper["id"]] = i
        #paper["id"] = i
        papers.append(paper)

    N += len(papers)
    authors_raw = data['authors']
    authors = []
    for i, author in enumerate(authors_raw, N):
        author_dict[author["id"]] = i
        #author["id"] = i
        authors.append(author)

    N += len(authors)
    authorships_raw = data['authorships']
    authorships = []
    for i, authship in enumerate(authorships_raw, N):
        authship["id"] = i
        authorships.append(authship)

    N += len(authorships)
    citations_raw = data['citations']
    citations = []
    for i, citation in enumerate(citations_raw, N):
        citation["id"] = i
        citations.append(citation)
    
    count = 0
    out = []

    for paper in papers:
        line = {"id": paper_dict[paper["id"]], "label": "paper"}
        refed_list = [{"id": ref["id"], "outV": paper_dict[ref["from"]]} for ref in citations if ref["to"] == paper["id"]]
        authed_list = [{"id": auth["id"], "outV": author_dict[auth["author"]]} for auth in authorships if auth["paper"] == paper["id"]]
        inE = {}
        if refed_list:
            inE["reference"] = refed_list
        if authed_list:
            inE["authorship"] = authed_list
        if inE:
            line.update({"inE": inE})

        refing_list = [{"id": ref["id"], "inV": paper_dict[ref["to"]]} for ref in citations if paper_dict[ref["from"]] == paper["id"]]
        if refing_list:
            line.update({"outE": {
                "reference": refing_list
            }})

        line.update({
            "properties": {
                "id": [{"id": count, "value": str(paper["id"])}],
                "title": [{"id": count + 1, "value": paper["title"]}],
                "year": [{"id": count + 2, "value": paper["year"]}]
            }
        })
        count += 3
        out.append(line)

    for author in authors:
        line = {"id": author_dict[author["id"]], "label": "author"}
        auth_list = [{"id": auth["id"], "inV": paper_dict[auth["paper"]]} for auth in authorships if auth["author"] == author["id"]]
        if auth_list:
            line.update({"outE": {
                "authorship": auth_list
            }})

        line.update({
            "properties": {
                "id": [{"id": count, "value": str(author["id"])}],
                "name": [{"id": count + 1, "value": author["name"]}]
            }
        })
        count += 2
        out.append(line)
    
    with open(outfile, 'w', encoding='utf-8') as f:
        for line in out:
            json.dump(line, f)
            f.write('\n')


if __name__ == "__main__":
    for scale in range(1, 7):
        print(scale)
        infile = pjoin(path, f'dataset_{scale}.pkl')
        outfile = pjoin(path, f'dataset_{scale}.json')
        write_graphson(infile, outfile)
