# Analysis and Design of Information Systems
## Performance Comparison of Two Distributed Graph Databases: Neo4j and JanusGraph

### Installation
#### Our system
```bash
conda create --name graphs python=3.9 pip
conda activate graphs
pip install -r requirements.txt
``` 

#### Neo4j
[Download](https://neo4j.com/download/) Noe4j Desktop and create a DBMS with 6 databases, named "scale-1" to "scale-6". Change the credentials in the `.env` file. Install the APOC plugin for the DBMS. 

You can start the DBMS from the UI.

#### Janusgraph
Run the [image](https://hub.docker.com/r/janusgraph/janusgraph) in a Docker container. Once launched, run
```bash
docker run --name container-name -it -p 8182:8182 janusgraph/janusgraph
``` 
in some terminal to create a container for the specific database, with name container-name (optional), which forwards the port 8182. If you also want a Gremlin client console, run
```bash
docker run --rm --link janusgraph-default:janusgraph -e GREMLIN_REMOTE_HOSTS=janusgraph -it janusgraph/janusgraph:latest ./bin/gremlin.sh
```
Use `:remote connect tinkerpop.server conf/remote.yaml` and `:remote console` to connect the console to the same port.


### Usage
Run `fill_neo.py` or `fill_janus.py` to fill each database. By default, the script empties and fills the databses a few times to measure the transaction time. Then, run `create.py`, `read.py`, `update.py` or `delete.py` to test any of the CRUD operations. The CRUD scripts also test each operation a few times to exctract the desired metrics. You can easily change that by changing the `N_TRIALS` parameter (set it to 1 for one iteration). 

We test sequentially for all datasets, from scales 1 to 6. You can change the `START` and `END` variables, or explicitly modify the itarable for the scale code.

We also implemeted the same API for the two systems under test, so that the CRUD scripts can run for either database, interchangeably.

### Data
We have synced the dataset `txt` files and corresponding `pkl` pickles (made for fast loading during experimentation) up to scale 4. To construct other subsets of data, use the script `data/process_dataset.py` that contains utility functions to process the DBLP V11 dataset. The full processed dataset can be found [here](https://drive.google.com/file/d/1FyYhJMTntnDpKKBXBev8cOCE2PkIDjBf).

### Contributors (alphabetically)
- Gkrinias Georgios
- Milis Georgios
- Tsorvantzis Apostolos