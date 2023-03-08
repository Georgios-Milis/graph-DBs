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
Download Noe4j Desktop and create a DBMS with 6 databases, named "scale-1" to "scale-6". Change the credentials in the `.env` file.

#### Janusgraph
Run the image in a Docker container. Once launched, run
```bash
docker run -it -p 8182:8182 janusgraph/janusgraph
``` 
in some terminal to forward the port. If ypu want a Gremlin client console, run
```bash
docker run --rm --link janusgraph-default:janusgraph -e GREMLIN_REMOTE_HOSTS=janusgraph -it janusgraph/janusgraph:latest ./bin/gremlin.sh
```
Use `:remote connect tinkerpop.server conf/remote.yaml` and `:remote console` to connect the console to the same port.


### Contributors
- Gkrinias Georgios
- Milis Georgios
- Tsorvantzis Apostolos