# Analysis and Design of Information Systems
## Performance Comparison of Distributed Graph Databases

#### Installation
`conda create --name graphs python=3.9 pip`
`conda activate graphs`
`conda install pip`
`pip install -r requirements.txt`

### Neo4j
Download Noe4j Desktop and create a DBMS with 6 databases, named "scale-1" to "scale-6". Change the credentials in the .env file.

### Janusgraph
Run the image in a Docker container. Once launched, run 
`docker run -it -p 8182:8182 janusgraph/janusgraph` 
in some terminal to forward the port. For the Gremlin client console, run
`docker run --rm --link janusgraph-default:janusgraph -e GREMLIN_REMOTE_HOSTS=janusgraph -it janusgraph/janusgraph:latest ./bin/gremlin.sh`

#### Temporary
schema link: https://www.overleaf.com/9269282994wzfjkwgfdshk
