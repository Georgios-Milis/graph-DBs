# info-systems


### Environment
conda create --name graphs python=3.8

conda activate graphs

pip install -r requirements.txt


### PREREQUISITES
sudo apt update
sudo apt install default-jre

python3 -m pip install testresources
python3 -m pip install gremlinpython==3.2.6

./bin/gremlin-server.sh install org.apache.tinkerpop gremlin-python 3.2.6

##### START SERVER IN A TERMINAL
./bin/gremlin-server.sh start

##### LAUNCH GREMLIN IN ANOTHER
./bin/gremlin.sh

:remote connect tinkerpop.server conf/remote.yaml
:remote console


### Temporary links
schema link: https://www.overleaf.com/9269282994wzfjkwgfdshk
