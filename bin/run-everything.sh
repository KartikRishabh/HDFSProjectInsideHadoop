#!/bin/bash

cd /home/accts/krv6/Documents/junior/project/HDFSProjectInsideHadoop/

rm -rf logs/
rm -rf output/
bin/start-all.sh
bin/hadoop-daemons.sh --config conf2/ start datanode
bin/hadoop-daemons.sh --config conf4/ start datanode
jps | grep -v "Jps"
