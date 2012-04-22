#!/bin/bash

cd /home/accts/krv6/Documents/junior/project/HDFSProjectInsideHadoop/

bin/start-all.sh
bin/hadoop-daemons.sh --config conf2/ start datanode
