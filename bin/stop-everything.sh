#!/bin/bash

cd /home/accts/krv6/Documents/junior/project/HDFSProjectInsideHadoop/

bin/stop-all.sh
bin/hadoop-daemons.sh --config conf2/ stop datanode
bin/hadoop-daemons.sh --config conf4/ stop datanode
