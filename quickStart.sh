#!/usr/bin/env bash
sh rmLog.sh
chmod u+x *.sh
./generateScript 2 2 start
./generateScript 2 3 middle
sh server0fork.sh
sh server1fork.sh
sleep 3
sh worker0fork.sh
sh worker1fork.sh
