#!/usr/bin/env bash
sh rmLog.sh
chmod u+x *.sh
./generateScript
sh server0fork.sh
sh server1fork.sh
sleep 1
sh worker0fork.sh
sh worker1fork.sh
