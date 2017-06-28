#!/usr/bin/env bash
sh rmLog.sh
sudo chmod u+x *.sh
./main
sh server0fork.sh
sh server1fork.sh
sleep 3
sh worker0fork.sh
sh worker1fork.sh
