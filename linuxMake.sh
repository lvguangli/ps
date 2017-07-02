#!/usr/bin/env bash
g++ -o server server.cpp -lzmq -I /usr/local/include/ -L /usr/local/lib/ -std=c++11 -lpthread
g++ -o worker worker.cpp -lzmq -I /usr/local/include/ -L /usr/local/lib/ -std=c++11 -lpthread
g++ -o scheduler scheduler.cpp -lzmq -I /usr/local/include/ -L /usr/local/lib/ -std=c++11 -lpthread
g++ -o generateScript generateScript.cpp -lzmq -I /usr/local/include/ -L /usr/local/lib/ -std=c++11 -lpthread
g++ -o newWorker newWorker.cpp -lzmq -I /usr/local/include/ -L /usr/local/lib/ -std=c++11 -lpthread