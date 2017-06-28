#include <iostream>
#include <zmq.h>
#include<unordered_map>
#include <cstring>
#include "scheduler.h"
#include "node.h"
#include "constant.h"
#include <pthread.h>
#include <unistd.h>

/**
 * scheduler
 */
using namespace std;

unordered_map<int, Node*> workers;
unordered_map<int, Node*> servers;
Node* scheduler;





void bindSW(Node* s, Node* w) {
    s->addLinks(w->getTCP());
    w->addLinks(s->getTCP());
}
void* start(void* agrs) {
    startScheduler(servers, workers, scheduler);
}

int main(int argc, char *argv[]) {
    log("scheduler.txt");
    scheduler = new Node("scheduler",0,schedulerIP[0], schedulerIP[1], "");
    for (int i =0; i<serverN ; i++){
        string host = shosts[i][0];
        string port = shosts[i][1];
        string name = "server" + to_string(i);
        Node* nodePtr = new Node(name,i, host, port, scheduler->getTCP());
        servers[i] = nodePtr;
    }
    for (int i =serverN; i<serverN + workerN ; i++){
        string host = whosts[i - serverN][0];
        string port = whosts[i - serverN][1];
        string name = "worker" + to_string(i - serverN);
        Node* nodePtr = new Node(name, i - serverN, host, port, scheduler->getTCP());
        workers[i - serverN] = nodePtr;
    }
    for(int i = 0; i < serverN; i++) {
        for(int j = 0; j < workerN; j++) {
            bindSW(servers[i], workers[j]);
        }
    }
    pthread_t id;
    pthread_create( &id, NULL, start, NULL);

    cout << "Hello, World!" << std::endl;
    pthread_join(id,NULL);
    return 0;
}