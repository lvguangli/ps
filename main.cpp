#include <iostream>
#include <zmq.h>
#include<unordered_map>
#include <cstring>
#include "scheduler.h"
#include "node.h"
#include "constant.h"

/**
 * scheduler
 */
using namespace std;

unordered_map<int, Node*> workers;
unordered_map<int, Node*> servers;





void bindSW(Node* s, Node* w) {
    s->addLinks(w->host + ':' + w->port);
    w->addLinks(s->host + ':' + s->port);
}

int main(int argc, char *argv[]) {
    for (int i =0; i<serverN ; i++){
        string host = shosts[i][0];
        string port = shosts[i][1];
        string name = "server" + to_string(i);
        Node* nodePtr = new Node(name, host, port, schedulerIP[0] + schedulerIP[1]);
        servers[i] = nodePtr;
    }
    for (int i =serverN; i<serverN + workerN ; i++){
        string host = whosts[i - serverN][0];
        string port = whosts[i - serverN][1];
        string name = "worker" + to_string(i);
        Node* nodePtr = new Node(name, host, port, schedulerIP[0] + schedulerIP[1]);
        workers[i - serverN] = nodePtr;
    }
    for(int i = 0; i < serverN; i++) {
        for(int j = 0; j < workerN; j++) {
            bindSW(servers[i], workers[j]);
        }
    }
    Node* scheduler = new Node("scheduler",schedulerIP[0], schedulerIP[1], schedulerIP[0] + schedulerIP[1]);
    startScheduler(servers, workers, scheduler);

    cout << "Hello, World!" << std::endl;
    return 0;
}