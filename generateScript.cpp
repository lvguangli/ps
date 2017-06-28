#include <iostream>
#include <zmq.h>
#include<unordered_map>
#include <cstring>
#include "node.h"
#include "constant.h"
#include <pthread.h>
#include <unistd.h>
#include "log.h"

/**
 * scheduler
 */
using namespace std;

unordered_map<int, Node*> workers;
unordered_map<int, Node*> servers;
Node* scheduler;

void startNode(Node* node) {
//    string cmd = "ssh -o StrictHostKeyChecking=no " + node->host + " 'cd /Users/sahara/CLionProjects/ps; ";
    string cmd = "ssh sahara@" + node->host + " && cd /Users/sahara/CLionProjects/ps && ";
    if(node->name[0] == 's') {
        cmd = cmd + "./server " + node->toString();
    } else if(node->name[0] == 'w') {
        cmd = cmd + "./worker " + node->toString();
    }
    cmd = cmd + " &";
    string file = node->name + "fork.sh";
    cout<<cmd.c_str()<<endl;
    ofstream out(file,ios::out);
    out<<cmd<<endl;
    out.close();
}

void generateScript() {
    unordered_map<int, void*> sockets;
    for(int i = 0; i < servers.size(); i++) {
        startNode(servers[i]);
    }
    for(int i = 0; i < workers.size(); i++) {
        startNode(workers[i]);
    }

}

void bindSW(Node* s, Node* w) {
    s->addLinks(w->getTCP());
    w->addLinks(s->getTCP());
}

int main(int argc, char *argv[]) {
//    system("sh rmLog.sh");
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
        Node* nodePtr = new Node(name, i- serverN, host, port, scheduler->getTCP());
        workers[i - serverN] = nodePtr;
    }
    for(int i = 0; i < serverN; i++) {
        for(int j = 0; j < workerN; j++) {
            bindSW(servers[i], workers[j]);
        }
    }
    generateScript();
    cout << "Hello, World!" << std::endl;
    return 0;
}