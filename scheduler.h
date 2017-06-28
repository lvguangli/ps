//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_SCHEDULER_H
#define PS_SCHEDULER_H

#include "node.h"
#include <cstdlib>
#include "util.h"
#include "message.h"
#include <pthread.h>
#include <unistd.h>
#include "log.h"
using namespace std;
string file = "scheduler.txt";
void* call(void * args) {
    string shell = (char*)args;
    shell = "sh " + shell;
    log(shell, file);
    int r = system(shell.c_str());
    cout <<"call r="<<r<<endl;
    return NULL;
}

void startNode(Node* node) {
//    cout<<"scheduler::startNode"<<endl;

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
//    pthread_t id;
//    pthread_create( &id, NULL, call, (void*)file.c_str());
}

void* sendMsg(void* socket) {
//    cout<<"scheduler::sendMsg"<<endl;
    log("try to send msg", file);
    Data* message = new Data();
    message->type = ADDNODE;
    int r = zmq_send(socket, message, sizeof(message), 0);
    if(r < 0) {
        log("send fail");
    }
    return NULL;
}

void startScheduler(unordered_map<int, Node*> servers, unordered_map<int, Node*> workers,Node* scheduler) {
//    cout<<"scheduler::startScheduler"<<endl;
    unordered_map<int, void*> sockets;
    int sLen = servers.size();
    for(int i = 0; i < sLen; i++) {
        startNode(servers[i]);
//        sockets[i] = reqListener(servers[i]->getTCP());
    }
    sleep(3);
    int wLen = workers.size();
    for(int i = 0; i < wLen; i++) {
        startNode(workers[i]);
//        sockets[i + sLen] = reqListener(workers[i]->getTCP());
    }
//    int a;
    while(true) {
        string c;
        cin>>c;
        if(c == "stop") {
            return;
        }
        else if(c == "add") {

        }
    }
    getchar();
//    pthread_t id;
//    pthread_create( &id, NULL, sendMsg, sockets[0]);



}


#endif //PS_SCHEDULER_H
