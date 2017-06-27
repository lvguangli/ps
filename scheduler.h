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
void* call(void * args) {
    log((char*)args);
    system((char*) args);
}

void startNode(Node* node) {
    string cmd = "ssh -o StrictHostKeyChecking=no " + node->host + " 'cd /Users/sahara/CLionProjects/ps; ";
    if(node->name[0] == 's') {
        cmd = cmd + "./server " + node->toString() + "'";
    } else if(node->name[0] == 'w') {
        cmd = cmd + "./worker " + node->toString() + "'";
    } else {
        cout<<node->name<<endl;
    }
    pthread_t id;
    pthread_create( &id, NULL, call, (void*)cmd.c_str());
}

void* sendMsg(void* socket) {
    log("try to send msg");
    Message* message = new Message();
    message->type = OK;
    int r = zmq_send(socket, message, sizeof(message), 0);
    if(r < 0) {
        log("send fail");
    }
}

void startScheduler(unordered_map<int, Node*> servers, unordered_map<int, Node*> workers,Node* scheduler) {
    unordered_map<int, void*> sockets;
    int sLen = 1;//servers.size();
    for(int i = 0; i < sLen; i++) {
        startNode(servers[i]);
        sockets[i] = reqListener(servers[i]->getTCP());
    }
//    int wLen = workers.size();
//    for(int i = 0; i < wLen; i++) {
//        startNode(workers[i]);
//        sockets[i + sLen] = reqListener(workers[i]->getTCP());
//    }

    pthread_t id;
    pthread_create( &id, NULL, sendMsg, sockets[0]);



}


#endif //PS_SCHEDULER_H
