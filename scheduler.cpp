//
// Created by Sahara on 29/06/2017.
//

//
// Created by Sahara on 26/06/2017.
//

#include <iostream>
#include <zmq.h>
#include<unordered_map>
#include <cstring>
#include "node.h"
#include "message.h"
#include "util.h"
#include <pthread.h>
#include "log.h"
#include "constant.h"
#include <unistd.h>
#include<iostream>
#include<istream>
#include <stdlib.h>
#include <ctime>
#include <time.h>

using namespace std;
string file = "scheduler";
unordered_map<int,void*> sockets;
Node* scheduler;
int mainId = 9;
int hasIter[10];

void* notifyNodesADDMSG(void* args) {
    void* socket = args;
    int index = 0;
    for(;;) {
        if(socket == sockets[index]) {
            break;
        }
        index ++;
    }
    Data msg = Data();
    msg.type = ADDNODE;
    msg.start = 0;
    msg.end = 1;
    msg.initData(1,2);
    string str = msg.toString();
    string nodeName = "server" + to_string(index);
    if(index >= scheduler->serverNum){
        nodeName = "worker" + to_string(index - scheduler->serverNum);
    }

    log(" send msg to " + nodeName + " error.size " + to_string(str.size()) , file, mainId);
    int len = zmq_send(socket, str.c_str(), str.size(), 0);
    while(len < 0) {
        len = zmq_send(socket, str.c_str(), str.size(), 0);
    }
    char tmp[OKMSGLEN];
    len = zmq_recv(socket, tmp, OKMSGLEN, 0);
    while(len < 0) {
        len = zmq_recv(socket, tmp, OKMSGLEN, 0);
    }
    tmp[len] = '\0';
    msg = Data(tmp);
    log("receive msg from " + to_string(index) + " size =  " + to_string(strlen(tmp)) + " msg=" + msg.toString(),file, mainId);
    hasIter[index] = (int) msg.data[0][0];
    log("receive msg from index=" + to_string(index) + " hasIter=" + to_string(hasIter[index]),file, mainId);
    return NULL;
}

void* stopNode(void* args) {
    void* socket = args;
    int index = 0;
    for(;;) {
        if(socket == sockets[index]) {
            break;
        }
        index ++;
    }
    Data msg = Data();
    msg.type = STOP;
    msg.start = -1;
    msg.end = scheduler->workerNum - 1;
    string str = msg.head();
    string nodeName = "server" + to_string(index);
    if(index >= scheduler->serverNum){
        nodeName = "worker" + to_string(index - scheduler->serverNum);
    }

    log(" send msg to " + nodeName + " error.size " + to_string(str.size()) , file, mainId);

    while(zmq_send(socket, str.c_str(), str.size(), 0) < 0) {

    }
    int MAXLEN = 20;
    char tmp[MAXLEN];
    zmq_recv(socket, tmp, MAXLEN, 0);
    msg = Data(tmp);
    log("receive msg from server " + to_string(index) + " size =  " + to_string(strlen(tmp)) + msg.toString(),file, mainId);
    return NULL;
}

void* notifyNodesRESTART(void* args) {
    void* socket = args;
    int index = 0;
    for(;;) {
        if(socket == sockets[index]) {
            break;
        }
        index ++;
    }
    Data msg = Data();
    if(index < sockets.size() - 1) {
        msg.type = RESTART;
    } else {
        msg.type = ITERMSG;
    }
    msg.start = 0;
    msg.end = 1;
    msg.initData(1, hasIter[0]);
    string str = msg.toString();
    log("send msg.size " + to_string(str.size()) + "msg=" + str , file, mainId);
    int len = zmq_send(socket, str.c_str(), str.size(), 0);
    while(len < 0) {
        len = zmq_send(socket, str.c_str(), str.size(), 0);
    }
    char tmp[OKMSGLEN];
    len = zmq_recv(socket, tmp, OKMSGLEN, 0);
    while(len < 0) {
        len = zmq_recv(socket, tmp, OKMSGLEN, 0);
    }
    msg = Data(tmp);
    if(msg.type != OK) {
        exit(0);
    }
    log("receive msg from newWorker " + to_string(index) + " size =  " + to_string(strlen(tmp))  + " msg=" + msg.toString(),file, mainId);
    return NULL;
}

void* add_scheduler(void* args) {
    log("add_scheduler", file, mainId);
    for(int i = 0; i < 5; i++) {
        hasIter[i] = -1;
    }
    log("start_scheduler", file, mainId);
    Node* node =(Node*)args;
    log(node->name + " " + node->getTCP(),file, mainId);
    for(int i = 0; i< node->linksNum; i++) {
        string ip = node->linksIP[i] + to_string(node->id);
        void* socket = reqListener(ip);
        if(socket != NULL)
            log("scheduler connect to ip = " + ip, file, mainId);
        sockets[i] = socket;
    }
    vector<pthread_t> threads;
    for(int i = 0; i< sockets.size() - 1; i++) {
        pthread_t id;
        void* socket = sockets[i];
        pthread_create(&id, NULL, notifyNodesADDMSG, socket);
        threads.push_back(id);
    }
    for(int i =0; i < threads.size(); i++) {
        pthread_join(threads[i],NULL);
    }

    threads.clear();
    int iter = hasIter[0];
    for(int i = 1; i < sockets.size() - 1; i++) {
        if(hasIter[i] > iter) {
            iter = hasIter[i];
        }
    }
    if(iter < 0) {
        exit(0);
    }
    hasIter[0] = iter + 2;
    for(int i = 0; i< sockets.size(); i++) {
        pthread_t id;
        void* socket = sockets[i];
        pthread_create(&id, NULL, notifyNodesRESTART, socket);
        threads.push_back(id);
        if(i == node->serverNum - 1) {
            sleep(1);
        }
    }
    for(int i =0; i < threads.size(); i++) {
        pthread_join(threads[i],NULL);
    }
    log("add_scheduler finished", file, mainId);
    return NULL;
}

void* stop_scheduler(void* args) {
    log("stop_scheduler", file, mainId);
    Node* node =(Node*)args;

    log(node->name + " " + node->getTCP(),file, mainId);
    for(int i = 0; i< node->linksNum; i++) {
        string ip = node->linksIP[i] + to_string(node->id);
        void* socket = reqListener(ip);
        if(socket != NULL)
            log("scheduler connect to ip = " + ip, file, mainId);
        sockets[i] = socket;
    }
    vector<pthread_t*> threads;
    for(int i = 0; i< sockets.size(); i++) {
        pthread_t id;
        void* socket = sockets[i];
        pthread_create(&id, NULL, stopNode, socket);
        threads.push_back(&id);
        if(i == node->serverNum) {
            sleep(2);
        }
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    //./scheduler 0 scheduler0 192.168.1.100 12313 tcp://192.168.1.100:12400 192.168.1.100:12308 192.168.1.100:12309

    scheduler = new Node(argv[1]);
    for(int i = 6; i < argc; i++) {
        scheduler->addLinks(argv[i]);
    }
    file = file + to_string(scheduler->id);
    log(file, mainId);
    log("start scheduler", file, mainId);
    if(strcmp(argv[2], "StopNode") == 0) {
        pthread_t id;
        pthread_create(&id, NULL, stop_scheduler, (void*)scheduler);
    } else if(strcmp(argv[2], "AddNode") == 0){
        pthread_t id;
        pthread_create(&id, NULL, add_scheduler, (void*)scheduler);
        pthread_join(id, NULL);
    }
    sleep(20);
    log("scheduler "+ to_string(scheduler->id) + " finished", file, mainId);


}