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

using namespace std;

void* start(void* args) {
    cout<<"son"<<endl;
    Node* node =(Node*)args;
    void* socket = repListener(node->getTCP());
    while(true) {
        cout<<"try to receive msg"<<endl;
        log("try to receive msg");
        Message* msg = new Message();
        zmq_recv(socket, msg, sizeof(msg), 0);
        if (msg->type == OK) {
            cout<<"receive OK"<<endl;
            cout << "error msg->type => " <<  msg->type << endl;
            log("receive OK");
            log("error msg->type => " + msg->type);
            break;
        } else if (msg->type == ADDNODE) {
            cout<<"receive ADDNODE"<<endl;
            break;
        } else if (msg->type == PUSH) {
            cout<<"receive PUSH"<<endl;
            break;
        } else if (msg->type == PULL) {
            cout<<"receive PULL"<<endl;
            break;
        } else if (msg ->type == STOP) {
            cout<<"receive ok"<<endl;
        } else {
            cout << "error msg->type => " <<  msg->type << endl;
            log("error msg->type");
            break;
        }
    }
}



int main(int argc, char *argv[]) {
    cout<<"start server"<<endl;
    Node* node = new Node(argv[1], argv[2], argv[3], argv[4]);
    for(int i = 5; i < argc; i++) {
        node->addLinks(argv[i]);
    }
//    pthread_t id;
//    pthread_create(&id, NULL, start, (void*)node);
    start((void*) node);
}