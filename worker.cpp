//
// Created by Sahara on 27/06/2017.
//

#include <iostream>
#include <zmq.h>
#include<unordered_map>
#include <cstring>
#include "node.h"
#include "message.h"
#include "util.h"

using namespace std;

void start(Node* node) {
    void* socket = repListener(node->getTCP());
    while(true) {
        Message* msg = new Message();
        zmq_recv(socket, msg, sizeof(msg), 0);
        if (msg->type == OK) {
            cout<<"receive OK"<<endl;
            cout << "error msg->type => " <<  msg->type << endl;
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
            break;
        }


    }
}



int main(int argc, char *argv[]) {
    cout<<"start worker"<<endl;
//    Node* node = new Node(argv[0], argv[1], argv[2], argv[3]);
//    for(int i = 4; i < argc; i++) {
//        node->addLinks(argv[i]);
//    }
//    start(node);

}