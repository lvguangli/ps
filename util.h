//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_UTIL_H
#define PS_UTIL_H

#include <cstdlib>
#include <zmq.h>
#include <cstring>
#include <string>

using namespace std;
void* repListener(string ip) {
    void* context = zmq_ctx_new();
    if(context == NULL) {
        cout<<"context == NULL"<<endl;
    }
    void* socket = zmq_socket(context, ZMQ_REP);
    if(socket == NULL) {
        cout<<"socket == NULL"<<endl;
    }
//    cout<<"ip=" <<ip<<endl;
    int r = zmq_bind(socket, ip.c_str());
//    cout<<"zmq_errno() = "<<zmq_errno()<<endl;
    if(context == NULL || socket == NULL || r < 0) {
        cout<<"bind fail ip = "<<ip<<endl;
        zmq_close(socket);
        zmq_ctx_destroy(context);
        return NULL;
    } else {
        cout<<"bind success"<<endl;
    }
    return socket;
}

void* reqListener(string ip) {
    void* context = zmq_ctx_new();
    void* socket = zmq_socket(context, ZMQ_REQ);
    int r = zmq_connect(socket, ip.c_str());
    if(context == NULL || socket == NULL || r < 0) {
        cout<<"connect fail"<<endl;
        return NULL;
    } else {
        cout<<"connect success"<<endl;
    }
    return socket;
}

void bindSW(Node* s, Node* w) {
    s->addLinks(w->getTCP());
    w->addLinks(s->getTCP());
}

void generateNodeShell(Node *node, string args) {
    cout<<node->toString()<<endl;
//    string cmd = "ssh -o StrictHostKeyChecking=no " + node->host + " 'cd /Users/sahara/CLionProjects/ps; ";
    string cmd = "ssh sahara@" + node->host + " && cd /Users/sahara/CLionProjects/ps && ";
    if(node->name[0] == 's' && node->name[1] == 'e') {
        cmd = cmd + "./server " + node->toString();
    } else if(node->name[0] == 'w') {
        cmd = cmd + "./worker " + node->toString();
    }
    else if(node->name[0] == 's'&& node->name[1] == 'c') {
        cmd = cmd + "./scheduler " + node->toString();
    }
    cmd = cmd  + " " + args + " &";
    string file = node->name + args +  "fork.sh";
    cout<<file<<endl;
    cout<<cmd<<endl;
    ofstream out(file,ios::out);
    out<<cmd<<endl;
    out.close();
}

#endif //PS_UTIL_H
