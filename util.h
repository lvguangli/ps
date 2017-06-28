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
        cout<<"bind fail"<<endl;
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
#endif //PS_UTIL_H
