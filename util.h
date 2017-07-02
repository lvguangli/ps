//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_UTIL_H
#define PS_UTIL_H

#include <cstdlib>
#include <zmq.h>
#include <cstring>
#include <string>
#include "constant.h"
#include "message.h"
#include <sys/time.h>

using namespace std;
const int OKMSGLEN = 100;
int iRcvTimeout = 60000;// millsecond

long getCurrentTime()
{
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

void* repListener(string ip) {
    void* context = zmq_ctx_new();
    if(context == NULL) {
        cout<<"context == NULL"<<endl;
    }
    void* socket = zmq_socket(context, ZMQ_REP);
    if(socket == NULL) {
        cout<<"socket == NULL"<<endl;
    }
    int t = zmq_setsockopt(socket, ZMQ_RCVTIMEO, &iRcvTimeout, sizeof(iRcvTimeout));
    int b = zmq_bind(socket, ip.c_str());
//    cout<<"zmq_errno() = "<<zmq_errno()<<endl;
    if(context == NULL || socket == NULL || b < 0 || t < 0) {
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
    int t = zmq_setsockopt(socket, ZMQ_RCVTIMEO, &iRcvTimeout, sizeof(iRcvTimeout));
    int b = zmq_connect(socket, ip.c_str());
    if(context == NULL || socket == NULL || b < 0 || t < 0) {
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
    string cmd = "ssh sahara@" + node->host + " 'cd /Users/sahara/CLionProjects/ps && ";
    if(node->name[0] == 's' && node->name[1] == 'e') {
        cmd = cmd + "./server " + node->toString();
    } else if(node->name[0] == 'w') {
        cmd = cmd + "./worker " + node->toString();
    } else if(node->name[0] == 'n') {
        cmd = cmd + "./newWorker " + node->toString();
    } else if(node->name[0] == 's'&& node->name[1] == 'c') {
        cmd = cmd + "./scheduler " + node->toString();
    }
    cmd = cmd  + " " + args + " &'";
    string file = "/Users/sahara/CLionProjects/ps/bin/" + node->name + args +  "fork.sh";
//    cout<<file<<endl;
    cout<<"generateNodeShell:"<<cmd<<endl;
    ofstream out(file,ios::out);
    out<<cmd<<endl;
    out.close();
}


//int getSocketIndex(void* socket, unordered_map<int,void*>* sockets) {
//    int index = 0;
//    for(;;) {
//        if(socket == (*sockets)[index]) {
//            break;
//        }
//        index ++;
//        if(index > 100) {
//            index = -1;
//            break;
//        }
//    }
//    return index;
//}

//int singleErrorSend(void* socket, string file, int index) {
//    Data error = Data();
//    error.timeStamp = getCurrentTime();
//    error.start = -1;
//    error.end = -1;
//    error.type = ERROR;
//    int i = 0;
//    string str = error.toString();
//    while(zmq_send(socket, str.c_str(), str.size(), 0) < 0) {
//        log("singleErrorSend zmq_send  < 0, try again",file, index);
//        i++;
//        if(i > 3) {
//            return -1;
//        }
//    }
//    return 1;
//}
//
//void* BidirectionalReceiveLink(void * args) {
//    int errorN = 0;
//    LinkMsg* linkMsg = (LinkMsg*) args;
//    void* socket = linkMsg->socket;
//    unordered_map<int,void*>* receiveSockets = linkMsg->sockets;
//    vector<Data*>* receiveMsgList = linkMsg->msgList;
//    vector<int>* receiveMsgArray = linkMsg->msgArray;
//    string file = linkMsg->file;
//    int index = getSocketIndex(socket, receiveSockets);
//    log("BidirectionalReceiveLink",file, index);
//    log("BidirectionalReceiveLink socket="+ to_string((long)socket),file, index);
//    int maxLen = M * N * sizeof(double) + M + 1;
//    log("BidirectionalReceiveLink maxLen = " + to_string(maxLen),file, index);
//    char tmp[M * N * sizeof(double) + M + 1];
//    log("BidirectionalReceiveLink tmp",file, index);
//    if(zmq_recv(socket, tmp, maxLen, 0) < 0) {
//        log("BidirectionalReceiveLink zmq_recv < 0",file, index);
//        if(singleErrorSend(socket, file, index) > 0) {
//            BidirectionalReceiveLink(args);
//        }
//        return NULL;
//    }
//    log("receive msg.size = " + to_string(strlen(tmp)) , file, index);
//    log(tmp,file, index);
//    Data* msg = new Data(tmp);
//    //接收消息并存储然后标记
//    if((*receiveMsgList)[index]->timeStamp == msg->timeStamp) {
//        log("msg has received, just return ok", file, index);
//    } else {
//        (*receiveMsgList)[index] = msg;
//        (*receiveMsgArray)[index] = 1;
//    }
//    log("BidirectionalReceiveLink parse right from " + to_string(index),file, index);
//    log(msg->head(), file, index);
//    Data respon = Data();
//    respon.type = OK;
//    if(msg->type == UNKNOWN) {
//        respon.type = ERROR;
//    }
//    respon.start = -1;
//    respon.end = -1;
//    string str = respon.head();
//    log("respon str.size = " + to_string(str.size()) + " to " + to_string(index), file, index);
//    while(zmq_send(socket, str.c_str(), str.size(), 0) < 0) {
//        log("BidirectionalReceiveLink zmq_send  < 0, try again",file, index);
//    }
//    if((*receiveMsgArray)[index] == 0) {
//        log("BidirectionalReceiveLink (*receiveMsgArray)[index] == 0, try again",file, index);
//        BidirectionalReceiveLink(args);
//    }
//    return NULL;
//}
//
//void* BidirectionalSendLink(void * args) {
//
//    LinkMsg* linkMsg = (LinkMsg*) args;
//    void* socket = linkMsg->socket;
//    unordered_map<int,void*>* sendSockets = linkMsg->sockets;
//    vector<Data*>* sendMsgList = linkMsg->msgList;
//    vector<int>* sendMsgArray = linkMsg->msgArray;
//    string file = linkMsg->file;
//    int index = getSocketIndex(socket, sendSockets);
//    log("BidirectionalSendLink",file, index);
//    log("BidirectionalSendLink socket="+ to_string((long)socket),file, index);
//    Data* request = (*sendMsgList)[index];
//    string str = request->toString();
//    log("BidirectionalSendLink to " + to_string(index) + "str=" + str,file, index);
//    while(zmq_send(socket, str.c_str(), str.size(), 0) < 0) {
//        log("BidirectionalSendLink zmq_send < 0; try again",file, index);
//    }
//    char tmp[OKMSGLEN];
//    if(zmq_recv(socket, tmp, OKMSGLEN, 0) < 0) {
//        log("BidirectionalSendLink zmq_recv < 0",file, index);
//        return NULL;
//    }
//    log("receive from " + to_string(index) + " msg.size = " + to_string(strlen(tmp)) , file, index);
//    Data msg = Data(tmp);
//    if(msg.type != OK) {
//        log("receive wrong msg", file, index);
//        // 重发消息
//        BidirectionalSendLink(args);
//        return NULL;
//    } else {
//        log("reveive msg match so set 1 to index " + to_string(index), file, index);
//        (*sendMsgArray)[index] = 1;
//    }
//    log("BidirectionalSendLink success to " + to_string(index),file, index);
//    return NULL;
//}
//
//
///**
// * 接收消息，如果是pull操作，直接send数据，如果是push操作，则保存数据并结束。
// * @param args
// * @return
// */
//void* safeReceiveSingleSocket(void *args) {
//    LinkMsg* linkMsg = (LinkMsg*) args;
//    void* socket = linkMsg->socket;
//    unordered_map<int,void*>* receiveSockets = linkMsg->sockets;
//    vector<int>* receiveMsgArray = linkMsg->msgArray;
//    string file = linkMsg->file;
//    int index = getSocketIndex(socket, receiveSockets);
//    log("safeReceiveSingleSocket",file, index);
//    log("safeReceiveSingleSocket socket="+ to_string((long)socket),file, index);
//    while((*receiveMsgArray)[index] == 0) {
//        pthread_t id;
//        pthread_create(&id, NULL, BidirectionalReceiveLink, args);
////        void*a;
//        pthread_join(id,NULL);
//        log("safeReceiveSingleSocket safeReceiveSingleSocket &&&&&&&&&&&&&&&&&", file, index);
//        if((*receiveMsgArray)[index] == 0)
//            log("safeReceiveSingleSocket BidirectionalReceiveLink maybe crash ", file, index);
//    }
//    log("safeReceiveSingleSocket (*receiveMsgArray)[index] " + to_string((*receiveMsgArray)[index]), file, index);
//    log("safeReceiveSingleSocket receive from " + to_string(index), file, index);
//    return NULL;
//}
//
///**
// * 根据参数套接字接口发送数据
// * @param args socket id
// * @return
// */
//void* safeSendSingleSocket(void* args) {
//
//    LinkMsg* linkMsg = (LinkMsg*) args;
//    void* socket = linkMsg->socket;
//    unordered_map<int,void*>* sendSockets = linkMsg->sockets;
//    vector<int>* sendMsgArray = linkMsg->msgArray;
//    string file = linkMsg->file;
//    int index = getSocketIndex(socket, sendSockets);
//    log("safeSendSingleSocket",file, index);
//    log("safeSendSingleSocket socket="+ to_string((long)socket),file, index);
//
//    while((*sendMsgArray)[index] == 0) {
//        pthread_t id;
//        pthread_create(&id, NULL, BidirectionalSendLink, args);
//        pthread_join(id,NULL);
//    }
//    log("safeSendSingleSocket send to " + to_string(index), file, index);
//    return NULL;
//}
//
//void receiveAllSockets(void* args, int mainId) {
//    LinkMsg* linkMsg = (LinkMsg*) args;
//    unordered_map<int,void*>* receiveSockets = linkMsg->sockets;
//    vector<int>* receiveMsgArray = linkMsg->msgArray;
//    vector<Data*>* receiveMsgList = linkMsg->msgList;
//    string file = linkMsg->file;
//    log("receiveAllSockets",file, mainId);
//    log("receiveAllSockets#######################",file, 0);
//    log("receiveAllSockets#######################",file, 1);
//    log("receiveAllSockets#######################",file, 0);
//    log("receiveAllSockets#######################",file, 1);
//    for(int i = 0; i< receiveMsgArray->size(); i++) {
//        (*receiveMsgArray)[i] = 0;
//    }
//    vector<pthread_t*> threads;
//    for(int i = 0; i< receiveSockets->size(); i++) {
////        if((*receiveMsgArray)[i] == 1) {
////            continue;
////        }
//        pthread_t id;
//        void* receiveSocket = (*receiveSockets)[i];
//        LinkMsg* tmp =new LinkMsg(linkMsg);
//        tmp->socket = receiveSocket;
//        log("receiveAllSockets socket=" + to_string((long) receiveSocket),file, mainId);
//        pthread_create(&id, NULL, safeReceiveSingleSocket, tmp);
//        threads.push_back(&id);
//    }
//    for(int i =0; i < receiveSockets->size(); i++) {
////        void*a;
//        pthread_join(*(threads[i]),NULL);
////        log("receiveAllSockets a = " + to_string(*(int*)a), file, mainId);
//    }
////    threads.clear();
//    log("receiveAllSockets finished , then check",file, mainId);
//    for(int i = 0; i< receiveSockets->size(); i++) {
//        if((*receiveMsgArray)[i] == 0) {
//            log("safeReceiveSingleSocket func has bug + i = " + to_string(i), file, mainId);
//            log("*receiveMsgArray)[i] = " +to_string((*receiveMsgArray)[i]), file, mainId);
//            log("msg i = " + to_string(i) + " msg=" + (*receiveMsgList)[i]->toString(), file, mainId);
////            pthread_t id;
////            void* receiveSocket = (*receiveSockets)[i];
////            LinkMsg* tmp =new LinkMsg(linkMsg);
////            tmp->socket = receiveSocket;
////            log("receiveAllSockets try just once",file, mainId);
////            pthread_create(&id, NULL, safeReceiveSingleSocket, tmp);
////            pthread_join(id,NULL);
////            i=-1;
//        }
//    }
//
//    log("receiveAllSockets finished , check finished",file, mainId);
//
//}
//
//void sendAllSockets(void* args, int mainId) {
//    LinkMsg* linkMsg = (LinkMsg*) args;
//    unordered_map<int,void*>* sendSockets = linkMsg->sockets;
//    vector<int>* sendMsgArray = linkMsg->msgArray;
//    vector<Data*>* sendMsgList = linkMsg->msgList;
//    string file = linkMsg->file;
//    log("sendAllSockets",file, mainId);
//    log("sendAllSockets##########################",file, 0);
//    log("sendAllSockets##########################",file, 1);
//    log("sendAllSockets##########################",file, 0);
//    log("sendAllSockets##########################",file, 1);
//    vector<pthread_t*> threads;
//    for(int i = 0; i< sendSockets->size(); i++) {
//        pthread_t id;
//        void* sendSocket = (*sendSockets)[i];
//        LinkMsg* tmp =new LinkMsg(linkMsg);
//        tmp->socket = sendSocket;
//        log("sendAllSockets socket=" + to_string((long) sendSocket),file, mainId);
//        pthread_create(&id, NULL, safeSendSingleSocket, tmp);
//        threads.push_back(&id);
//    }
//    for(int i =0; i < threads.size(); i++) {
//        pthread_join(*(threads[i]),NULL);
//    }
//    threads.clear();
//    for(int i = 0; i< sendMsgArray->size(); i++) {
//        (*sendMsgArray)[i] = 0;
//    }
//}






#endif //PS_UTIL_H
