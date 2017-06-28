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
#include "log.h"
#include "constant.h"
#include <unistd.h>
#include <stdlib.h>

using namespace std;

unordered_map<int,void*> sockets;

string file = "worker";
// 只计算自己worker分配的error部分 = N / workerN
Data* error;
// 保留全部权重特征，即需要向server拉取所有weight
Data* weight;
// 定义一个用来存放server拉取部分weight的对象指针
//Weight* wFromServer;

Data* data;

int WORKERNUM = workerN;
int SERVERNUM = serverN;
int hasItered = 0;
int addFlag = false;
/**
 * 从server拉取data数据，并更新其他标记
 * @param args  socket
 * @return
 */

void pullFromIP(void* args){
    void* socket = args;
    int index = 0;
    for(;;) {
        if(socket == sockets[index]) {
            break;
        }
        index ++;
    }
    Data msg = Data();
    msg.type = PULL;
    msg.start = data->start;
    msg.end = data->end;
    log("send PULL to server" + to_string(index), file);
    string str = msg.toString();
    log("send msg =  to server" + to_string(index) +" "+ str, file);
    log("send  to server" + to_string(index) +" msg.size = "+ to_string(str.size()), file);
    zmq_send(socket, str.c_str(), str.size(), 0);
    log("send to server" + to_string(index) + "done", file);
    char tmp[M*N*(8 + 1)];
//    log("tmp for receive size = " + to_string(M*N*(8 + 1))   + " from server" + to_string(index), file);
    zmq_recv(socket, tmp, M*N*(8 + 1), 0);
    log("tmp for receive size = " + to_string(strlen(tmp))   + " from server" + to_string(index), file);
    int len = strlen(tmp);
    log("res from" + to_string(index) + " len = " + to_string(len)  + " from server" + to_string(index),file);
    log(tmp, file);
    Data tmpd = Data(tmp, file);
    log("parse right from server" + to_string(index), file);
    if(tmpd.start != -1) {
        data->addData(tmpd.start, tmpd.end, tmpd.data);
        log("server "+ to_string(index) + " has pulled right data", file);
    }
    else {
        log("server "+ to_string(index) + " has no data for this node", file);
    }
    log("pull done", file);
}

/**
 * 从单个server获取data
 * @param args  server index
 * @return
 */
void* pull(void * args) {
    pullFromIP(args);
    return NULL;
}
/**
 * 单次将error push 到 server,并接收weight
 * @param args server index
 * @return
 */
void* push(void* args) {
    void* socket = args;
    int index = 0;
    for(;;) {
        if(socket == sockets[index]) {
            break;
        }
        index ++;
    }
    error->type = PUSH;

    string str = error->toString();
    log("hasItered = " + to_string(hasItered) + " send PUSH to server " + to_string(index) + "error.size " + to_string(str.size()) , file);
    zmq_send(socket, str.c_str(), str.size(), 0);
    char tmp[500 * (sizeof(double) + 1)];
    zmq_recv(socket, tmp, 500 * (sizeof(double) + 1), 0);
    string s = tmp;
    log("receive push from server " + to_string(index) + " size =  " + to_string(s.size()),file);
    Data msg = Data(tmp);
    weight->addData(msg.start, msg.end, msg.data);
    return NULL;
}
void getError(Node* node, int index) {
    error = new Data();
    error->start = (M / WORKERNUM + 1) * index;
    error->end = (M / WORKERNUM + 1) * (index + 1);
    if(error->end > M) {
        error->end = M;
    }
    error->initData(error->end - error->start, 1);

    data = new Data();
    data->start = (M / WORKERNUM + 1) * index;
    data->end = (M / WORKERNUM + 1) * (index + 1);
    if(data->end > M) {
        data->end = M;
    }
    data->initData(data->end - data->start);
}
void getWeight(Node* node, int index) {
    weight = new Data();
    weight->start = 0;
    weight->end = N;
    weight->initData(N, 1);
    // 每个server存放的weight只会小于定义的对象的大小
//    wFromServer = new Weight();
//    wFromServer->start = (N / SERVERNUM + 1) * node->id;
//    wFromServer->end = (N / SERVERNUM + 1) * (node->id + 1);
//    wFromServer->initWeight(error->end - error->start);

}

void calc(){

}
void* start_worker(void* args) {
    Node* node  = (Node*)args;
    getError(node, node->id);
    getWeight(node, node->id);
    for(int i = 0; i < node->linksNum; i++) {
        string ip = node->linksIP[i] + to_string(node->id);
        log("connect to ip = " + ip, file);
        void* socket = reqListener(ip);
        log("socket " + to_string((unsigned long)socket), file);
        sockets[i] = socket;
    }

    vector<pthread_t*> threads;
    for(int i =0; i < node->linksNum; i++) {
//        pthread_t id;
        void* socket = sockets[i];
//        pthread_create(&id,NULL,pull, socket);
//        threads.push_back(&id);
        pull(socket);
    }
//    for(int i =0; i < node->linksNum; i++) {
//        pthread_join(*(threads[i]),NULL);
//    }
    log("check data : " + data->toString(), file);
//    threads.clear();
    log("pull finished", file);
    for(int k =0; k < ITERATOR * 2; k++) {
        calc();
        for(int i = 0; i < node->linksNum; i++) {
            pthread_t id;
            void* socket = sockets[i];
            pthread_create(&id,NULL,push, socket);
            threads.push_back(&id);
        }
        for(int i =0; i < threads.size(); i++) {
            pthread_join(*(threads[i]),NULL);
        }
        hasItered ++;
        threads.clear();
        if(hasItered >= ITERATOR) {
            break;
        }
    }
    return NULL;
}



int main(int argc, char *argv[]) {

    Node* node = new Node(argv[1], atoi(argv[2]), argv[3], argv[4], argv[5]);
    for(int i = 6; i < argc; i++) {
        node->addLinks(argv[i]);
    }

    file = file + to_string(node->id) + ".txt";
    cout<<file<<endl;
    log(file);
    log("start worker", file);
//    if(node->id == 0) {
//        log("node->linksNum" + to_string(node->linksNum),file);
//        node->linksNum = 1;
//    }
//    for(int i =0; i< argc; i++) {
//        string s = argv[i];
//        log("i = " + to_string(i) + "argv = " + s, file);
//    }
    pthread_t id;
    pthread_create(&id, NULL, start_worker, (void*)node);
//    start_worker((void*) node);
    pthread_join(id,NULL);
    log("work "+ to_string(node->id) + " finished", file);
//    sleep(10);
    // 将结果保存
}