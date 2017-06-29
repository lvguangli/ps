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
#include <cmath>

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

int hasItered = 0;
bool isAddNode = false;

/**
 * 从单个server获取data
 * @param args  server index
 * @return
 */
void* pull(void * args) {
    log("pull",file);
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
    string str = msg.toString();
    log("send  to server" + to_string(index) +" msg.size = "+ to_string(str.size()) + " msg = " + msg.head(), file);
    zmq_send(socket, str.c_str(), str.size(), 0);
    char tmp[M*N*(8 + 1)];
    zmq_recv(socket, tmp, M*N*(8 + 1), 0);
    int len = strlen(tmp);
    log("tmp for receive size = " + to_string(strlen(tmp))   + " from server" + to_string(index), file);
    Data result = Data(tmp);
    if(strlen(tmp) > len) {
        log("tmp for receive contain char can't understand" , file);
    }
    log("parse right from server" + to_string(index), file);
    if(result.start != -1) {
        data->addData(result.start, result.end, result.data);
        log("server "+ to_string(index) + " has pulled right data", file);
    }
    else {
        log("server "+ to_string(index) + " has no data for this node", file);
    }
    return NULL;
}
/**
 * 单次将error push 到 server,并接收weight
 * @param args server index
 * @return
 */
void* push(void* args) {
    log("push",file);
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
    log("hasItered = " + to_string(hasItered) + " send PUSH to server " + to_string(index) + " error.size " + to_string(str.size()) , file);
    zmq_send(socket, str.c_str(), str.size(), 0);
    int MAXLEN = 500 * (8 + 1);
    char tmp[500 * (sizeof(double) + 1)];
    zmq_recv(socket, tmp, MAXLEN, 0);
    log("receive push from server " + to_string(index) + " size =  " + to_string(strlen(tmp)),file);
    log(tmp, file);
    Data msg = Data(tmp);
    weight->addData(msg.start, msg.end, msg.data);
    return NULL;
}

void getDataWithWorkerNum(Node* node, int index) {
    log("getDataWithWorkerNum",file);
    error = new Data();
    error->start = (M / node->workerNum + 1) * index;
    error->end = (M / node->workerNum + 1) * (index + 1);
    if(error->end > M) {
        error->end = M;
    }
    error->initData(error->end - error->start, 1);

    data = new Data();
    data->start = (M / node->workerNum + 1) * index;
    data->end = (M / node->workerNum + 1) * (index + 1);
    if(data->end > M) {
        data->end = M;
    }
    data->initData(data->end - data->start);
}

void getDataWithOutWorkerNum(Node* node, int index) {
    log("getDataWithOutWorkerNum",file);
    weight = new Data();
    weight->start = 0;
    weight->end = (N-1);
    weight->initData((N-1), 1);
}

void calc(){
    log("calc",file);
    int count = error->end - error->start;
    for(int i = 0; i < count; i++) {
        double result = 0;
        for(int j = 0; j< N - 1; j++) {
            result += weight->data[j][0] * data->data[i][j];
        }
        error->data[i][0] = data->data[i][N-1] - 1.0 / (1 + exp(-result));
    }
}

void initLocalData(void* args) {
    log("initLocalData",file);
    Node* node  = (Node*)args;
    if(isAddNode) {
        getDataWithWorkerNum(node, node->id);
    }
    else {
        getDataWithWorkerNum(node, node->id);
        getDataWithOutWorkerNum(node, node->id);
    }
}

void initSockets(void* args) {
    log("initSockets",file);
    Node* node  = (Node*)args;
    for(int i = 0; i < node->serverNum; i++) {
        string ip = node->linksIP[i] + to_string(node->id);
        log("connect to ip = " + ip, file);
        void* socket = reqListener(ip);
        log("socket " + to_string((unsigned long)socket), file);
        sockets[i] = socket;
    }
}

void pullData(void* args) {
    log("pullData",file);
    Node* node  = (Node*)args;
    for(int i =0; i < node->serverNum; i++) {
        void* socket = sockets[i];
        pull(socket);
    }
}

void pushData(void* args) {
    log("pushData",file);
    Node* node  = (Node*)args;
    for(int k =0; k < ITERATOR * 2; k++) {
        calc();
        for(int i = 0; i < node->serverNum; i++) {
            void* socket = sockets[i];
            push(socket);
        }
        hasItered ++;
        if(hasItered > ITERATOR) {
            break;
        }
        if(isAddNode) {
            initLocalData(args);
            //todo 可以优化
            pullData(args);
            isAddNode = false;
        }
    }
}

void* listenScheduler(void* args) {
    log("listenScheduler",file);
    return NULL;
    Node* node =(Node*)args;
    string ip = node->getTCP() + to_string(node->scheduler->id);
    log("worker bind to ip = " + ip, file);
    void* socket = repListener(ip);
    int MAXLEN = 500 * (8 + 1);
    char tmp[500 * (sizeof(double) + 1)];
    zmq_recv(socket, tmp, MAXLEN, 0);
    log("receive msg from scheduler size =  " + to_string(strlen(tmp)),file);
    log(tmp, file);
    Data msg = Data(tmp);
    if(msg.type == ADDNODE) {
        node->workerNum += 1;
        isAddNode = true;
    } else if(msg.type == STOP) {
        exit(0);
    }
    log("receive msg from scheduler finished",file);
    return NULL;
}

void* start_worker(void* args) {
    log("start_worker",file);
    Node* node =(Node*)args;
    // start_worker 起一个不需要等待的线程用来处理scheduler的消息
    pthread_t id1;
    pthread_create(&id1, NULL, listenScheduler, (void*)node);
    initLocalData(args);
    initSockets(args);
    pullData(args);
    pushData(args);
    log("func start_worker finished", file);
    return NULL;
}



int main(int argc, char *argv[]) {
    Node* worker = new Node(argv[1]);
    for(int i = 6; i < argc; i++) {
        worker->addLinks(argv[i]);
    }

    file = file + to_string(worker->id) + ".txt";
    cout<<file<<endl;
    log(file);
    log("start worker", file);
    log("worker->workerNum == worker->serverNum = " + to_string(worker->workerNum == worker->serverNum), file);
    pthread_t id;
    pthread_create(&id, NULL, start_worker, (void*)worker);
    pthread_join(id,NULL);
    log("work "+ to_string(worker->id) + " finished", file);
//    sleep(10);
    // 将结果保存
}