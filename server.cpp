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
double alpha = 0.01;
string file = "server";

int hasItered = 0;
// 保存全部error
Data* error;
// 只保留和自己server计算相关的weight
Data* weight;
// 只保存和自己相关的data
Data* data;

vector<Data> msgList;

bool isAddNode = false;

unordered_map<int,void*> sockets;

int getSocketIndex(void* socket) {
    log("getSocketIndex",file);
    int index = 0;
    for(;;) {
        if(socket == sockets[index]) {
            break;
        }
        index ++;
    }
    return index;
}

void initLocalData(Node* node) {
    log("initLocalData",file);
    error = new Data();
    error->start = 0;
    error->end = M;
    error->initData(error->end - error->start,1);
    data = new Data();
    data->start = 0;
    data->end = M;
    weight = new Data();
    weight->start = ((N-1) / node->serverNum + 1) * node->id;
    weight->end = ((N-1) / node->serverNum + 1) * (node->id + 1);
    if(weight->end > (N-1)) {
        weight->end = (N-1);
    }
    weight->initData(weight->end - weight->start, 1);

}

/**
 * 加载数据
 * @param train
 */
void initTrainData(string train) {
    log("initTrainData",file);
    ifstream input(train);
    char line[1200];
    while(input.getline(line, 1200)) {
        vector<double> vector2;
        char word[10];
        int index = 0;
        int cur = 0;
        while(line[cur]) {
            if(line[cur] == ' ') {
                word[index] = '\0';
//                log("init word " + to_string(atof(word)), file);
                vector2.push_back(atof(word));
                index = 0;
            } else {
                word[index] = line[cur];
                index++;
            }
            cur++;
        }
        word[index] = '\0';
        vector2.push_back(atof(word));

        data->data.push_back(vector2);
    }
    input.close();

}

void initSockets(void* args) {
    log("initSockets",file);
    Node* node =(Node*)args;
    for(int i = 0; i< node->workerNum; i++) {
        string ip = node->getTCP() + to_string(i);
        log("server bind to ip = " + ip, file);
        void* socket = repListener(ip);
        sockets[i] = socket;
    }
}

void addSocket(void* args) {
    log("addSocket",file);
    Node* node =(Node*)args;
    int workerId = node->workerNum - 1;
    string ip = node->getTCP() + to_string(workerId);
    log("server bind to ip = " + ip, file);
    void* socket = repListener(ip);
    sockets[workerId] = socket;
}

/**
 * 只保存push上来的error，push操作需要同步，所以只保存数据，然后等待所有节点全部完成再发送数据
 * @param socket
 * @param msg
 */
void responsePUSH(void* socket, Data* msg) {
    log("responsePUSH",file);
    error->addData(msg->start, msg->end, msg->data);
    log("responsePUSH save data", file);
}


/**
 * 根据msg来判断send的数据范围，have是自己拥有的数据
 * @param index
 * @param msg
 * @param have
 */
void sendData(void* socket, Data *msg, Data *have) {
    log("sendData",file);
    int index = getSocketIndex(socket);
    if(msg->start == have->start && msg->end == have->end) {
        have->type = msg->type;
        string str = have->toString();
        log("response to worker " + to_string(index) + " str.size = " + to_string(str.size()), file);
        zmq_send(socket, str.c_str(), str.size(), 0);
        return ;
    }
    Data res = Data();
    res.type = PULL;
    if(msg->start >= have->end || msg->end <= have->start) {
        res.start = -1;
        res.end = -1;
        string str = res.toString();
        log("response str.size = " + to_string(str.size()) + " to worker " + to_string(index), file);
        zmq_send(socket, str.c_str(), str.size(), 0);
        return ;
    }
    int start = msg->start;
    for(; start < msg->end; start++) {
        if(start < have->start)
            continue;
        else
            break;
    }
    res.start = start;
    int end = msg->end;
    if(end > have->end)
        end = have->end;
    res.end = end;
    res.initData(res.end - res.start);
    for(; start < end; start++) {
        res.data[start - res.start] = have->data[start - have->start];
    }
    string str = res.toString();
    log("response str.size = " + to_string(str.size()) + " to worker " + to_string(index), file);
    zmq_send(socket, str.c_str(), str.size(), 0);

}

/**
 * PULL操作可以直接响应，不需要等待
 * @param socket
 * @param msg
 */
void responsePULL(void* socket, Data* msg) {
    log("responsePULL",file);
    sendData(socket, msg, data);
}

/**
 * 接收消息，如果是pull操作，直接send数据，如果是push操作，则保存数据并结束。
 * @param args
 * @return
 */
void* receive(void *args) {
    log("receive",file);
    void* socket = args;
    int index = getSocketIndex(socket);
    for(int i=0;i< ITERATOR * 2;i++) {
        log("hasItered = " + to_string(hasItered) + "try to receive msg from worker" + to_string(index), file);
        int maxLen = M * sizeof(double) + M;
        char tmp[M * sizeof(double) +M];
        zmq_recv(socket, tmp, maxLen, 0);
        log("receive from worker" + to_string(index) + " msg.size = " + to_string(strlen(tmp)) , file);
        Data msg = Data(tmp);
        log("parse right from worker" + to_string(index),file);
        log(msg.head(), file);
        if (msg.type == OK) {
            log("receive OK from worker" + to_string(index), file);
        } else if (msg.type == ADDNODE) {
            log("receive ADDNODE from worker" + to_string(index), file);
        } else if (msg.type == PUSH) {
            log("receive PUSH from worker" + to_string(index), file);
            responsePUSH(socket, &msg);
            break;
        } else if (msg.type == PULL) {
            log("receive PULL from worker"  + to_string(index), file);
            responsePULL(socket, &msg);
        } else if (msg.type == STOP) {
            log("receive STOP  from worker" + to_string(index), file);
        } else {
            log("error msg->type from worker" + to_string(index), file);
        }
    }
    return NULL;
}

/**
 * 根据参数套接字接口发送数据
 * @param args socket id
 * @return
 */
void* send(void* args) {
    log("send",file);
    void* socket = args;
    Data msg = Data();
    msg.start = weight->start;
    msg.end = weight->end;
    sendData(socket, &msg, weight);
    return NULL;
}

/**
 * 计算权重
 */
void calc() {
    log("calc",file);
    int count = weight->end - weight->start;
//    log("weight->start = " + to_string(weight->start), file);
    for(int i = 0; i < count; i++) {
//        log("calc weight i " + to_string(i),file);
        double result = 0;
        for(int j = 0; j< M; j++) {
//            log("calc weight j " + to_string(j),file);
//            log("error->data[j][0] = " + to_string(error->data[j][0]), file);
//            log("data->data[j][i + weight->start] = " + to_string(data->data[j][i + weight->start]), file);
            result += error->data[j][0] * data->data[j][i + weight->start];
        }
        weight->data[i][0] += result * alpha;
    }
    log("calc weight finished",file);
}

void interactWithWorker(void* args) {
    log("interactWithWorker",file);
    for(int k = 0; k < ITERATOR * 2; k++) {
        vector<pthread_t*> threads;
        for(int i = 0; i< sockets.size(); i++) {
            pthread_t id;
            void* socket = sockets[i];
            pthread_create(&id, NULL, receive, socket);
            threads.push_back(&id);
        }
        for(int i =0; i < sockets.size(); i++) {
            pthread_join(*(threads[i]),NULL);
        }
        threads.clear();
        calc();
        for(int i = 0; i< sockets.size(); i++) {
            pthread_t id;
            void* socket = sockets[i];
            pthread_create(&id, NULL, send, socket);
            threads.push_back(&id);
        }
        for(int i =0; i < sockets.size(); i++) {
            pthread_join(*(threads[i]),NULL);
        }
        hasItered++;
        threads.clear();
        if(hasItered > ITERATOR) {
            break;
        }
        if(isAddNode) {
            addSocket(args);
            isAddNode = false;
        }
    }
}

void* listenScheduler(void* args) {
    log("listenScheduler",file);
    return NULL;
    Node* node =(Node*)args;
    string ip = node->getTCP() + to_string(node->scheduler->id);
    log("server bind to ip = " + ip, file);
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
    return NULL;
}


/**
 *
 * @param args  node
 * @return
 */
void* start_server(void* args) {
    log("start_server",file);
    Node* node =(Node*)args;
    // start_server 起一个不需要等待的线程用来处理scheduler的消息
    pthread_t id1;
    pthread_create(&id1, NULL, listenScheduler, (void*)node);
    initLocalData(node);
    initTrainData("train.txt");
    initSockets(args);
    interactWithWorker(args);
    log("start_server finished", file);
    return NULL;
}



int main(int argc, char *argv[]) {

    Node* server = new Node(argv[1]);
    for(int i = 6; i < argc; i++) {
        server->addLinks(argv[i]);
    }
    file = file + to_string(server->id) + ".txt";
    log(file);
    log("start server", file);
    log(server->toString(), file);
    pthread_t id;
    pthread_create(&id, NULL, start_server, (void*)server);
    pthread_join(id,NULL);
    log("server "+ to_string(server->id) + " finished", file);
}