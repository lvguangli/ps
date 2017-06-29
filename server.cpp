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

unordered_map<int,void*>* receiveSockets;
vector<Data*>* receiveMsgList;
vector<int>*  receiveMsgArray;
LinkMsg* receiveLinkMsg;

unordered_map<int,void*>* sendSockets;
vector<Data*>* sendMsgList;
vector<int>*  sendMsgArray;
LinkMsg* sendLinkMsg;

bool isAddNode = false;
int mainId = 6;

void initLocalData(Node* node) {
    log("initLocalData",file, mainId);
    error = new Data();
    error->start = 0;
    error->end = M;
    error->initData(error->end - error->start,1);
    data = new Data();
    data->start = 0;
    data->end = M;
    weight = new Data();
    weight->type = PUSH;
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
    log("initTrainData",file, mainId);
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

void initSocketsAndLinkMsg(void* args) {
    log("initSockets",file, mainId);
    Node* node =(Node*)args;

    receiveSockets = new unordered_map<int,void*>();
    receiveMsgList = new vector<Data*>();
    receiveMsgArray = new vector<int>();
    receiveLinkMsg = new LinkMsg();

    sendSockets = new unordered_map<int,void*>();;
    sendMsgList = new vector<Data*>();
    sendMsgArray = new vector<int>();
    sendLinkMsg = new LinkMsg();


    for(int i = 0; i < node->workerNum; i++) {
        string ip = node->getTCP() + to_string(i);
        void* receiveSocket = repListener(ip);
        if(receiveSocket != NULL) {
            log("server bind to ip = " + ip, file, mainId);
        }
        else {
            log("server bind fail to ip = " + ip, file, mainId);
        }
        (*receiveSockets)[i] = receiveSocket;
    }
    for(int i = 0; i < node->workerNum; i++) {
        string ip = node->linksIP[i] + to_string(node->id);
        void* sendSocket = reqListener(ip);
        if(sendSocket != NULL) {
            log("server connect to ip = " + ip, file, mainId);
        }
        else {
            log("server connect fail to ip = " + ip, file, mainId);
        }
        (*sendSockets)[i] = sendSocket;
    }

    Data nullMsg = Data();
    nullMsg.type = PULL;
    nullMsg.timeStamp = getCurrentTime();
    nullMsg.start = -1;
    nullMsg.end = -1;
    for(int i =0; i < node->workerNum; i++) {
        receiveMsgArray->push_back(0);
        receiveMsgList->push_back(&nullMsg);
        sendMsgArray->push_back(0);
        sendMsgList->push_back(&nullMsg);
    }

    sendLinkMsg->msgList = sendMsgList;
    sendLinkMsg->msgArray = sendMsgArray;
    sendLinkMsg->sockets = sendSockets;
    sendLinkMsg->file = file;
    receiveLinkMsg->msgList = receiveMsgList;
    receiveLinkMsg->msgArray = receiveMsgArray;
    receiveLinkMsg->sockets = receiveSockets;
    receiveLinkMsg->file = file;
}

void addSocket(void* args) {
    log("addSocket",file, mainId);
    Node* node =(Node*)args;
    int workerId = node->workerNum - 1;
    string ip = node->getTCP() + to_string(workerId);
    log("server bind to ip = " + ip, file, mainId);
    void* receiveSocket = repListener(ip);
    (*receiveSockets)[workerId] = receiveSocket;

    ip = node->linksIP[workerId] + to_string(node->id);
    log("server connect to ip = " + ip, file, mainId);
    void* sendSocket = reqListener(ip);
    (*sendSockets)[workerId] = sendSocket;
    receiveMsgArray->push_back(0);
    sendMsgArray->push_back(0);
}

/**
 * 计算权重
 */
void calc() {
    log("calc",file, mainId);
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
    log("calc weight finished",file, mainId);
}

Data* generatePullData(Data *msg, Data* have) {
    Data* res;
    if(msg->start == have->start && msg->end == have->end) {
        res = have;
        res->type = PULL;
        res->timeStamp = getCurrentTime();
        return res;
    }
    res = new Data();
    if(msg->start >= have->end || msg->end <= have->start) {
        res->start = -1;
        res->end = -1;
    }
    int start = msg->start;
    for(; start < msg->end; start++) {
        if(start < have->start)
            continue;
        else
            break;
    }
    res->start = start;
    int end = msg->end;
    if(end > have->end)
        end = have->end;
    res->end = end;
    res->initData(res->end - res->start);
    for(; start < end; start++) {
        res->data[start - res->start] = have->data[start - have->start];
    }
    res->type = PULL;
    res->timeStamp = getCurrentTime();
    return res;
}

void handlePULL(void* args) {
    log("handlePULL",file, mainId);
//    Node* node =(Node*)args;
    receiveAllSockets(receiveLinkMsg, mainId);
    for(int i = 0; i < receiveSockets->size(); i++) {
        if((*receiveMsgList)[i]->type != PULL) {
            log("receive pull error", file, mainId);
            exit(1);
        }
        Data* msg = (*receiveMsgList)[i];
        (*sendMsgList)[i] = generatePullData(msg, data);
        (*sendMsgArray)[i] = 0;
    }
    sendAllSockets(sendLinkMsg, mainId);
}

void handlePUSH(void* args) {
    log("handlePUSH",file, mainId);
    //    Node* node =(Node*)args;
    receiveAllSockets(receiveLinkMsg, mainId);
    for(int i = 0; i < receiveSockets->size(); i++) {
        if((*receiveMsgList)[i]->type != PUSH) {
            log("receive push error", file, mainId);
            exit(1);
        }
        Data* msg = (*receiveMsgList)[i];
        error->addData(msg->start, msg->end, msg->data);
    }
    calc();
    weight->timeStamp = getCurrentTime();
    weight->type = PUSH;
    for(int i=0; i < receiveSockets->size(); i++) {
        (*sendMsgList)[i]= weight;
        (*sendMsgArray)[i] = 0;
    }
    sendAllSockets(sendLinkMsg, mainId);

}


void interactWithWorker(void* args) {
    log("interactWithWorker",file, mainId);
//    Node* node =(Node*)args;
    handlePULL(args);
    for(int k = 0; k < ITERATOR * 2; k++) {
        handlePUSH(args);
        hasItered++;
        log("interactWithWorker hasItered = " + to_string(hasItered),file, mainId);
        if(hasItered > ITERATOR) {
            break;
        }
//        // 用来测试添加节点消息
//        while(hasItered == ITERATOR/2 && !isAddNode) {
//            sleep(2);
//        }
        if(isAddNode) {
            addSocket(args);
            isAddNode = false;
            handlePULL(args);
        }
    }
}

void* listenScheduler(void* args) {
    log("listenScheduler",file, mainId);
//    return NULL;
    Node* node =(Node*)args;
    string ip = node->getTCP() + to_string(node->scheduler->id);
    log("server bind to ip = " + ip, file, mainId);
    void* receiveSocket = repListener(ip);

    ip = node->scheduler->getTCP() + to_string(node->id);
    log("server connect to ip = " + ip, file, mainId);
    void* sendSocket = reqListener(ip);

    void* socket = repListener(ip);
    int MAXLEN = 500 * (8 + 1);
    char tmp[500 * (sizeof(double) + 1)];
    zmq_recv(socket, tmp, MAXLEN, 0);
    log("receive msg from scheduler size =  " + to_string(strlen(tmp)),file, mainId);
    log(tmp, file, mainId);
    Data msg = Data(tmp);
    if(msg.type == ADDNODE) {
        node->workerNum += 1;
        node->addLinks("tcp://" + whosts[msg.end][0] + ":" + whosts[msg.end][1]);
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
    log("start_server",file, mainId);
    Node* node =(Node*)args;
    // start_server 起一个不需要等待的线程用来处理scheduler的消息
//    pthread_t id1;
//    pthread_create(&id1, NULL, listenScheduler, (void*)node);
    initLocalData(node);
    initTrainData("train.txt");
    initSocketsAndLinkMsg(args);
    interactWithWorker(args);
    log("start_server finished", file, mainId);
    return NULL;
}



int main(int argc, char *argv[]) {
    getCurrentTime();
    Node* server = new Node(argv[1]);
    for(int i = 6; i < argc; i++) {
        server->addLinks(argv[i]);
    }
    mainId += server->id;
    file = file + to_string(server->id);
    log(file, mainId);
    log("start server", file, mainId);
    log(server->toString(), file, mainId);
    pthread_t id;
    pthread_create(&id, NULL, start_server, (void*)server);
    pthread_join(id,NULL);
    log("server "+ to_string(server->id) + " finished", file, mainId);
}