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

string file = "worker";
// 只计算自己worker分配的error部分 = N / workerN
Data* error;
// 保留全部权重特征，即需要向server拉取所有weight
Data* weight;
// 定义一个用来存放server拉取部分weight的对象指针
//Weight* wFromServer;

Data* data;

int hasItered = 0;

int mainId = 6;

unordered_map<int,void*>* receiveSockets;
vector<Data*>* receiveMsgList;
vector<int>*  receiveMsgArray;
LinkMsg* receiveLinkMsg;

unordered_map<int,void*>* sendSockets;
vector<Data*>* sendMsgList;
vector<int>*  sendMsgArray;
LinkMsg* sendLinkMsg;

bool isAddNode = false;

void getDataWithWorkerNum(Node* node, int index) {
    log("getDataWithWorkerNum",file, mainId);
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
    log("getDataWithOutWorkerNum",file, mainId);
    weight = new Data();
    weight->start = 0;
    weight->end = (N-1);
    weight->initData((N-1), 1);
}

void calc(){
    log("calc",file, mainId);
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
    log("initLocalData",file, mainId);
    Node* node  = (Node*)args;
    getDataWithWorkerNum(node, node->id);
    getDataWithOutWorkerNum(node, node->id);
}

void initSocketsAndLinkMsg(void* args) {
    log("initSocketsAndLinkMsg",file, mainId);
    Node* node =(Node*)args;
    receiveSockets = new unordered_map<int,void*>();
    receiveMsgList = new vector<Data*>();
    receiveMsgArray = new vector<int>();
    receiveLinkMsg = new LinkMsg();

    sendSockets = new unordered_map<int,void*>();;
    sendMsgList = new vector<Data*>();
    sendMsgArray = new vector<int>();
    sendLinkMsg = new LinkMsg();
    for(int i = 0; i < node->serverNum; i++) {
        string ip = node->getTCP() + to_string(i);
        void* receiveSocket = repListener(ip);
        if(receiveSocket != NULL) {
            log("worker bind to ip = " + ip, file, mainId);
        }
        else {
            log("worker bind fail to ip = " + ip, file, mainId);
        }
        (*receiveSockets)[i] = receiveSocket;
    }
    for(int i = 0; i < node->serverNum; i++) {
        string ip = node->linksIP[i] + to_string(node->id);
        void* sendSocket = reqListener(ip);
        if(sendSocket != NULL) {
            log("worker connect to ip = " + ip, file, mainId);
        }
        else {
            log("worker connect fail to ip = " + ip, file, mainId);
        }
        (*sendSockets)[i] = sendSocket;
    }
    Data nullMsg = Data();
    nullMsg.type = PULL;
    nullMsg.timeStamp = getCurrentTime();
    nullMsg.start = -1;
    nullMsg.end = -1;
    for(int i =0; i < node->serverNum; i++) {
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

void tryPULL(void* args) {
    log("tryPULL",file, mainId);
    Node* node =(Node*)args;
    for(int i = 0; i < sendSockets->size(); i++) {
        Data msg = Data();
        msg.type = PULL;
        msg.timeStamp =getCurrentTime();
        msg.start = data->start;
        msg.end = data->end;
        int start = (M / node->serverNum + 1) * i;
        if(msg.start < start) {
            msg.start = start;
        }
        int end = (M / node->serverNum + 1) * (i + 1);
        if(msg.end > end) {
            msg.end = end;
        }
        (*sendMsgList)[i] = &msg;
        (*sendMsgArray)[i] = 0;
    }
    sendAllSockets(sendLinkMsg, mainId);
    receiveAllSockets(receiveLinkMsg, mainId);
    for(int i = 0; i < receiveSockets->size(); i++) {
        cout<< (*receiveMsgArray)[i]<<" ";
    }
    cout<<endl;
    log("tryPULL receiveAllSockets finished",file, mainId);
    for(int i = 0; i < receiveSockets->size(); i++) {
        if ((*receiveMsgList)[i]->type != PULL) {
            log("receive pull error" + (*receiveMsgList)[i]->head(), file, mainId);
            exit(1);
        }
        log("tryPULL " + (*receiveMsgList)[i]->toString(),file, mainId);
        if((*receiveMsgList)[i]->start != -1) {
            data->addData((*receiveMsgList)[i]->start, (*receiveMsgList)[i]->end, (*receiveMsgList)[i]->data);
//            log("server "+ to_string(i) + " has pulled right data", file);
        }
    }


}

void tryPUSH(void* args) {
    log("tryPUSH",file, mainId);
    Data* msg = error;
    msg->type = PUSH;
    msg->start = data->start;
    msg->end = data->end;
    msg->timeStamp = getCurrentTime();
    for(int i = 0; i < sendSockets->size(); i++) {
        (*sendMsgList)[i] = msg;
        (*sendMsgArray)[i] = 0;
    }
    sendAllSockets(sendLinkMsg, mainId);
//    sleep(3);
    receiveAllSockets(receiveLinkMsg, mainId);
    for(int i = 0; i < receiveSockets->size(); i++) {
        if ((*receiveMsgList)[i]->type != PUSH) {
            log("receive push error" + (*receiveMsgList)[i]->head(), file, mainId);
            exit(1);
        }
        if((*receiveMsgList)[i]->start != -1) {
            weight->addData((*receiveMsgList)[i]->start, (*receiveMsgList)[i]->end, (*receiveMsgList)[i]->data);
//            log("server "+ to_string(i) + " has pulled right data", file);
        }
    }
}

void reInitLocalData(void* args) {
    log("reInitLocalData",file, mainId);
    Node* node =(Node*)args;
    getDataWithWorkerNum(node, node->id);
}

void interactWithServer(void* args) {
    log("interactWithWorker",file, mainId);
//    Node* node =(Node*)args;
    tryPULL(args);
    for(int k = 0; k < ITERATOR * 2; k++) {
        tryPUSH(args);
        hasItered++;
        log("interactWithServer hasItered = " + to_string(hasItered),file, mainId);
        if(hasItered > ITERATOR) {
            break;
        }
//        // 用来测试添加节点消息
//        while(hasItered == ITERATOR/2 && !isAddNode) {
//            sleep(2);
//        }
        if(isAddNode) {
            reInitLocalData(args);
            isAddNode = false;
            tryPULL(args);
        }
    }
}

void* listenScheduler(void* args) {
    log("listenScheduler",file, mainId);
//    return NULL;
    Node* node =(Node*)args;
    string ip = node->getTCP() + to_string(node->scheduler->id);
    log("worker bind to ip = " + ip, file, mainId);
    void* socket = repListener(ip);
    int MAXLEN = 500 * (8 + 1);
    char tmp[500 * (sizeof(double) + 1)];
    zmq_recv(socket, tmp, MAXLEN, 0);
    log("receive msg from scheduler size =  " + to_string(strlen(tmp)),file, mainId);
    log(tmp, file, mainId);
    Data msg = Data(tmp);
    if(msg.type == ADDNODE) {
        node->workerNum += 1;
        isAddNode = true;
    } else if(msg.type == STOP) {
        exit(0);
    }
    log("receive msg from scheduler finished",file, mainId);
    return NULL;
}

void* start_worker(void* args) {
    log("start_worker",file, mainId);
    Node* node =(Node*)args;
    // start_worker 起一个不需要等待的线程用来处理scheduler的消息
//    pthread_t id1;
//    pthread_create(&id1, NULL, listenScheduler, (void*)node);
    initLocalData(args);
    initSocketsAndLinkMsg(args);
    interactWithServer(args);
    log("func start_worker finished", file, mainId);
    return NULL;
}

int main(int argc, char *argv[]) {
    Node* worker = new Node(argv[1]);
    for(int i = 6; i < argc; i++) {
        worker->addLinks(argv[i]);
    }
    file = file + to_string(worker->id);
    mainId += worker->id;
    log(file,mainId);
    log("start worker", file, mainId);
//    log("worker->workerNum == worker->serverNum = " + to_string(worker->workerNum == worker->serverNum), file);
    pthread_t id;
    pthread_create(&id, NULL, start_worker, (void*)worker);
    pthread_join(id,NULL);
    log("work "+ to_string(worker->id) + " finished", file, mainId);
//    sleep(10);
    // 将结果保存
}