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

unordered_map<int,void*> receiveSockets;
vector<Data> receiveMsgList[3];
char* receiveBuffer[5];
int receiveBufferLen = 0;

unordered_map<int,void*> sendSockets;
vector<Data> sendMsgList[3];
char* sendBuffer[5];
int sendBufferLen = 0;

void* schedulerRepSocket;
int schedulerSetIter = -1;

Data* pullMsg;


bool isAddNode = false;
int mainId = 6;

int getSocketIndex(void* socket, unordered_map<int,void*> sockets) {
    int index = 0;
    for(;;) {
        if(socket == sockets[index]) {
            break;
        }
        index ++;
        if(index > 100) {
            index = -1;
            break;
        }
    }
    return index;
}

void* send(void* socket) {
    int index = getSocketIndex(socket, sendSockets);
    log("send", file+"send", index);
    while(true) {
        log("send hasItered = " + to_string(hasItered),file+"send", index);
        if(hasItered >= ITERATOR) {
            log("hasItered >= ITERATOR",file+"send", index);
            break;
        }
        if(sendMsgList[index].size() > 0) {
            Data msg = sendMsgList[index][0];
            int len = msg.save2Buffer(sendBuffer[index]);
            log("send to server" + to_string(index) + " msg.head=" + msg.head(),file + "send", index);
            log("send to server" + to_string(index) + " msg.toString=" + msg.head(),file + "send", index);
            log(sendBuffer[index], file + "send", index);
            if(zmq_send(socket, sendBuffer[index], len, 0) < 0) {
                log("send zmq_send < 0; try again",file+"send", index);
                sleep(1);
                continue;
            }
            int maxTry = 5;
            char tmp[OKMSGLEN];
            len = zmq_recv(socket, tmp, OKMSGLEN, 0);
            while(len < 0) {
                log("send zmq_recv < 0",file+"send", index);
                sleep(1);
                maxTry--;
                if(maxTry < 0) {
                    break;
                }
                len = zmq_recv(socket, tmp, OKMSGLEN, 0);
            }
            if(maxTry < 0) {
                continue;
            }
            tmp[len] = '\0';
            log("send receive from " + to_string(index) + " msg.size = " + to_string(strlen(tmp)) , file+"send", index);
            log(tmp, file+"send", index);
            Data respon = Data(tmp);
            if(respon.type != OK) {
                log("send receive wrong msg", file+"send", index);
                continue;
            } else {
                sendMsgList[index].pop_back();
            }
        }
        sleep(1);
    }
    return NULL;
}

void* receive(void* socket) {
    int index = getSocketIndex(socket, receiveSockets);
    log("receive", file+"receive", index);
    while(true) {
        log("receive hasItered = " + to_string(hasItered), file+"receive", index);
        if(hasItered >= ITERATOR - 1) {
            log("receive hasItered >= ITERATOR", file+"receive", index);
            break;
        }
        char* tmp = receiveBuffer[index];
        int len = zmq_recv(socket, tmp, receiveBufferLen, 0);
        if(len < 0) {
            log("receive zmq_recv < 0",file+"receive", index);
            sleep(1);
            continue;
        }
        tmp[len] = '\0';
        log("receive zmq_recv len=" + to_string(len), file+"receive", index);
        log(tmp, file+"receive", index);
        Data msg = Data(tmp);
//        log("msg.head=" + msg.head(),file, index);
        if(receiveMsgList[index].size() > 0 && msg.timeStamp == receiveMsgList[index][0].timeStamp) {
            log("receive msg has receive ,just resopnse ok", file+"receive" ,index);
        } else {
            receiveMsgList[index].push_back(msg);
        }
        Data ok = Data();
        ok.type = OK;
        ok.timeStamp = msg.timeStamp;
        ok.start = -1;
        ok.end = -1;
        string str = ok.head();
        int maxTry = 5;
        while(zmq_send(socket, str.c_str(), str.size(), 0) < 0) {
            log("receive zmq_send < 0; try again",file+"receive", index);
            maxTry--;
            if(maxTry < 0) {
                break;
            }
        }
    }
    return NULL;
}

void waitReceiveQueue(int len) {
    log("waitReceiveQueue", file, mainId);
    while(true) {
        int count = 0;
        for(int i=0; i < len; i++) {
            if(receiveMsgList[i].size() > 0) {
                count++;
            }
        }
        log("waitReceiveQueue count=" + to_string(count), file, mainId);
        if(count < len) {
            sleep(2);
        } else {
            break;
        }
    }
}

void waitSendQueue(int len) {
    log("waitSendQueue", file, mainId);
    log("waitSendQueue len=" + to_string(len), file, mainId);
    while(true) {
        int count = 0;
        for(int i=0; i < len; i++) {
            log("waitSendQueue i=" + to_string(i), file, mainId);
            if(sendMsgList[i].size() == 0) {
                count++;
            }
        }
        log("waitSendQueue count=" + to_string(count), file, mainId);
        if(count < len) {
            sleep(2);
        } else {
            break;
        }
    }
}

void getDataWithWorkerNum(Node* node, int index, Data ** dataMsg, int value) {
    log("getDataWithWorkerNum",file, mainId);
    *dataMsg = new Data();
    (*dataMsg)->start = (M / node->workerNum + 1) * index;
    (*dataMsg)->end = (M / node->workerNum + 1) * (index + 1);
    if((*dataMsg)->end > M) {
        (*dataMsg)->end = M;
    }
    log("try init", file, mainId);
    if(value > 0) {
        (*dataMsg)->initData((*dataMsg)->end - (*dataMsg)->start, value);
    } else {
        (*dataMsg)->initData((*dataMsg)->end - (*dataMsg)->start);
    }

    log("has init dataMsg->start = " + to_string((*dataMsg)->start), file, mainId);
}

void getDataWithOutWorkerNum(Node* node, int index, Data ** dataMsg) {
    log("getDataWithOutWorkerNum",file, mainId);
    *dataMsg = new Data();
    (*dataMsg)->start = 0;
    (*dataMsg)->end = (N-1);
    (*dataMsg)->initData((N-1), 1);
}

float sigmoid(float x) {
    return (1.0 / (1 + exp(-x)));
}

void calc(void* args){
    log("calc",file, mainId);
    Node* node =(Node*)args;
    int count = error->end - error->start;
    for(int i = 0; i < count; i++) {
        float result = 0;
        for(int j = 0; j< N - 1; j++) {
            result += weight->data[j][0] * data->data[i][j];
        }
        error->data[i][0] = data->data[i][N-1] - sigmoid(result);
    }
    log("calc error finished",file, mainId);
    if(node->id == 0 && ITERATOR - hasItered < 10 && ITERATOR - hasItered > 0) {
        log("worker0 save weight", file, mainId);
        ofstream out("output/weight_"+ to_string(hasItered) + ".txt",ios::out);
        for(int i = weight->start; i < weight->end - 1; i++) {
            out<<weight->data[i][0]<<",";
        }
        out<<weight->data[weight->end - 1][0]<<endl;
        out.close();
    }
//    char tmp[10000];
//    error->save2Buffer(tmp);
//    log(tmp, file, mainId);
}

void initLocalData(void* args) {
    log("initLocalData",file, mainId);
    Node* node  = (Node*)args;
    log("initLocalData buffer",file, mainId);
    sendBufferLen = M * sizeof(double) + M + 1;
    receiveBufferLen = M * N * sizeof(double) + M + 1;
    for(int i = 0; i < node->serverNum; i++) {
        sendBuffer[i] = new char[sendBufferLen];
        receiveBuffer[i] = new char[receiveBufferLen];
    }
    log("initLocalData error",file, mainId);
    getDataWithWorkerNum(node, node->id, &error, 1);
    log("initLocalData data",file, mainId);
    getDataWithWorkerNum(node, node->id, &data, -1);
    log("initLocalData1",file, mainId);
    pullMsg = new Data();
    log("initLocalData2 data->start=" + to_string(data->start),file, mainId);
    pullMsg->start = data->start;
    log("initLocalData3",file, mainId);
    pullMsg->end = data->end;
    log("initLocalData4",file, mainId);
    getDataWithOutWorkerNum(node, node->id, &weight);
}

void initSockets(void* args) {
    log("initSockets",file, mainId);
    Node* node =(Node*)args;
    string ip = node->getTCP() + to_string(node->scheduler->id);
    log("workerListenScheduler worker bind to ip = " + ip, file, mainId);
    schedulerRepSocket = repListener(ip);

    for(int i = 0; i < node->serverNum; i++) {
        receiveMsgList[i] = vector<Data>();
        sendMsgList[i] = vector<Data>();
    }
    for(int i = 0; i < node->serverNum; i++) {
        string ip = node->getTCP() + to_string(i);
        void* receiveSocket = repListener(ip);
        if(receiveSocket != NULL) {
            log("worker bind to ip = " + ip, file, mainId);
        }
        else {
            log("worker bind fail to ip = " + ip, file, mainId);
        }
        receiveSockets[i] = receiveSocket;
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
        sendSockets[i] = sendSocket;
    }

}

void tryPULL(void* args) {
    log("tryPULL",file, mainId);
    Node* node =(Node*)args;
    int len =  node->serverNum;
    for(int i = 0; i < len; i++) {
        Data msg = Data();
        msg.type = PULL;
        msg.timeStamp =getCurrentTime();
        msg.start = pullMsg->start;
        msg.end = pullMsg->end;
        int start = (M / node->serverNum + 1) * i;
        if(msg.start < start) {
            msg.start = start;
        }
        int end = (M / node->serverNum + 1) * (i + 1);
        if(msg.end > end) {
            msg.end = end;
        }
        if(msg.start >= msg.end) {
            msg.start = -1;
            msg.end = -1;
        }
        sendMsgList[i].push_back(msg);
    }
    waitSendQueue(len);
    waitReceiveQueue(len);
    log("tryPULL waitReceiveQueue finished",file, mainId);
    for(int i = 0; i < len; i++) {
        log("i=" + to_string(i), file, mainId);
        if (receiveMsgList[i][0].type != PULL) {
            log("receive pull error" + receiveMsgList[i][0].head(), file, mainId);
            exit(1);
        }
//        log("tryPULL msg=" + receiveMsgList[i][0].toString(),file, mainId);
        if(receiveMsgList[i][0].start != -1) {
            data->addData(receiveMsgList[i][0].start, receiveMsgList[i][0].end, receiveMsgList[i][0].data);
        }

    }
    log("pulldata from server finish",file, mainId);
    for(int i = 0; i < len; i++) {
        receiveMsgList[i].pop_back();
    }
    calc(args);
}

int tryPUSH(void* args) {
    log("tryPUSH",file, mainId);
    Node* node =(Node*)args;
    Data* msg = error;
    msg->type = PUSH;
    msg->timeStamp = getCurrentTime();
    int len = node->serverNum;
    for(int i = 0; i < len; i++) {
        sendMsgList[i].push_back(*msg);
    }
    waitSendQueue(len);
    waitReceiveQueue(len);
    for(int i = 0; i < len; i++) {
        if (receiveMsgList[i][0].type != PUSH) {
            log("receive push error=" + receiveMsgList[i][0].head(), file, mainId);
            exit(1);
        }
        if(receiveMsgList[i][0].start != -1) {
            weight->addData(receiveMsgList[i][0].start, receiveMsgList[i][0].end, receiveMsgList[i][0].data);
//            log("server "+ to_string(i) + " has pulled right data", file);
        }
    }
    log("pushdata from server finish",file, mainId);
    for(int i = 0; i < len; i++) {
        receiveMsgList[i].pop_back();
    }
    calc(args);
    return 1;
}

void reInitLocalData(void* args) {
    log("reInitLocalData",file, mainId);
    Node* node =(Node*)args;
    getDataWithWorkerNum(node, node->id, &error, 1);
    //data 可以复用本地数据
    Data *msg = data;
    getDataWithWorkerNum(node, node->id, &data, -1);
    pullMsg->start = data->start;
    pullMsg->end = data->end;
    if(msg->start < data->end && msg->start >= data->start) {
        pullMsg->end = msg->start;
        int len = data->end - msg->start;
        int start = msg->start - data->start;
        for(int i = 0; i <len; i++) {
            data->data[i + start] = msg->data[i];
        }
    }
    if(pullMsg->start == pullMsg->end) {
        pullMsg->start = -1;
        pullMsg->end = -1;
    }
}

void* responseScheduler(void* args, int value) {
    log("sendMsgScheduler",file, mainId);
    Node* node =(Node*)args;
    Data msg = Data();
    msg.type = OK;
    if(value >= 0) {
        msg.start = 0;
        msg.end = 1;
        msg.timeStamp = getCurrentTime();
        msg.data = vector<vector<double>>();
        vector<double> tmp;
        double v = hasItered;
        tmp.push_back(v);
        msg.data.push_back(tmp);
    } else {
        msg.start = -1;
        msg.end = -1;
        msg.timeStamp = getCurrentTime();
    }
    char tmp[100];
    int size = msg.save2Buffer(tmp);
    int len = zmq_send(schedulerRepSocket, tmp, size, 0);
    while(len < 0) {
        len = zmq_send(schedulerRepSocket, tmp, size, 0);
    }
    log("sendMsgScheduler send to schedulerRepSocket len=" + to_string(len),file, mainId);
    return NULL;
}

void* workerListenScheduler(void* args) {
    log("listenScheduler",file, mainId);
    Node* node =(Node*)args;
    char tmp[OKMSGLEN];
    int len = zmq_recv(schedulerRepSocket, tmp, OKMSGLEN, 0);
    while(len < 0) {
        len = zmq_recv(schedulerRepSocket, tmp, OKMSGLEN, 0);
        sleep(1);
    }
    tmp[len] = '\0';
    log("receive msg from scheduler size =  " + to_string(len),file, mainId);
    log(tmp, file, mainId);
    Data msg = Data(tmp);
    if(msg.type == ADDNODE) {
        isAddNode = true;
        responseScheduler(args, hasItered);
        workerListenScheduler(args);
    } else if(msg.type == RESTART) {
        schedulerSetIter = (int) msg.data[0][0];
        responseScheduler(args, -1);
    } else if(msg.type == STOP) {
        exit(0);
    }
    log("receive msg from scheduler finished",file, mainId);
    return NULL;
}

void dealWithScheduler(void* args) {
    log("checkScheduler",file, mainId);
    Node* node =(Node*)args;
    node->workerNum += 1;
    reInitLocalData(args);
    isAddNode = false;
    tryPULL(args);
}

void* interactWithServer(void* args) {
    log("interactWithWorker",file, mainId);
//    Node* node =(Node*)args;
    tryPULL(args);
    for(int k = 0; k < ITERATOR * 2; k++) {
        int re = tryPUSH(args);
        hasItered += re;
        log("interactWithServer hasItered = " + to_string(hasItered),file, mainId);
        if(hasItered >= ITERATOR) {
            break;
        }
//        // 用来测试添加节点消息
//        while(hasItered == ITERATOR/2 && !isAddNode) {
//            sleep(2);
//        }
        if(isAddNode && hasItered == schedulerSetIter) {
            dealWithScheduler(args);
        }
    }
    return NULL;
}

void* start_worker(void* args) {
    log("start_worker",file, mainId);
    Node* node =(Node*)args;
    // start_worker 起一个不需要等待的线程用来处理scheduler的消息
    initLocalData(args);
    initSockets(args);
    pthread_t lid;
    pthread_create(&lid, NULL, workerListenScheduler, (void*)node);
//    vector<pthread_t> ids;
    int len = node->serverNum;
    for(int i = 0; i < len; i++) {
        pthread_t rid;
        pthread_create(&rid, NULL, receive, receiveSockets[i]);
        pthread_t sid;
        pthread_create(&sid, NULL, send, sendSockets[i]);
    }
    pthread_t mid;
    pthread_create(&mid, NULL, interactWithServer, args);
    pthread_join(mid, NULL);
//    ids.push_back(mid);
//    for(int i = 0; i < ids.size(); i++) {
//        pthread_join(ids[i], NULL);
//    }
    log("start_worker finished", file, mainId);
    return NULL;
}

int main(int argc, char *argv[]) {
    long startTime = getCurrentTime();
    initConfig();
    Node* worker = new Node(argv[1]);
    for(int i = 6; i < argc; i++) {
        worker->addLinks(argv[i]);
    }
    file = file + to_string(worker->id);
//    mainId += worker->id;
    log(file,mainId);
    log("start worker", file, mainId);
//    log("worker->workerNum == worker->serverNum = " + to_string(worker->workerNum == worker->serverNum), file);
    pthread_t id;
    pthread_create(&id, NULL, start_worker, (void*)worker);
    pthread_join(id,NULL);
    if(worker->id == 0) {
        log("worker0 save weight", file, mainId);
        ofstream out("output/weight.txt",ios::out);
        for(int i = weight->start; i < weight->end - 1; i++) {
            out<<weight->data[i][0]<<",";
        }
        out<<weight->data[weight->end - 1][0]<<endl;
        out.close();
    }
    log("work "+ to_string(worker->id) + " finished", file, mainId);
    long endTime = getCurrentTime();
    log("worker cost time = " + to_string(endTime - startTime), file, mainId);

}