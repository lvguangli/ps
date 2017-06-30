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
int receiveIter = 0;

unordered_map<int,void*> sendSockets;
vector<Data> sendMsgList[3];
int sendIter = -1;


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
        if(sendIter > ITERATOR) {
            log("sendIter > ITERATOR",file+"send", index);
            break;
        }
        if(sendMsgList[index].size() > 0) {
            Data msg = sendMsgList[index][0];
            string str = msg.toString();
            log("send to worker" + to_string(index) + " sendIter=" + to_string(sendIter) + " str=" + str,file+"send", index);
            int maxTry = 3;
            while(zmq_send(socket, str.c_str(), str.size(), 0) < 0) {
                log("send zmq_send < 0; try again",file+"send", index);
                maxTry--;
                if(maxTry < 0) {
                    break;
                }
            }
            if(maxTry < 0) {
                continue;
            }
            maxTry = 3;
            char tmp[OKMSGLEN];
            int len = zmq_recv(socket, tmp, OKMSGLEN, 0);
            while(len < 0) {
                log("send zmq_recv < 0",file+"send", index);
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
                //已确认发送成功的消息被重置timeStamp，只保留head
                sendMsgList[index].pop_back();
                if(sendIter >= ITERATOR) {
                    log("sendIter >= ITERATOR",file+"send", index);
                    break;
                }
            }
        } else {
            log("send has send data sendIter=" + to_string(sendIter), file+"send", index);
        }
        sleep(1);
    }
    return NULL;
}

void* receive(void* socket) {
    int index = getSocketIndex(socket, receiveSockets);
    log("receive", file+"receive", index);
    while(true) {
        if(receiveIter >= ITERATOR) {
            log("receive receiveIter >= ITERATOR", file+"receive", index);
            break;
        }
        log("receive receiveIter = " + to_string(receiveIter), file+"receive", index);
        char tmp[M * N * sizeof(double) + M + 1];
        int len = zmq_recv(socket, tmp, M * N * sizeof(double) + M + 1, 0);
        while(len < 0) {
            log("receive zmq_recv < 0",file+"receive", index);
            sleep(1);
            len = zmq_recv(socket, tmp, M * N * sizeof(double) + M + 1, 0);
        }
        tmp[len] = '\0';
        log("receive zmq_recv len=" + to_string(len), file+"receive", index);
        log(tmp, file, index);
        Data msg = Data(tmp);
        log("msg->" + msg.toString(),file, index);
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
        int maxTry = 3;
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
            sleep(1);
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
            sleep(1);
        } else {
            break;
        }
    }
}

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

void initSockets(void* args) {
    log("initSockets",file, mainId);
    Node* node =(Node*)args;
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
        if(msg.start >= msg.end) {
            msg.start = -1;
            msg.end = -1;
        }
        sendMsgList[i].push_back(msg);
    }
    sendIter++;
    waitSendQueue(len);
    waitReceiveQueue(len);
    log("tryPULL waitReceiveQueue finished",file, mainId);
    for(int i = 0; i < len; i++) {
        log("i=" + to_string(i), file, mainId);
        if (receiveMsgList[i][0].type != PULL) {
            log("receive pull error" + receiveMsgList[i][0].head(), file, mainId);
            exit(1);
        }
        log("tryPULL msg=" + receiveMsgList[i][0].toString(),file, mainId);
        if(receiveMsgList[i][0].start != -1) {
            data->addData(receiveMsgList[i][0].start, receiveMsgList[i][0].end, receiveMsgList[i][0].data);
        }

    }
    log("pulldata from server finish",file, mainId);
    for(int i = 0; i < len; i++) {
        receiveMsgList[i].pop_back();
    }
    receiveIter++;
}

void tryPUSH(void* args) {
    log("tryPUSH",file, mainId);
    Node* node =(Node*)args;
    Data* msg = error;
    msg->type = PUSH;
    msg->start = data->start;
    msg->end = data->end;
    msg->timeStamp = getCurrentTime();
    int len = node->serverNum;
    for(int i = 0; i < len; i++) {
        sendMsgList[i].push_back(*msg);
    }
    sendIter++;
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
    receiveIter++;
}

void reInitLocalData(void* args) {
    log("reInitLocalData",file, mainId);
    Node* node =(Node*)args;
    getDataWithWorkerNum(node, node->id);
}

void* interactWithServer(void* args) {
    log("interactWithWorker",file, mainId);
//    Node* node =(Node*)args;
    tryPULL(args);
    for(int k = 0; k < ITERATOR * 2; k++) {
        tryPUSH(args);
        hasItered++;
        log("interactWithServer hasItered = " + to_string(hasItered),file, mainId);
        if(hasItered >= ITERATOR) {
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
    return NULL;
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
    initSockets(args);
    vector<pthread_t> ids;
    int len = node->serverNum;
    for(int i = 0; i < len; i++) {
        pthread_t rid;
        pthread_create(&rid, NULL, receive, receiveSockets[i]);
        ids.push_back(rid);
        pthread_t sid;
        pthread_create(&sid, NULL, send, sendSockets[i]);
        ids.push_back(sid);
    }
    pthread_t mid;
    pthread_create(&mid, NULL, interactWithServer, args);
    ids.push_back(mid);
    for(int i = 0; i < ids.size(); i++) {
        pthread_join(ids[i], NULL);
    }
    log("start_worker finished", file, mainId);
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