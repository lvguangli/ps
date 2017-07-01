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

unordered_map<int,void*> receiveSockets;
vector<Data> receiveMsgList[5];
char* receiveBuffer[5];
int receiveBufferLen = 0;

unordered_map<int,void*> sendSockets;
vector<Data> sendMsgList[5];
char* sendBuffer[5];
int sendBufferLen = 0;
void* schedulerRepSocket;
int schedulerSetIter = -1;

int realMsgMaxLen = 0;

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
            if(len > realMsgMaxLen) {
                realMsgMaxLen = len;
            }
            log("send to server" + to_string(index) + " msg.head=" + msg.head(),file+"send", index);
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

void initLocalData(Node* node) {
    log("initLocalData",file, mainId);
    sendBufferLen = M * N * sizeof(double) + M + 1;
    receiveBufferLen = M * sizeof(double) + M + 1;
    for(int i = 0; i < node->workerNum; i++) {
        sendBuffer[i] = new char[sendBufferLen];
        receiveBuffer[i] = new char[receiveBufferLen];
    }
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
    log("load train data finish", file, mainId);

}

void initSockets(void* args) {
    log("initSockets",file, mainId);
    Node* node =(Node*)args;

    string ip = node->getTCP() + to_string(node->scheduler->id);
    log("serverListenScheduler server bind to ip = " + ip, file, mainId);
    schedulerRepSocket = repListener(ip);

    for(int i = 0; i < node->workerNum; i++) {
        receiveMsgList[i] = vector<Data>();
        sendMsgList[i] = vector<Data>();
    }
    for(int i = 0; i < node->workerNum; i++) {
        string ip = node->getTCP() + to_string(i);
        void* receiveSocket = repListener(ip);
        if(receiveSocket != NULL) {
            log("server bind to ip = " + ip, file, mainId);
        }
        else {
            log("server bind fail to ip = " + ip, file, mainId);
        }
        receiveSockets[i] = receiveSocket;
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
        sendSockets[i] = sendSocket;
    }
}

void addSocket(void* args) {
    log("addSocket",file, mainId);
    Node* node =(Node*)args;
    int workerId = node->workerNum - 1;
    sendBuffer[workerId] = new char[sendBufferLen];
    receiveBuffer[workerId] = new char[receiveBufferLen];
    string ip = node->getTCP() + to_string(workerId);
    log("server bind to ip = " + ip, file, mainId);
    void* receiveSocket = repListener(ip);
    receiveSockets[workerId] = receiveSocket;

    ip = node->linksIP[workerId] + to_string(node->id);
    log("server connect to ip = " + ip, file, mainId);
    void* sendSocket = reqListener(ip);
    sendSockets[workerId] = sendSocket;
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

Data generatePullData(Data *msg, Data* have) {
    Data res = Data();
    if(msg->start == -1) {
        res.start = -1;
        res.end = -1;
        res.timeStamp = getCurrentTime();
        res.type = PULL;
        return res;
    }
    if(msg->start == have->start && msg->end == have->end) {
        res = *have;
        res.type = PULL;
        res.timeStamp = getCurrentTime();
        return res;
    }

    if(msg->start >= have->end || msg->end <= have->start) {
        res.start = -1;
        res.end = -1;
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
    res.type = PULL;
    res.timeStamp = getCurrentTime();
    return res;
}

void handlePULL(void* args) {
    log("handlePULL",file, mainId);
    Node* node =(Node*)args;
    int len = node->workerNum;
    waitReceiveQueue(len);
    log("handlePULL waitReceiveQueue",file, mainId);
    for(int i = 0; i < len; i++) {
        if(receiveMsgList[i][0].type != PULL) {
            log("receive pull error", file, mainId);
            exit(1);
        }
        Data msg = receiveMsgList[i][0];
        sendMsgList[i].push_back(generatePullData(&msg, data));
    }
    for(int i = 0; i < len; i++) {
        receiveMsgList[i].pop_back();
    }
    waitSendQueue(len);
}

int handlePUSH(void* args) {
    log("handlePUSH",file, mainId);
        Node* node =(Node*)args;
    int len =  node->workerNum;
    waitReceiveQueue(len);
    log("handlePUSH waitReceiveQueue finish", file, mainId);
    for(int i = 0; i < len; i++) {
        if(receiveMsgList[i][0].type != PUSH) {
            log("receive push error", file, mainId);
            exit(1);
        }
        Data msg = receiveMsgList[i][0];
//        log("handlePUSH msg=" + msg.toString(),file, mainId);
//        log("handlePUSH error=" + error->head(),file, mainId);
        error->addData(msg.start, msg.end, msg.data);
    }
    log("handlePUSH set error finish", file, mainId);
    for(int i = 0; i < len; i++) {
        receiveMsgList[i].pop_back();
    }
    calc();
    weight->timeStamp = getCurrentTime();
    weight->type = PUSH;
    for(int i=0; i < len; i++) {
        sendMsgList[i].push_back(*weight);
    }
    waitSendQueue(len);
    return 1;
}

void* responseScheduler(void* args, int value) {
    log("responseScheduler",file, mainId);
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
    string str = tmp;
    log("responseScheduler send msg=" + str, file, mainId);
    int len = zmq_send(schedulerRepSocket, tmp, size, 0);
    while(len < 0) {
        len = zmq_send(schedulerRepSocket, tmp, size, 0);
    }
    log("sendMsgScheduler send to schedulerRepSocket len=" + to_string(len),file, mainId);
    return NULL;
}

void* serverListenScheduler(void* args) {
    log("listenScheduler",file, mainId);
    Node* node =(Node*)args;
    char tmp[OKMSGLEN];
    int len = zmq_recv(schedulerRepSocket, tmp, OKMSGLEN, 0);
    while(len < 0) {
        len = zmq_recv(schedulerRepSocket, tmp, OKMSGLEN, 0);
        sleep(1);
    }
    tmp[len] = '\0';
    log("receive msg from scheduler size =  " + to_string(strlen(tmp)),file, mainId);
    log(tmp, file, mainId);
    Data msg = Data(tmp);
    log(tmp, file, mainId);
    if(msg.type == ADDNODE) {
        int workerId = (int) msg.data[0][0];
        node->addLinks("tcp://" + whosts[workerId][0] + ":" + whosts[workerId][1]);
        isAddNode = true;
        responseScheduler(args, hasItered);
        serverListenScheduler(args);
    } else if(msg.type == RESTART) {
        schedulerSetIter = (int) msg.data[0][0];
        responseScheduler(args, -1);
    } else if(msg.type == STOP) {
        exit(0);
    }

    return NULL;
}

void dealWithScheduler(void* args) {
    Node* node =(Node*)args;
    isAddNode = false;
    node->workerNum += 1;
    addSocket(args);
    //TODO 起线程send receive
    pthread_t rid;
    pthread_create(&rid, NULL, receive, receiveSockets[node->workerNum - 1]);
    pthread_t sid;
    pthread_create(&sid, NULL, send, sendSockets[node->workerNum - 1]);
    handlePULL(args);
}

void* interactWithWorker(void* args) {
    log("interactWithWorker",file, mainId);
    handlePULL(args);
    for(int k = 0; k < ITERATOR * 2; k++) {
        int re = handlePUSH(args);
        hasItered += re;
        log("interactWithWorker hasItered = " + to_string(hasItered),file, mainId);
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



/**
 *
 * @param args  node
 * @return
 */
void* start_server(void* args) {
    log("start_server",file, mainId);
    Node* node =(Node*)args;
//    vector<pthread_t> ids;
    initLocalData(node);
    initTrainData(trainFile);
    initSockets(args);
    // start_server 起一个不需要等待的线程用来处理scheduler的消息
    pthread_t lid;
    pthread_create(&lid, NULL, serverListenScheduler, (void*)node);
    int len = node->workerNum;
    for(int i = 0; i < len; i++) {
        pthread_t rid;
        pthread_create(&rid, NULL, receive, receiveSockets[i]);
        pthread_t sid;
        pthread_create(&sid, NULL, send, sendSockets[i]);
    }
    pthread_t mid;
    pthread_create(&mid, NULL, interactWithWorker, args);
    pthread_join(mid, NULL);
//    ids.push_back(mid);
//    for(int i = 0; i < ids.size(); i++) {
//        pthread_join(ids[i], NULL);
//    }
    log("start_server finished", file, mainId);
    return NULL;
}


int main(int argc, char *argv[]) {
    long startTime = getCurrentTime();
    initConfig();
    Node* server = new Node(argv[1]);
    for(int i = 6; i < argc; i++) {
        server->addLinks(argv[i]);
    }
//    mainId += server->id;
    file = file + to_string(server->id);
    log(file, mainId);
    log("start server", file, mainId);
    log(server->toString(), file, mainId);
    pthread_t id;
    pthread_create(&id, NULL, start_server, (void*)server);
    pthread_join(id,NULL);
    log("server "+ to_string(server->id) + " finished", file, mainId);
    long endTime = getCurrentTime();
    log("server cost time = " + to_string(endTime - startTime), file, mainId);
}