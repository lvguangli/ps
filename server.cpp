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
vector<Data> receiveMsgList[3];
int receiveIter = 0;

unordered_map<int,void*> sendSockets;
vector<Data> sendMsgList[3];
int sendIter = -1;

void* schedulerRepSocket;


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

void initSockets(void* args) {
    log("initSockets",file, mainId);
    Node* node =(Node*)args;
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
    receiveIter++;
    sendIter++;
    waitSendQueue(len);
}

void handlePUSH(void* args) {
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
        log("handlePUSH msg=" + msg.toString(),file, mainId);
        log("handlePUSH error=" + error->head(),file, mainId);
        error->addData(msg.start, msg.end, msg.data);
    }
    log("handlePUSH set error finish", file, mainId);
    for(int i = 0; i < len; i++) {
        receiveMsgList[i].pop_back();
    }
    receiveIter++;
    calc();
    weight->timeStamp = getCurrentTime();
    weight->type = PUSH;
    for(int i=0; i < len; i++) {
        sendMsgList[i].push_back(*weight);
    }
    sendIter++;
    waitSendQueue(len);
}

void* responseScheduler(void* args, int value) {
    log("sendMsgScheduler",file, mainId);
    Node* node =(Node*)args;
    Data msg = Data();
    msg.type = OK;
    if(value > 0) {
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
    string str = msg.toString();
    int len = zmq_send(schedulerRepSocket, str.c_str(), str.size(), 0);
    while(len < 0) {
        len = zmq_send(schedulerRepSocket, str.c_str(), str.size(), 0);
    }
    log("sendMsgScheduler send to schedulerRepSocket len=" + to_string(len),file, mainId);
    return NULL;
}

void* repScheduler(void* args) {
    responseScheduler(args, hasItered);
    return NULL;
}


void* serverListenScheduler(void* args) {
    log("listenScheduler",file, mainId);
//    return NULL;
    Node* node =(Node*)args;
    string ip = node->scheduler->getTCP() + to_string(node->id);
    log("server connect to ip = " + ip, file, mainId);
    schedulerRepSocket = repListener(ip);
    char tmp[OKMSGLEN];
    int len = zmq_recv(schedulerRepSocket, tmp, OKMSGLEN, 0);
    while(len < 0) {
        len = zmq_recv(schedulerRepSocket, tmp, OKMSGLEN, 0);
        sleep(1);
    }
    log("receive msg from scheduler size =  " + to_string(strlen(tmp)),file, mainId);
    log(tmp, file, mainId);
    Data msg = Data(tmp);
    if(msg.type == ADDNODE) {
        int workerId = (int) msg.data[0][0];
        node->addLinks("tcp://" + whosts[workerId][0] + ":" + whosts[workerId][1]);
        isAddNode = true;
    }
    return NULL;
}

void* checkScheduler(void* args) {
    log("checkScheduler",file, mainId);
    Node* node =(Node*)args;
    char tmp[OKMSGLEN];
    int len = zmq_recv(schedulerRepSocket, tmp, OKMSGLEN, 0);
    while(len < 0) {
        len = zmq_recv(schedulerRepSocket, tmp, OKMSGLEN, 0);
        sleep(1);
    }
    tmp[len] = '\0';
    log("checkScheduler receive msg from scheduler size =  " + to_string(len),file, mainId);
    log(tmp, file, mainId);
    Data msg = Data(tmp);
    if(msg.type != RESTART) {
        exit(0);
    }
    responseScheduler(args, -1);
    log("checkScheduler receive msg from scheduler finished",file, mainId);
    return NULL;
}

void dealWithScheduler(void* args) {
    Node* node =(Node*)args;
    isAddNode = false;
    addSocket(args);
    //TODO 起线程send receive
    pthread_t rid;
    pthread_create(&rid, NULL, receive, receiveSockets[node->workerNum - 1]);
    pthread_t sid;
    pthread_create(&sid, NULL, send, sendSockets[node->workerNum - 1]);
    pthread_t schid;
    pthread_create(&schid, NULL, repScheduler, args);
    pthread_t checkid;
    pthread_create(&checkid, NULL, checkScheduler, args);
    pthread_join(checkid, NULL);
    handlePULL(args);
}

void* interactWithWorker(void* args) {
    log("interactWithWorker",file, mainId);
    handlePULL(args);
    for(int k = 0; k < ITERATOR * 2; k++) {
        handlePUSH(args);
        hasItered++;
        log("interactWithWorker hasItered = " + to_string(hasItered),file, mainId);
        if(hasItered >= ITERATOR) {
            break;
        }
//        // 用来测试添加节点消息
//        while(hasItered == ITERATOR/2 && !isAddNode) {
//            sleep(2);
//        }
        if(isAddNode) {
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
    // start_server 起一个不需要等待的线程用来处理scheduler的消息
    pthread_t lid;
    pthread_create(&lid, NULL, serverListenScheduler, (void*)node);
    initLocalData(node);
    initTrainData("train.txt");
    initSockets(args);
    vector<pthread_t> ids;
    int len = node->workerNum;
    for(int i = 0; i < len; i++) {
        pthread_t rid;
        pthread_create(&rid, NULL, receive, receiveSockets[i]);
        ids.push_back(rid);
        pthread_t sid;
        pthread_create(&sid, NULL, send, sendSockets[i]);
        ids.push_back(sid);
    }
    pthread_t mid;
    pthread_create(&mid, NULL, interactWithWorker, args);
    ids.push_back(mid);
    for(int i = 0; i < ids.size(); i++) {
        pthread_join(ids[i], NULL);
    }
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