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
string file = "server";

int WORKERNUM = workerN;
int SERVERNUM = serverN;
int hasItered = 0;
// 保存全部error
Data* error;
// 只保留和自己server计算相关的weight
Data* weight;
// 只保存和自己相关的data
Data* data;

unordered_map<int,void*> sockets;



void getError(Node* node) {
    error = new Data();
    error->start = 0;
    error->end = M;
    error->initData(error->end - error->start);


}
void getWeight(Node* node) {
    weight = new Data();
    weight->start = (N / SERVERNUM + 1) * node->id;
    weight->end = (N / SERVERNUM + 1) * (node->id + 1);
    log("node->id" + to_string(node->id), file);
    log("weight->end" + to_string(weight->end), file);
    if(weight->end > N) {
        weight->end = N;
    }
//    log("test0",file);
    weight->initData(weight->end - weight->start, 1);

//    log("test1",file);
    data = new Data();
    data->start = (M / SERVERNUM + 1) * node->id;
    data->end = (M / SERVERNUM + 1) * (node->id + 1);
    if(data->end > M) {
        data->end = M;
    }
//    log("test2",file);
    log("data->start " + to_string(data->start), file);
    log("data->end " + to_string(data->end), file);
    data->initData(data->end - data->start);
//    log("test3",file);

}

/**
 * 只保存push上来的error，push操作需要同步，所以只保存数据，然后等待所有节点全部完成再发送数据
 * @param socket
 * @param msg
 */
void responsePUSH(void* socket, Data* msg) {
    error->addData(msg->start, msg->end, msg->data);
}

/**
 * 加载数据
 * @param train
 */
void initData(string train) {
    ifstream input(train);
    char line[1000];
    int first = 0;
    while(input.getline(line, 1000)) {
        if(data->start > first) {
            first++;
            continue;
        }
        vector<double> second;
        char word[10];
        int index = 0;
        int cur = 0;
        while(line[cur]) {
            if(line[cur] == ' ') {
                word[index] = '\0';
//                log("init word " + to_string(atof(word)), file);
                second.push_back(atof(word));
                index = 0;
            } else {
                word[index] = line[cur];
                index++;
            }
            cur++;
        }
        word[index] = '\0';
//        log("init word " + to_string(atof(word)), file);
        second.push_back(atof(word));
        data->data[first - data->start] = second;
        first++;
        if(data->end <= first) {
            break;
        }
    }
    input.close();

}

//void responsePULL(void* socket,Data* msg) {
//    Data* res = new Data();
//    res->type = PULL;
//    if(msg->start >= data->end || msg->end <= data->start) {
//        log("responsePULL not found data", file);
//        res->start = -1;
//        res->end = -1;
//        string str = res->toString();
//        log("response pull str.size = " + to_string(str.size()), file);
//        zmq_send(socket, str.c_str(), str.size(), 0);
//        return ;
//    }
//    int start = msg->start;
//    for(; start < msg->end; start++) {
//        if(start < data->start)
//            continue;
//        else
//            break;
//    }
////    log("start = " + to_string(start), file);
//    res->start = start;
//    int end = msg->end;
//    if(end > data->end)
//        end = data->end;
//    res->end = end;
//    res->initData(res->end - res->start);
//    for(; start < end; start++) {
//        res->data[start - res->start] = data->data[start - data->start];
//    }
//    string str = res->toString();
//    log("response pull str.size = " + to_string(str.size()), file);
//    zmq_send(socket, str.c_str(), str.size(), 0);
//
//}

/**
 * 根据msg来判断send的数据范围，have是自己拥有的数据
 * @param index
 * @param msg
 * @param have
 */
void sendData(void* socket, int index, Data *msg, Data *have) {
//    void* socket = sockets[index];
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
void responsePULL(void* socket,int index, Data* msg) {
//    void* socket = sockets[index];
    sendData(socket,index, msg, data);
}

/**
 * 接收消息，如果是pull操作，直接send数据，如果是push操作，则保存数据并结束。
 * @param args
 * @return
 */
void* receive(void *args) {
    void* socket = args;
    int index = 0;
    for(;;) {
        if(socket == sockets[index]) {
            break;
        }
        index ++;
    }
    for(int i=0;i< ITERATOR * 2;i++) {
        log("hasItered = " + to_string(hasItered) + "try to receive msg from worker" + to_string(index), file);
        int maxLen = M * sizeof(double);
        char tmp[M * sizeof(double)];
        zmq_recv(socket, tmp, maxLen, 0);
//        string str = tmp;
        log("receive from worker" + to_string(index) + " msg.size = " + to_string(strlen(tmp)) , file);
        if(strlen(tmp) < 10) {
            log(tmp, file);
        }
        Data msg = Data(tmp);
        log("parse right from worker" + to_string(index),file);
//        delete[] tmp;
        log(msg.head(), file);
        log(to_string(msg.type),file);
        if (msg.type == OK) {
            log("receive OK", file);
        } else if (msg.type == ADDNODE) {
            log("receive ADDNODE", file);
        } else if (msg.type == PUSH) {
            log("receive PUSH", file);
            responsePUSH(socket, &msg);
            break;
        } else if (msg.type == PULL) {
            log("receive PULL from worker" + to_string(index), file);
            responsePULL(socket,index, &msg);
        } else if (msg.type == STOP) {
            log("receive STOP", file);
        } else {
            log("error msg->type", file);
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
    void* socket = args;
    int index = 0;
    for(;;) {
        if(socket == sockets[index]) {
            break;
        }
        index ++;
    }
    Data msg = Data();
    msg.start = weight->start;
    msg.end = weight->end;
    sendData(socket, index, &msg, weight);
    return NULL;
}

/**
 * 计算权重
 */
void calc() {

}

void* start_server(void* args) {
    log("start", file);
    Node* node =(Node*)args;
    getError(node);
//    log("geterror done", file);
    getWeight(node);
//    log("getweight done", file);
    initData("train.txt");
    log(node->name + " " + node->getTCP(),file);
    for(int i = 0; i< WORKERNUM; i++) {
        string ip = node->getTCP() + to_string(i);
        log("server bind to ip = " + ip, file);
        void* socket = repListener(ip);
        sockets[i] = socket;
    }
    for(int k = 0; k < ITERATOR * 2; k++) {
        vector<pthread_t*> threads;
//        int index[3]  = {0};
        for(int i = 0; i< sockets.size(); i++) {
            pthread_t id;
//            index[i] = i;
            void* socket = sockets[i];
            pthread_create(&id, NULL, receive, socket);
            threads.push_back(&id);
        }
        for(int i =0; i < threads.size(); i++) {
            pthread_join(*(threads[i]),NULL);
        }
        threads.clear();
        calc();
        for(int i = 0; i< sockets.size(); i++) {
            pthread_t id;
//            index[i] = i;
            void* socket = sockets[i];
            pthread_create(&id, NULL, send, socket);
            threads.push_back(&id);
        }
        for(int i =0; i < threads.size(); i++) {
            pthread_join(*(threads[i]),NULL);
        }
//        sleep(2);
//        break;
        hasItered++;
        threads.clear();
        if(hasItered >= ITERATOR) {
            break;
        }
    }
    return NULL;
}



int main(int argc, char *argv[]) {

    //./server 0 server0 192.168.1.100 12313 tcp://192.168.1.100:12400 192.168.1.100:12308 192.168.1.100:12309'

    Node* node = new Node(argv[1], atoi(argv[2]), argv[3], argv[4],argv[5]);
    for(int i = 6; i < argc; i++) {
        node->addLinks(argv[i]);
    }
    file = file + to_string(node->id) + ".txt";
    log(file);
    log("start server", file);
    pthread_t id;
    pthread_create(&id, NULL, start_server, (void*)node);
//    start((void*) node);
    pthread_join(id,NULL);
    log("server "+ to_string(node->id) + " finished", file);
}