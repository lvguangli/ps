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
    log("test0",file);
    weight->initData(weight->end - weight->start);
    for(int i = weight->start; i< weight->end; i++) {
        vector<double> second;
        second.push_back(1);
        weight->data.push_back(second);
    }
    log("test1",file);
    data = new Data();
    data->start = (M / SERVERNUM + 1) * node->id;
    data->end = (M / SERVERNUM + 1) * (node->id + 1);
    if(data->end > M) {
        data->end = M;
    }
    log("test2",file);
    log("data->start " + to_string(data->start), file);
    log("data->end " + to_string(data->end), file);
    data->initData(data->end - data->start);
    log("test3",file);

}

void responsePUSH(void* socket, Data* msg) {
    //calc
    zmq_send(socket, weight, sizeof(*weight), 0);
}

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

void responsePULL(void* socket,Data* msg) {
    Data* res = new Data();
    res->type = PULL;
    if(msg->start >= data->end || msg->end <= data->start) {
        log("responsePULL not found data", file);
        res->start = -1;
        res->end = -1;
        string str = res->toString();
        zmq_send(socket, str.c_str(), str.size(), 0);
        return ;
    }
    int start = msg->start;
    for(; start < msg->end; start++) {
        if(start < data->start)
            continue;
        else
            break;
    }
    log("start = " + to_string(start), file);
    res->start = start;
    int end = msg->end;
    if(end > data->end)
        end = data->end;
    res->end = end;
    res->initData(res->end - res->start);
    for(; start < end; start++) {
        res->data[start - res->start] = data->data[start - data->start];
    }
    string str = res->toString();
    log("response pull str.size = " + to_string(str.size()), file);
    zmq_send(socket, str.c_str(), str.size(), 0);

}

void* start(void* args) {
    log("start", file);
    Node* node =(Node*)args;
    getError(node);
    log("geterror done", file);
    getWeight(node);
    log("getweight done", file);
    initData("train.txt");
    log(node->name + " " + node->getTCP(),file);
    void* socket = repListener(node->getTCP());
     for(int i=0;i< ITERATOR * 2;i++){
        log("try to receive msg", file);
         int maxLen = M * sizeof(double);
         char tmp[M * sizeof(double)];
         zmq_recv(socket, tmp, maxLen, 0);
         string str = tmp;
         log("receive msg = " + str,file);
         Data* msg = new Data(str,file);
         log(msg->toString(),file);
        if (msg->type == OK) {
            log("receive OK", file);
        } else if (msg->type == ADDNODE) {
            log("receive ADDNODE", file);
        } else if (msg->type == PUSH) {
            hasItered ++;
            if(hasItered >= ITERATOR) {
                break;
            }
            log("receive PUSH", file);
            responsePUSH(socket, msg);
        } else if (msg->type == PULL) {
            log("receive PULL", file);
            responsePULL(socket, msg);
        } else if (msg ->type == STOP) {
            log("receive STOP", file);
        } else {
            log("error msg->type", file);
        }
    }
    zmq_disconnect(socket,node->getTCP().c_str());
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
    pthread_create(&id, NULL, start, (void*)node);
//    start((void*) node);
    pthread_join(id,NULL);
}