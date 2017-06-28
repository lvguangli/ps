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

using namespace std;
string file = "worker";
// 只计算自己worker分配的error部分 = N / workerN
Data* error;
// 保留全部权重特征，即需要向server拉取所有weight
Data* weight;
// 定义一个用来存放server拉取部分weight的对象指针
//Weight* wFromServer;

Data* data;

int WORKERNUM = workerN;
int SERVERNUM = serverN;
int hasItered = 0;
int addFlag = false;
/**
 * 从server拉取data数据，并更新其他标记
 * @param args
 * @return
 */
void* pull(void* args){
    string ip = (char*)args;
    Data* msg = new Data();
    msg->type = PULL;
    msg->start = data->start;
    msg->end = data->end;
    void* socket = reqListener(ip);
    log("send PULL", file);
    string str = msg->toString();
    log("send msg = " + str, file);
    log("send msg.size = " + to_string(str.size()), file);
    zmq_send(socket, str.c_str(), str.size(), 0);
    char tmp[M*N*(sizeof(double) + 1)];
    zmq_recv(socket, tmp, M*N*(sizeof(double) + 1), 0);
    int len = strlen(tmp);
    log("res len = " + to_string(len),file);
    log(tmp, file);
    Data* tmpd = new Data(tmp);
    if(tmpd->start != -1)
        data->addData(tmpd->start, tmpd->end, tmpd->data);
    else {
        log(ip + " has no data for this node", file);
    }
//    zmq_disconnect(socket,ip.c_str());
}
void* push(void* args) {
    string ip = (char*)args;
//    int n = atoi(ip.substr(0,1).c_str());
//    ip = ip.substr(1, ip.length() - 2);
    void* socket = reqListener(ip);
    for(;hasItered < ITERATOR; hasItered++) {
        error->type = PUSH;
        log("send PUSH", file);
        zmq_send(socket, error, sizeof(*error), 0);
        char tmp[500 * sizeof(double)];
        zmq_recv(socket, tmp, 500 * sizeof(double), 0);
        int len = strlen(tmp);
        char * tmp2 = new char[len];
        Data* wTmp = (Data*) tmp2;
        weight->addData(wTmp->start, wTmp->end, wTmp->data);
        delete[] tmp2;
//        if(addFlag > 0) {
//            addFlag -= 1;
//            break;
//        }
    }
    zmq_disconnect(socket,ip.c_str());
}
void getError(Node* node) {
    error = new Data();
    error->start = (M / WORKERNUM + 1) * node->id;
    error->end = (M / WORKERNUM + 1) * (node->id + 1);
    if(error->end > M) {
        error->end = M;
    }
    error->initData(error->end - error->start);

    data = new Data();
    data->start = (M / WORKERNUM + 1) * node->id;
    data->end = (M / WORKERNUM + 1) * (node->id + 1);
    if(data->end > M) {
        data->end = M;
    }
    data->initData(data->end - data->start);
}
void getWeight(Node* node) {
    weight = new Data();
    weight->start = 0;
    weight->end = N;
    weight->initData(N);
    for(int i = weight->start; i< weight->end; i++) {
        vector<double> vector2;
        vector2.push_back(1);
        weight->data.push_back(vector2);
    }
    // 每个server存放的weight只会小于定义的对象的大小
//    wFromServer = new Weight();
//    wFromServer->start = (N / SERVERNUM + 1) * node->id;
//    wFromServer->end = (N / SERVERNUM + 1) * (node->id + 1);
//    wFromServer->initWeight(error->end - error->start);

}
void* start(void* args) {

    Node* node  = (Node*)args;
    getError(node);
    getWeight(node);

    vector<pthread_t*> ids;
    log("node->linksNum " + to_string(node->linksNum), file);
    //按照顺序拉取数据
    for(int i = 0; i < node->linksNum; i++) {
        pthread_t id;
        pthread_create(&id,NULL,pull, (void*)(node->linksIP[i]).c_str());
    }
    sleep(10);
//    for(int i = 0; i < node->linksNum; i++) {
//        pthread_t id;
//        pthread_create(&id,NULL,push, (void*)(node->linksIP[i]).c_str());
//        ids.push_back(&id);
//    }
//    for(pthread_t* id : ids) {
//        pthread_join(*id, NULL);
//    }
//    ids.clear();
}



int main(int argc, char *argv[]) {
    Node* node = new Node(argv[1], atoi(argv[2]), argv[3], argv[4], argv[5]);
    for(int i = 6; i < argc; i++) {
        node->addLinks(argv[i]);
    }
    file = file + to_string(node->id) + ".txt";
    log(file);
    log("start worker", file);
//    pthread_t id;
//    pthread_create(&id, NULL, start, (void*)node);
    start((void*) node);
//    pthread_join(id,NULL);
//    sleep(10);
    // 将结果保存
}