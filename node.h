//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_DATA_H
#define PS_DATA_H
#include <iostream>
#include <cstring>
#include <stdlib.h>
using namespace std;
const int maxN = 10;
class Node {
public:
    string name;
    int id;
    string host;
    string port;
    string tcp;
    ////调度器节点不包含links信息
    Node* scheduler;
    string linksIP[maxN];
    int linksNum = 0;
    int serverNum;
    int workerNum;
    string getStr(string str, int* cur, char delim) {
        int first = *cur;
        while(str[*cur] != delim) {
            (*cur)++;
        }
        int n = (*cur) - first;
        (*cur)++;
        return str.substr(first, n);
    }
    Node(){

    }
    Node(string name, int id, string host, string port, int serverNum, int workerNum){
        this->name = name;
        this->id = id;
        this->host = host;
        this->port = port;
        this->serverNum = serverNum;
        this->workerNum = workerNum;
        this->tcp = "tcp://" + host + ":" + port;
    }
    Node(string str) {
        int cur = 0;
        this->name = getStr(str, &cur, ',');
        this->id = atoi(getStr(str, &cur, ',').c_str());
        this->host = getStr(str, &cur, ',');
        this->port = getStr(str, &cur, ',');
        this->serverNum = atoi(getStr(str, &cur, ',').c_str());
        this->workerNum = atoi(getStr(str, &cur, '@').c_str());
        Node* node =new  Node();
        node->name = getStr(str, &cur, ',');
        node->id = atoi(getStr(str, &cur, ',').c_str());
        node->host = getStr(str, &cur, ',');
        node->port = getStr(str, &cur, '@');
        this->scheduler = node;
        int len = str.size();
        while(cur < len) {
            this->addLinks(getStr(str, &cur, ','));
        }
        this->tcp = "tcp://" + host + ":" + port;
    }
    void addLinks(string ip) {
        linksIP[linksNum] = ip;
        linksNum ++;
    }
    string getTCP() {
        if(tcp.size() == 0) {
            tcp = "tcp://" + host + ":" + port;
        }
        return tcp;
    }
    string toString(){
        string str = name + "," + to_string(id) + "," + host + "," + port + "," + to_string(serverNum) + "," + to_string(workerNum) + "@";
        string schedulerStr = scheduler->name + "," + to_string(scheduler->id) + "," + scheduler->host + "," + scheduler->port + "@";
        str = str + schedulerStr;
        for(int i=0; i < linksNum; i++) {
            str  = str + linksIP[i] + ",";
        }
        return str;
    }

};
#endif //PS_DATA_H
