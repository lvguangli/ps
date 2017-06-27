//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_DATA_H
#define PS_DATA_H
#include <iostream>
#include <cstring>
using namespace std;
int maxN = 3;
class Node {
public:
    string name;
    string host;
    string port;
    string schedulerIP;
    string* linksIP;
    int linksNum = 0;
    Node(string name, string host, string port, string schedulerIP){
        this->name = name;
        this->host = host;
        this->port = port;
        this->schedulerIP = schedulerIP;
        linksIP = new string[maxN];
        linksNum = 0;
    }
    void addLinks(string ip) {
        linksIP[linksNum] = ip;
        linksNum ++;
    }
    string getTCP() {
        return "tcp://" + host + ":" + port;
    }
    string toString(){
        string str = name + ' ' + host + ' ' + port + ' ' + schedulerIP;
        for(int i=0; i < linksNum; i++) {
            str  = str + ' ' + linksIP[i];
        }
        return str;
    }
};
#endif //PS_DATA_H
