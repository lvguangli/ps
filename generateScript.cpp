#include <iostream>
#include<unordered_map>
#include "node.h"
#include "constant.h"
#include "log.h"
#include "util.h"


using namespace std;

unordered_map<int, Node*> workers;
unordered_map<int, Node*> servers;
Node* scheduler;
string file = "generateScript";
int mainId = 6;

void generateScript(char argv[3][10]) {
    int serverNum = atoi(argv[0]);
    int workerNum = atoi(argv[1]);
    scheduler = new Node("scheduler",9,schedulerIP[0], schedulerIP[1], serverNum, workerNum);
    scheduler->scheduler = scheduler;
    for (int i =0; i<serverNum ; i++){
        string host = shosts[i][0];
        string port = shosts[i][1];
        string name = "server" + to_string(i);
        Node* nodePtr = new Node(name,i, host, port, serverNum, workerNum);
        nodePtr ->scheduler = scheduler;
        servers[i] = nodePtr;
    }
    for (int i =serverNum; i<serverNum + workerNum; i++){
        string host = whosts[i - serverNum][0];
        string port = whosts[i - serverNum][1];
        string name = "worker" + to_string(i - serverNum);
        if( i == 5) {
            name = "newWorker" + to_string(i - serverNum);
        }
        Node* nodePtr = new Node(name, i- serverNum, host, port, serverNum, workerNum);
        nodePtr ->scheduler = scheduler;
        workers[i - serverNum] = nodePtr;
    }
    for(int i = 0; i < serverNum; i++) {
        for(int j = 0; j < workerNum; j++) {
            bindSW(servers[i], workers[j]);
        }
    }

    for(int i = 0; i < serverNum; i ++) {
        scheduler->addLinks(servers[i]->getTCP());
    }
    for(int i = 0; i < workerNum; i ++) {
        scheduler->addLinks(workers[i]->getTCP());
    }
    if(strcmp(argv[2], "start") == 0) {
        for(int i = 0; i < servers.size(); i++) {
            generateNodeShell(servers[i],"");
        }
        for(int i = 0; i < workers.size(); i++) {
            generateNodeShell(workers[i],"");
        }
    }
    else if(strcmp(argv[2], "middle") == 0){
        generateNodeShell(workers[workerNum - 1],"");
        generateNodeShell(scheduler, "AddNode");
        generateNodeShell(scheduler, "StopNode");
    }
    else {
        log(argv[2], file, mainId);
    }
}

int main(int argc, char*argv[]) {
    log(file, mainId);
    char start[3][10] = {"2","2","start"};
    char middle[3][10] = {"2","3","middle"};
    generateScript(start);
    generateScript(middle);

    cout << "Hello, World!" << std::endl;
    return 0;
}