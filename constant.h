//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_CONSTANT_H
#define PS_CONSTANT_H
#include<cstring>
#include<string.h>
#include <string>

using namespace std;

int serverN = 2;
int workerN = 2;

string shosts[][2] = {{"192.168.1.101","12312"}, {"192.168.1.100","12307"}};
string whosts[][2] = {{"192.168.1.100","12308"}, {"192.168.1.100","12309"}};
string addhost = "tcp://ip1:port1";
string schedulerIP[2] = {"192.168.1.100","12400"};
#endif //PS_CONSTANT_H
