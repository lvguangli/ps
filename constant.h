//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_CONSTANT_H
#define PS_CONSTANT_H
#include<cstring>
#include<string.h>
#include <string>

using namespace std;

const int M = 32;
const int N = 410;

int serverN = 2;
int workerN = 2;
int ITERATOR = 10;

string shosts[][2] = {{"192.168.1.100","12640"}, {"192.168.1.100","12650"}};
string whosts[][2] = {{"192.168.1.100","12642"}, {"192.168.1.100","12643"}};
string addhost = "tcp://ip1:port1";
string schedulerIP[2] = {"192.168.1.100","12400"};
#endif //PS_CONSTANT_H
