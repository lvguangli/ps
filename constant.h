//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_CONSTANT_H
#define PS_CONSTANT_H
#include<cstring>
#include<string.h>
#include <string>
#include<iostream>
#include<fstream>

using namespace std;

//const int M = 199522;
//const int N = 410;
//const string trainFile = "data/train.txt";
//static int ITERATOR = 50;
static int M = 32;
static int N = 410;
static string trainFile = "train.txt";
static int ITERATOR = 10;

static string shosts[][2] = {{"192.168.1.100","14535"}, {"192.168.1.100","14331"}};
static string whosts[][3] = {{"192.168.1.100","12642"}, {"192.168.1.100","12643"}, {"192.168.1.100","12644"}};
static string schedulerIP[2] = {"192.168.1.100","12400"};

static string configFile = "config.conf";
string getStr(string str, int* cur, char delim) {
    int first = *cur;
    while(str[*cur] != delim) {
        (*cur)++;
    }
    int n = (*cur) - first;
    (*cur)++;
    return str.substr(first, n);
}

string getStr(char* tmp, int* cur, char delim) {
    string str = tmp;
    return getStr(str, cur, delim);
}
void initConfig() {
//        rowM 32;
//        colN 410;
//        trainFile train.txt;
//        ITERATOR 50;
//        shosts 192.168.1.100;14535;192.168.1.100;14331;
//        whosts 192.168.1.100;12642;192.168.1.100;12643;192.168.1.100;12644;
//        scheduler 192.168.1.100;12400;
    ifstream input(configFile);
    char line[1000];
    while(input.getline(line, 1000)) {
        cout<<line<<endl;
        int cur = 0;
        string name = getStr(line, &cur, ' ');
        if(name[0] == '#') {
            continue;
        }
        if(strcmp(name.c_str(), "rowM") == 0) {
            M = atoi(getStr(line, &cur, ';').c_str());
        } else if(strcmp(name.c_str(), "colN") == 0) {
            N = atoi(getStr(line, &cur, ';').c_str());
        } else if(strcmp(name.c_str(), "trainFile") == 0) {
            trainFile = getStr(line, &cur, ';').c_str();
        } else if(strcmp(name.c_str(), "ITERATOR") == 0) {
            ITERATOR = atoi(getStr(line, &cur, ';').c_str());
        } else if(strcmp(name.c_str(), "serverHosts") == 0) {
            int len = strlen(line);
            int start = 0;
            while(cur < len) {
                shosts[start/2][start%2] = getStr(line, &cur, ';');
                start++;
            }
        } else if(strcmp(name.c_str(), "workerHosts") == 0) {
            int len = strlen(line);
            int start = 0;
            while(cur < len) {
                whosts[start/2][start%2] = getStr(line, &cur, ';');
                start++;
            }
        } else if(strcmp(name.c_str(), "schedulerHosts") == 0) {
            int len = strlen(line);
            int start = 0;
            while(cur < len) {
                schedulerIP[start] = getStr(line, &cur, ';');
                start++;
            }
        }
    }
    cout<<"ITERATOR="<<ITERATOR<<endl;
}
//ifstream input(train);


#endif //PS_CONSTANT_H
