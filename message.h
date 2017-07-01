//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_MESSAGE_H
#define PS_MESSAGE_H

#include <string>
#include<string.h>
#include <vector>
#include "log.h"
#include "StringBuilder.h"
#include <cstring>
using namespace std;
enum msg_type {
    UNKNOWN, //未知消息
    OK, // 消息接受
    ADDNODE, // 添加节点
    PUSH,    // worker节点push计算完成后的参数
    PULL,    // worker节点pull源数据数据
    STOP,    // 调度器终止计算
    ERROR,   // 接受错误
    ITERMSG, // 更新iterator
    RESTART    // 调度器通知节点从断点继续执行任务
};

//class Error {
//public:
//    //消息类型，addnode, pull,push,stop,ok
//    msg_type type;
//    // 负责处理的数据下标起始结束位置
//    int start;
//    int end;
//    // 参数的个数
//    // 参数数组
//    vector<double> error;
//    void initError(int len) {
//        error.reserve(len);
//    }
//    void setError(int start, int end, vector<double>e) {
//        for(int i = start, j =0; i < end; i++, j++) {
//            error[i] = e[j];
//        }
//    }
//};
//
//class Weight {
//public:
//    //消息类型，addnode, pull,push,stop,ok
//    msg_type type;
//    // 负责处理的数据下标起始结束位置
//    int start;
//    int end;
//    // 参数的个数
//    // 参数数组
//    vector<double> weight;
//    void initWeight(int len) {
//        weight.reserve(len);
//    }
//    void setWeight(int start, int end, vector<double>w) {
//        for(int i = start, j =0; i < end; i++, j++) {
//            weight[i] = w[j];
//        }
//    }
//};

class Data {
private:
    string d2s(double a) {
        //TODO 是否可以优化
        string s = to_string(a);
        if(s.find('.')) {
            int right = s.size() - 1;
            while(s[right] == '0') {
                right--;
            }
            if(s[right] == '.')
                right--;
            s = s.substr(0, right + 1);
        }
        return s;
    }

//    char* d2c() {
//        char* point = strchr(buffer, '.');
//        if(!point) return;
//    }

    int getInt(char* str, int* cur, char delim){
        char word[100];
        int index = 0;
        while(str[*cur] != delim) {
            word[index] = str[*cur];
            index ++;
            (*cur)++;
        }
        word[index] = '\0';
        (*cur)++;
        return atoi(word);
    }

    long getLong(char* str, int* cur, char delim){
        char word[100];
        int index = 0;
        while(str[*cur] != delim) {
            word[index] = str[*cur];
            index ++;
            (*cur)++;
        }
        word[index] = '\0';
        (*cur)++;
        return atol(word);
    }

    double getDouble(char* str, int* cur, char delim){
        char word[100];
        int index = 0;
        while(str[*cur] != delim) {
            word[index] = str[*cur];
            index ++;
            (*cur)++;
        }
        word[index] = '\0';
        (*cur)++;
        return atof(word);
    }

public:
    //消息类型，addnode, pull,push,stop,ok
    msg_type type;
    // 负责处理的数据下标起始结束位置
    int start;
    int end;
    long timeStamp;
    // 参数的个数
    // 参数数组
    vector<vector<double>> data;
    void initData(int len) {
        data.reserve(len);
        for(int i =0; i< len; i++) {
            vector<double> vector2;
            data.push_back(vector2);
        }
    }

    void initData(int len, int value) {
        data.reserve(len);
        for(int i =0; i< len; i++) {
            vector<double> vector2;
            vector2.push_back(value);
            data.push_back(vector2);
        }
    }

    void addData(int start, int end, vector<vector<double>> d) {
        for(int i = start; i < end; i++) {
            data[i-this->start] = d[i - start];
        }
    }

    string head() {
        StringBuilder<char> sb = StringBuilder<char>();
        sb.Append(to_string(type));
        sb.Append(",");
        sb.Append(to_string(start));
        sb.Append(",");
        sb.Append(to_string(end));
        sb.Append(",");
        sb.Append(to_string(timeStamp));
        sb.Append(";");
        string str = sb.toString();
        return sb.toString();
    }

//    string toString() {
//        StringBuilder<char> sb = StringBuilder<char>();
//        sb.Append(to_string(type));
//        sb.Append(",");
//        sb.Append(to_string(start));
//        sb.Append(",");
//        sb.Append(to_string(end));
//        sb.Append(",");
//        sb.Append(to_string(timeStamp));
//        sb.Append(";");
//        int first = data.size();
//        if(first > 0) {
//            int second = data[0].size();
//            for(int i = 0; i < first ; i++) {
//                for(int j = 0; j < second; j++) {
//                    sb.Append(d2s(data[i][j]));
//                    if(j == second - 1) {
//                        sb.Append(";");
//                    }
//                    else {
//                        sb.Append(",");
//                    }
//                }
//            }
//        }
//        return sb.toString();
//    }

    int save2Buffer(char * buffer) {
        string headMsg = head();
        int len = headMsg.size();
        memcpy(buffer, headMsg.c_str(), len);
        char* ptr = buffer + len;
        int first = data.size();
        if(first > 0) {
            char tmp[20];
            int second = data[0].size();
            for(int i = 0; i < first ; i++) {
                for(int j = 0; j < second; j++) {
                    sprintf(tmp, "%f", data[i][j]);
                    int right = strlen(tmp) - 1;
                    char* point = strchr(tmp, '.');
                    if(point != NULL) {
                        while(tmp[right] == '0') {
                            tmp[right] = '\0';
                            right--;
                        }
                        if(tmp[right] == '.') {
                            tmp[right] = '\0';
                            right--;
                        }
                    }
                    memcpy(ptr, tmp, right + 1);
                    ptr += right + 1;
                    if(j == second - 1) {
                        *ptr = ';';
                    }
                    else {
                        *ptr = ',';
                    }
                    ptr += 1;
                }
            }
        }
        *ptr = '\0';
        return ptr - buffer;
    }

    Data() {

    }

    void parseData(char* str) {
        if(strlen(str) == 0) {
            this->type = UNKNOWN;
            return;
        }
        while(str[strlen(str) - 1] != ';') {
            str[strlen(str) - 1] = '\0';
        }
        int len = strlen(str);
        int cur = 0;
        int one = getInt(str, &cur, ',');
        type = msg_type(one);
        start = getInt(str, &cur, ',');
        end = getInt(str, &cur, ',');
        timeStamp = getLong(str, &cur, ';');
        if(cur >= len) {
            return;
        }
        int second = 1;
        int k = cur;
        while(str[k] != ';') {
            if(str[k] == ',') {
                second ++;
            }
            k++;
        }
        int count = 0;
        int num = end - start;
        if(count >= num) {
            return;
        }
        while(cur < len) {
            vector<double> vector2;
            for(int i = 0; i< second - 1; i++) {
                vector2.push_back(getDouble(str, &cur, ','));
            }
            vector2.push_back(getDouble(str, &cur, ';'));
            data.push_back(vector2);
            count++;
            if(count >= num) {
                break;
            }
        }
//        if(str.size() == 0) {
//            return;
//        }
//        while(str[str.size() - 1] != ';') {
//            str = str.substr(0, str.size() - 1);
//        }
//        int cur = 0;
//        int one = getInt(str, &cur, ',');
//        type = msg_type(one);
//        start = getInt(str, &cur, ',');
//        end = getInt(str, &cur, ',');
//        timeStamp = getLong(str, &cur, ';');
//        if(cur >= str.size()) {
//            return;
//        }
//        int second = 1;
//        int k = cur;
//        while(str[k] != ';') {
//            if(str[k] == ',') {
//                second ++;
//            }
//            k++;
//        }
//        int len = str.size();
//        int count = 0;
//        int num = this->end - this->start;
//        if(count >= num) {
//            return;
//        }
//        while(cur < len) {
//            vector<double> vector2;
//            for(int i = 0; i< second - 1; i++) {
//                vector2.push_back(getDouble(str, &cur, ','));
//            }
//            vector2.push_back(getDouble(str, &cur, ';'));
//            data.push_back(vector2);
//            count++;
//            if(count >= num) {
//                break;
//            }
//        }
    }

    Data(char* str) {
        parseData(str);

    }
};

class LinkMsg {
public:
    unordered_map<int,void*>* sockets;
    vector<Data*>* msgList;
    vector<int>*  msgArray;
    void* socket;
    string file;
    LinkMsg() {

    }
    LinkMsg(LinkMsg* linkMsg) {
        this->sockets = linkMsg->sockets;
        this->msgList = linkMsg->msgList;
        this->msgArray = linkMsg->msgArray;
//        this->socket = linkMsg->socket;
        this->file = linkMsg->file;
    };
};
#endif //PS_MESSAGE_H
