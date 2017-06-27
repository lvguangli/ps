//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_MESSAGE_H
#define PS_MESSAGE_H

#include <string>
using namespace std;
enum msg_type {
    OK, // 消息接受
    ADDNODE, // 添加节点
    PUSH,   // worker节点push计算完成后的参数
    PULL,   // worker节点pull源数据数据
    STOP    // 调度器终止计算
};

class Message {
public:
    //消息类型，addnode, pull,push,stop,ok
    msg_type type;
    // 负责处理的数据下标起始结束位置
    int start;
    int end;
    // 参数的个数
    int len;
    // 参数数组
    int msg[512];
};
#endif //PS_MESSAGE_H
