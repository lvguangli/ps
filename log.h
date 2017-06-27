//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_LOG_H
#define PS_LOG_H
#include<iostream>
#include<fstream>
using namespace std;
void log(string content)
{
    ofstream out("log",ios::app);
    out<<content<<endl;
    out.close();
}
void log(char* content)
{
    ofstream out("log",ios::app);
    out<<content<<endl;
    out.close();
}
#endif //PS_LOG_H
