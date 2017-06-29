//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_LOG_H
#define PS_LOG_H
#include<iostream>
#include<fstream>
using namespace std;
void log(string file, int index){
    ofstream out(file + to_string(index) + ".txt",ios::out);
    out<<"";
    out.close();
}
void log(string content,string file, int index)
{
    ofstream out(file  + to_string(index) + ".txt",ios::app);
    out<<content<<endl;
    out.close();
}
void log(char* content, string file, int index)
{
    ofstream out(file  + to_string(index) + ".txt",ios::app);
    out<<content<<endl;
    out.close();
}
#endif //PS_LOG_H
