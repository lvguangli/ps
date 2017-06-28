//
// Created by Sahara on 27/06/2017.
//

#ifndef PS_LOG_H
#define PS_LOG_H
#include<iostream>
#include<fstream>
using namespace std;
void log(string file){
    ofstream out(file,ios::out);
    out<<"";
    out.close();
}
void log(string content,string file)
{
    ofstream out(file,ios::app);
    out<<content<<endl;
    out.close();
}
void log(char* content, string file)
{
    ofstream out(file,ios::app);
    out<<content<<endl;
    out.close();
}
#endif //PS_LOG_H
