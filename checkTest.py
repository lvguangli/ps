import pandas as pd
import csv
import math


def sigmoid(inX):
    # 1 / (1 + exp(-z));
    try:
        s = 1.0 / (1 + math.exp(-inX))
    except:
        print "error:" + str(inX)
        return 0
    return s


def test(test_file=None, weight_file=None):
    weight_data = list()
    with open(weight_file, 'r') as f:
        temp = csv.reader(f)
        for row in temp:
            for weight in row:
                weight_data.append(float(weight))

    print("load weight_data success")
    test_datas = list()
    with open(test_file, 'r') as f:
        temp = csv.reader(f)
        for row in temp:
            tmp = list()
            for weight in row:
                tmp.append(float(weight))
            test_datas.append(tmp)

    print("load test_datas success")
    row_num = len(test_datas)
    predict_correct = 0
    count1 = 0
    count11 = 0
    counto = 0
    length = len(test_datas[0])
    for line in test_datas:
        result = 0.0
        for i in range(0, length - 1, 1):
            result += line[i] * weight_data[i]
        predict = 0
        s = sigmoid(result)
        if s > 0.5:
            predict = 1
        if predict == 1:
            count1 += 1
        if 1 == int(line[length - 1]):
            counto += 1
        if predict == int(line[length - 1]):
            predict_correct += 1
            if predict == 1:
                count11 += 1
    print("testNum:" + str(row_num))
    print("matchNum:" + str(predict_correct))
    print("count1:" + str(count1))
    print("count11:" + str(count11))
    print("counto:" + str(counto))
    print("correctRatio:" + str(float(predict_correct)/row_num))


if __name__ == '__main__':
    test("data/test.csv", "output/weight_200.txt")