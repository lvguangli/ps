import pandas as pd
import csv


def pretreat(train_file=None, test_file=None, dir=''):
    train = pd.read_csv(train_file)
    test = pd.read_csv(test_file)
    col_name = list()
    c_num = train.columns.shape[0]
    for i in range(c_num):
        col_name.append(i)
    train.columns = col_name

    col_name = list()
    c_num = test.columns.shape[0]
    for i in range(c_num):
        col_name.append(i)
    test.columns = col_name

    data = pd.concat([train, test])

    label = data[41]
    del data[41]
    print(data.columns)
    print ("origin:"+str(data.columns.shape[0]))
    data = pd.get_dummies(data)
    print("get_dummies:"+str(data.columns.shape[0]))
    label = pd.get_dummies(label)
    ll = list()
    data_num = data.index.shape[0]
    # count1 = 0
    for i in range(data_num):
        if label.iat[i, 0]:
            # iat = indexat
            ll.append(0)
        else:
            ll.append(1)
            # if i > data_num/3*2:
            #     count1 += 1
    # print "test count1="+str(count1)

    print(data.columns.shape[0])
    print(data.index.shape[0])
    num = data.index.shape[0]
    train = data[0:num/3*2]
    print(train.columns.shape[0])
    print(train.index.shape[0])
    test = data[num/3*2:num]
    print(test.columns.shape[0])
    print(test.index.shape[0])

    train.to_csv(dir+'tmp_train.txt', index=False, header=False, sep=",")
    test.to_csv(dir+'tmp_test.txt', index=False, header=False, sep=",")

    index = 0
    label = ll

    train_data = list()
    with open(dir+'tmp_train.txt', 'r') as f:
        temp = csv.reader(f)
        for row in temp:
            tmp = list()
            for value in row:
                tmp.append(value)
            tmp.append(label[index])
            index += 1
            train_data.append(tmp)
    with open(dir+'train.csv', 'w') as output:
        writer = csv.writer(output)
        for item in train_data:
            writer.writerow(item)

    print("index=" + str(index))

    test_data = list()
    with open(dir+'tmp_test.txt', 'r') as f:
        temp = csv.reader(f)
        for row in temp:
            tmp = list()
            for value in row:
                tmp.append(value)
            tmp.append(label[index])
            index += 1
            test_data.append(tmp)
    with open(dir+'test.csv', 'w') as output:
        writer = csv.writer(output)
        for item in test_data:
            writer.writerow(item)
    print("index=" + str(index))

if __name__ == '__main__':
    pretreat('census-income.data', 'census-income.test', 'data/')

