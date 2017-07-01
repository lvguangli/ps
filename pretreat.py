import pandas as pd


def proc(file=None, type=None):
    data = pd.read_csv(file)
    col_name = list()
    c_num = data.columns.shape[0]
    for i in range(c_num):
        col_name.append(i)
    data.columns = col_name
    label = data[41]
    # del data[40]
    del data[41]
    print(data.columns)
    print ("origin:"+str(data.columns.shape[0]))
    data = pd.get_dummies(data)
    print("get_dummies:"+str(data.columns.shape[0]))
    label = pd.get_dummies(label)
    ll = list()
    data_num = data.index.shape[0]
    for i in range(data_num):
        if label.iat[i, 0]:
            # iat = indexat
            ll.append(0)
        else:
            ll.append(1)
    ll = pd.Series(ll)
    train = data
    label = ll
    data[data.columns.shape[0]] = label[0]
    print(data.columns.shape[0])
    print(data.index.shape[0])
    data.to_csv(type+".txt", index=False, header=False, sep=" ")


if __name__ == '__main__':
    proc("census-income.data", "train")
    proc("census-income.test", "test")
# print(train.index) =  RangeIndex(start=0, stop=199522, step=1)
    # print(train.columns)
    # Index([u'train.columns:73', u'train.columns: Not in universe',
    #        u'train.columns: 0', u'train.columns: 0.1',
    #        u'train.columns: High school graduate', u'train.columns: 0.2',
    #        u'train.columns: Not in universe.1', u'train.columns: Widowed',
    #        u'train.columns: Not in universe or children',
    #        u'train.columns: Not in universe.2', u'train.columns: White',
    #        u'train.columns: All other', u'train.columns: Female',
    #        u'train.columns: Not in universe.3',
    #        u'train.columns: Not in universe.4',
    #        u'train.columns: Not in labor force', u'train.columns: 0.3',
    #        u'train.columns: 0.4', u'train.columns: 0.5',
    #        u'train.columns: Nonfiler', u'train.columns: Not in universe.5',
    #        u'train.columns: Not in universe.6',
    #        u'train.columns: Other Rel 18+ ever marr not in subfamily',
    #        u'train.columns: Other relative of householder',
    #        u'train.columns: 1700.09', u'train.columns: ?', u'train.columns: ?.1',
    #        u'train.columns: ?.2',
    #        u'train.columns: Not in universe under 1 year old',
    #        u'train.columns: ?.3', u'train.columns: 0.6',
    #        u'train.columns: Not in universe.7', u'train.columns: United-States',
    #        u'train.columns: United-States.1', u'train.columns: United-States.2',
    #        u'train.columns: Native- Born in the United States',
    #        u'train.columns: 0.7', u'train.columns: Not in universe.8',
    #        u'train.columns: 2', u'train.columns: 0.8', u'train.columns: 95',
    #        u'train.columns: - 50000.'],
    #       dtype='object')
