import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import style
from sklearn.model_selection import train_test_split
import warnings
import sqlite3

style.use('fivethirtyeight')
warnings.filterwarnings("ignore")





def main():
    labels = []
    connction = sqlite3.connect("../websocket_token_open_200.db")
    cursor = connction.cursor()
    pipeline_document_perf = cursor.execute("SELECT * FROM pipeline_document_perf;").fetchall()
    wsTotal = []
    for perf in pipeline_document_perf:
        # print((perf[0], perf[7]))
        wsTotal.append((perf[8], perf[6]))
    wsTotal = sorted(wsTotal, key=lambda tup: tup[0])
    ws_values = []
    for element in wsTotal:
        labels.append(element[0])
        ws_values.append(element[1])

    data = pd.DataFrame(wsTotal[1:])
    #print(data)
    x = np.array(data[0])
    x = list(map(lambda x: x / 10000, x))

    y = np.array(data[1])
    y = list(map(lambda x: x / 10000, y))

    l = len(x)
    # print(x)
    # print(y)
    # print(l)


    x_train, x_test, y_train, y_test = train_test_split(x, y, random_state=32)
    lx = len(x_train)
    print(lx)
    print(x_train)
    print(x_test)
    print(y_train)
    print(y_test)

    m = 0.1
    c = 0.01
    alpha = 0.01
    n = 4000
    for i in range(n):
        slope = 0
        intercept = 0
        for j in range(lx):
            intercept = intercept + ((m * x_train[j] + c) - y_train[j])
            slope = slope + ((m * x_train[j] + c) - y_train[j]) * x_train[j]
        c = c - alpha * (intercept / lx)
        m = m - alpha * (slope / lx)
    print(f"slope is {m}")
    print(f"intercept is {c}")
    y_pred = np.dot(m, x_test) + c
    y_pred

    plt.plot(x_test,y_pred,marker='o',
         color='blue',markerfacecolor='red',
         markersize=10,linestyle='dashed')
    plt.scatter(x,y,marker='o',color='red')
    plt.xlabel("yexp")
    plt.ylabel("slaary")
    plt.title("Gradient descent")
    plt.show()


if __name__ == '__main__':
    main()