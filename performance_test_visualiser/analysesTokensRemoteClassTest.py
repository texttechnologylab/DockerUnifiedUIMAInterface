import sqlite3
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
import os



class Visualisation:
    def __init__(self, myindex):
        plt.figure(figsize=(40, 20))
        self.test_number = '0'
        self.token_numbers = '010'
        self.labels = []
        self.lines = []
        self.myindex = myindex
        self.dbNames = []
        self.dbNames_min_max = []
        self.all =["pipelinename" ,

                    "componenthash" , # 1

                    "durationSerialize" , # 2

                    "durationDeserialize" , # 3

                    "durationAnnotator" , # 4

                    "durationMutexWait" , # 5

                    "durationComponentTotal" , # 6

                    "totalAnnotations" , # 7

                    "documentSize" , # 8

                    "serializedSize", # 9
                   "remote" #10
                          ]


    def addLine(self, path):
        self.dbNames.append(path[-20:])
        connction = sqlite3.connect(path)
        cursor = connction.cursor()
        pipeline_perf = cursor.execute("SELECT * FROM pipeline_perf;").fetchall()
        pipeline_document_perf = cursor.execute("SELECT * FROM pipeline_document_perf;").fetchall()
        wsTotal = []
        # print(pipeline_document_perf)
        for perf in pipeline_document_perf:
            # print((perf[0], perf[7]))
            wsTotal.append((perf[8], perf[self.myindex]))
        wsTotal = sorted(wsTotal, key=lambda tup: tup[0])
        ws_values = []
        for element in wsTotal:
            if len(self.labels) < len(wsTotal):
                self.labels.append(element[0])
            ws_values.append(element[1])
        self.lines.append(ws_values)

    def plot(self):
        plt.title(self.all[self.myindex]+": remote", fontsize=40, fontweight="bold", pad=40)
        counter = 0
        for value in self.lines:
            plt.plot(self.labels[1:], value[1:],
                     # color='none',
                     label=str(str(self.dbNames[counter][:2] if self.dbNames[counter][2]=="_" else self.dbNames[counter])),
                     linestyle='dashed',
                     linewidth=3,
                     marker='o',
                     markersize=10,
                     # markerfacecolor='blue',
                     # markeredgecolor='blue'
                     )
            counter+=1
        plt.legend(loc='upper left', fontsize=15)
        plt.xticks(np.arange(min(self.labels), max(self.labels), step=4000))
        plt.xlabel(
            "-------------------------------------------- Document size -------------------------------------------->",
            fontsize=16, fontweight="bold", labelpad=30)
        plt.ylabel("--------- " + self.all[self.myindex] + " --------->", fontsize=16, fontweight="bold", labelpad=30)
        dir = 'results'
        newdir = dir+'/'+self.token_numbers+'/'+self.test_number+''
        if not os.path.exists(newdir):
            if not os.path.exists(dir+'/'+self.token_numbers+''):
                os.mkdir(dir+'/'+self.token_numbers+'')
            else:
                os.mkdir(dir+'/'+self.token_numbers+'/'+self.test_number+'')

        plt.savefig('results/'+self.token_numbers+'/'+self.test_number+'/plot.pdf')
        #plt.savefig('./remote.pdf')








    def gradientDescentCalcu(self, lx, x_train, y_train):
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
        # print(f"slope is {m}")
        # print(f"intercept is {c}")
        return {"m":m, "c":c}

    def gradientDescent(self):
        x = np.array(self.labels[1:])
        x = list(map(lambda x: x / 10000, x))
        x_train = ""
        x_test = ""
        lx = ""
        mAndC_s = [] # hier werden slope m und intercept c gespeichert
        y_s = []
        y_pred_s = []
        for line in self.lines:
            y = np.array(line[1:])
            y = list(map(lambda x: x / 10000, y))
            y_s.append(y)
            x_train, x_test, y_train, y_test = train_test_split(x, y, random_state=16)
            lx = len(x_train)
            mAndC = self.gradientDescentCalcu(lx, x_train, y_train)
            mAndC_s.append(mAndC)

        print(mAndC_s)
        m_min = min(list(map(lambda x: x['m'], mAndC_s)))
        print(m_min)

        m_max = max(list(map(lambda x: x['m'], mAndC_s)))
        print(m_max)

        new_mAndC_s_with_min_max = []
        for mc in mAndC_s:
            if mc['m']==m_min:
                new_mAndC_s_with_min_max.append(mc)
                index = mAndC_s.index(mc)
                print(index)
                self.dbNames_min_max.append(self.dbNames[index])
            if mc['m']==m_max:
                new_mAndC_s_with_min_max.append(mc)
                index = mAndC_s.index(mc)
                print(index)
                self.dbNames_min_max.append(self.dbNames[index])
        print(new_mAndC_s_with_min_max)


        for m_c in new_mAndC_s_with_min_max:
            y_pred = np.dot(m_c["m"], x_test) + m_c["c"]
            y_pred_s.append(y_pred)




        for i in range(len(new_mAndC_s_with_min_max)):
            plt.plot(x_test, y_pred_s[i],
                     # color='none',
                     label= str(self.dbNames_min_max[i]+" "+str(int(new_mAndC_s_with_min_max[i]['m'])/10000)),
                     linestyle='dashed',
                     linewidth=3,
                     marker='o',
                     markersize=10,
                     # markerfacecolor='blue',
                     # markeredgecolor='blue'
                     )
            # plt.scatter(x, y_s[i])


        plt.title(self.all[self.myindex]+": remote => Gradient descent", fontsize=40, fontweight="bold", pad=40)

        plt.legend(loc='upper left', fontsize=15)
        plt.xticks(np.arange(min(self.labels)/10000, max(self.labels)/10000, step=0.4))
        plt.xlabel(
            "-------------------------------------------- Document size -------------------------------------------->",
            fontsize=16, fontweight="bold", labelpad=30)
        plt.ylabel("--------- " + self.all[self.myindex] + " --------->", fontsize=16, fontweight="bold", labelpad=30)
        # plt.show()
        dir = 'results'
        newdir = dir+'/'+self.token_numbers+'/'+self.test_number+''
        if not os.path.exists(newdir):
            os.mkdir(dir+'/'+self.token_numbers+'/')

        plt.savefig('results/'+self.token_numbers+'/gradientDescent.pdf')


visualisation = Visualisation(6)
visualisation.addLine("../performance_dbs/local/websocket/010/0/performance.db")
visualisation.addLine("../performance_dbs/local/websocket/010/1/performance.db")
visualisation.addLine("../performance_dbs/local/websocket/010/2/performance.db")
visualisation.addLine("../performance_dbs/local/websocket/010/3/performance.db")
visualisation.addLine("../performance_dbs/local/websocket/010/4/performance.db")
visualisation.addLine("../performance_dbs/local/websocket/010/5/performance.db")
visualisation.addLine("../performance_dbs/local/websocket/010/6/performance.db")
visualisation.addLine("../performance_dbs/local/websocket/010/7/performance.db")
visualisation.addLine("../performance_dbs/local/websocket/010/8/performance.db")
visualisation.addLine("../performance_dbs/local/websocket/010/9/performance.db")


# print(visualisation.all)
# print(visualisation.myindex)
# visualisation.addLine("../rest_remote_1.db")
# visualisation.addLine("../ws_remote_1.db")
# visualisation.addLine("../websocket_token_open_60.db")
# visualisation.addLine("../websocket_token_open_50.db")
# visualisation.addLine("../websocket_token_open_25.db")
# visualisation.addLine("../websocket_token_open_15.db")
# print(visualisation.labels)
# print(visualisation.lines)
visualisation.gradientDescent()