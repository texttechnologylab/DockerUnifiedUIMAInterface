import sqlite3
import numpy as np
import matplotlib.pyplot as plt



class Visualisation:
    def __init__(self, myindex):
        plt.figure(figsize=(40, 20))
        self.labels = []
        self.lienes = []
        self.myindex = myindex
        self.dbNames = []
        self.all =["pipelinename" ,

                    "componenthash" , # 1

                    "durationSerialize" , # 2

                    "durationDeserialize" , # 3

                    "durationAnnotator" , # 4

                    "durationMutexWait" , # 5

                    "durationComponentTotal" , # 6

                    "totalAnnotations" , # 7

                    "documentSize" , # 8

                    "serializedSize" # 9
                          ]


    def addLine(self, path):
        self.dbNames.append(path[-6:-3])
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
        self.lienes.append(ws_values)

    def plot(self):
        plt.title(self.all[self.myindex], fontsize=40, fontweight="bold", pad=40)
        counter = 0
        for value in self.lienes:
            plt.plot(self.labels, value,
                     # color='none',
                     label='WS '+str(self.dbNames[counter]),
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
        plt.savefig('./figure_1.pdf')


visualisation = Visualisation(6)
# print(visualisation.all)
# print(visualisation.myindex)
# visualisation.addLine("../websocket_token_open_200.db")
# visualisation.addLine("../websocket_token_open_100.db")
# visualisation.addLine("../websocket_token_open_50.db")
visualisation.addLine("../websocket_token_open_25.db")
visualisation.addLine("../websocket_token_open_15.db")
# print(visualisation.labels)
# print(visualisation.lienes)
print(visualisation.dbNames)
visualisation.plot()