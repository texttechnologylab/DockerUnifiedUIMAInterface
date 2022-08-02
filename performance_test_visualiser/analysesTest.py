import sqlite3
import numpy as np
import matplotlib.pyplot as plt

ws_connction = sqlite3.connect("../websocket_test.db")
rest_connction = sqlite3.connect("../rest_test.db")
ws_cursor = ws_connction.cursor()
rest_cursor = rest_connction.cursor()
# show all tables
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# [('pipeline',), ('pipeline_perf',), ('pipeline_component',), ('pipeline_document',), ('pipeline_document_perf',)]


# show all data
ws_pipeline_perf = ws_cursor.execute("SELECT * FROM pipeline_perf;").fetchall()
rest_pipeline_perf = rest_cursor.execute("SELECT * FROM pipeline_perf;").fetchall()

ws_pipeline_document_perf = ws_cursor.execute("SELECT * FROM pipeline_document_perf;").fetchall()
rest_pipeline_document_perf = rest_cursor.execute("SELECT * FROM pipeline_document_perf;").fetchall()
# print(rest_pipeline_document_perf)
# print(len(rest_pipeline_document_perf))

myindex = 9
all =["pipelinename" ,

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
restTotal = []
for perf in rest_pipeline_document_perf:
    # print((perf[0], perf[7]))
    restTotal.append((perf[8], perf[myindex]))

# print(restDurationComponentTotal)
restTotal = sorted(restTotal, key=lambda tup: tup[0])
print(restTotal)

labels = []

rest_values = []
for element in restTotal:
    labels.append(element[0])
    rest_values.append(element[1])

wsTotal = []
for perf in ws_pipeline_document_perf:
    # print((perf[0], perf[7]))
    wsTotal.append((perf[8], perf[myindex]))

# print(wsDurationComponentTotal)
wsTotal = sorted(wsTotal, key=lambda tup: tup[0])
print(wsTotal)


ws_values = []

for element in wsTotal:
    ws_values.append(element[1])

print(labels)
print(rest_values)
print(ws_values)

plt.figure(figsize=(30, 10))

plt.title(all[myindex], fontsize=20, fontweight="bold")

plt.plot(labels, ws_values, label='WS', linestyle='--')
plt.plot(labels, rest_values, label='REST', linestyle='-.')
plt.legend(loc='upper left', fontsize=15)
plt.xticks(np.arange(min(labels), max(labels), step=4000))
plt.xlabel("-------------------------------------------- Document size -------------------------------------------->", fontsize=16, fontweight="bold", labelpad=30)
plt.ylabel("--------- "+ all[myindex]+" --------->", fontsize=16, fontweight="bold", labelpad=30)
plt.show()
