import sqlite3
import numpy as np
import matplotlib.pyplot as plt

connction_with_50 = sqlite3.connect("../websocket_token_open_50.db")
connction_with_25 = sqlite3.connect("../websocket_token_open_25.db")
connction_with_15 = sqlite3.connect("../websocket_token_open_15.db")
cursor_50 = connction_with_50.cursor()
cursor_25 = connction_with_25.cursor()
cursor_15 = connction_with_15.cursor()
# show all tables
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# [('pipeline',), ('pipeline_perf',), ('pipeline_component',), ('pipeline_document',), ('pipeline_document_perf',)]


# show all data
pipeline_perf_50 = cursor_50.execute("SELECT * FROM pipeline_perf;").fetchall()
pipeline_perf_25 = cursor_25.execute("SELECT * FROM pipeline_perf;").fetchall()
pipeline_perf_15 = cursor_15.execute("SELECT * FROM pipeline_perf;").fetchall()

pipeline_document_perf_50 = cursor_50.execute("SELECT * FROM pipeline_document_perf;").fetchall()
pipeline_document_perf_25 = cursor_25.execute("SELECT * FROM pipeline_document_perf;").fetchall()
pipeline_document_perf_15 = cursor_15.execute("SELECT * FROM pipeline_document_perf;").fetchall()
# print(rest_pipeline_document_perf)
# print(len(rest_pipeline_document_perf))



myindex = 6
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


wsTotal50 = []
print(pipeline_document_perf_50)
for perf in pipeline_document_perf_50:
    # print((perf[0], perf[7]))
    wsTotal50.append((perf[8], perf[myindex]))


wsTotal25 = []
for perf in pipeline_document_perf_25:
    # print((perf[0], perf[7]))
    wsTotal25.append((perf[8], perf[myindex]))
# print(wsDurationComponentTotal)


wsTotal15 = []
for perf in pipeline_document_perf_15:
    # print((perf[0], perf[7]))
    wsTotal15.append((perf[8], perf[myindex]))

# print(restDurationComponentTotal)
wsTotal50 = sorted(wsTotal50, key=lambda tup: tup[0])
wsTotal25 = sorted(wsTotal25, key=lambda tup: tup[0])
wsTotal15 = sorted(wsTotal15, key=lambda tup: tup[0])


ws_values_50 = []
for element in wsTotal50:
    ws_values_50.append(element[1])



ws_values_25 = []
for element in wsTotal25:
    ws_values_25.append(element[1])


labels = []
ws_values_15 = []
for element in wsTotal15:
    labels.append(element[0])
    ws_values_15.append(element[1])




plt.figure(figsize=(90, 45))

plt.title(all[myindex], fontsize=40, fontweight="bold", pad=40)
print(ws_values_50)
plt.plot(labels, ws_values_50,
         #color='none',
         label='WS_50',
         linestyle='dashed',
         linewidth=3,
         marker='o',
         markersize=6,
         markerfacecolor='blue',
         markeredgecolor='blue')
print(ws_values_25)

plt.plot(labels, ws_values_25,
         #color='none',
         label='WS_25',
         linestyle='dashed',
         linewidth=3,
         marker='o',
         markersize=6,
         markerfacecolor='red',
         markeredgecolor='red')
print(ws_values_15)
print(labels)

plt.plot(labels, ws_values_15,
         #color='none',
         label='WS_15',
         linestyle='dashed',
         linewidth=3,
         marker='o',
         markersize=6,
         markerfacecolor='green',
         markeredgecolor='green')

plt.legend(loc='upper left', fontsize=15)
plt.xticks(np.arange(min(labels), max(labels), step=4000))
plt.xlabel("-------------------------------------------- Document size -------------------------------------------->", fontsize=16, fontweight="bold", labelpad=30)
plt.ylabel("--------- "+ all[myindex]+" --------->", fontsize=16, fontweight="bold", labelpad=30)
#plt.show()

plt.savefig('./figure.pdf')
