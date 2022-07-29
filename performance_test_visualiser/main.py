# C:/Users/Givo/Desktop/2022/Deep Learning for Text Imaging/praktikum_new/DockerUnifiedUIMAInterface/websocket_rest_test.db
import sqlite3
connction = sqlite3.connect("../websocket_rest_test1.db")
cursor = connction.cursor()
# show all tables
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# [('pipeline',), ('pipeline_perf',), ('pipeline_component',), ('pipeline_document',), ('pipeline_document_perf',)]


# show all data
cursor.execute("SELECT * FROM pipeline_perf;")

#cursor.execute("SELECT * FROM pipeline_document_perf;")
if __name__ == '__main__':
    # print(connction)
    print(cursor.fetchall())

