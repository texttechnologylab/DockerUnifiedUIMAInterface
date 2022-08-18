# C:/Users/Givo/Desktop/2022/Deep Learning for Text Imaging/praktikum_new/DockerUnifiedUIMAInterface/websocket_rest_test.db
import sqlite3
ws_connction = sqlite3.connect("../websocket_token_open_15.db")
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
if __name__ == '__main__':
    # print(connction)
    print(len(ws_pipeline_document_perf))
   # print(rest_pipeline_perf)
    print(ws_pipeline_perf)




    #print(rest_pipeline_document_perf)
    print(ws_pipeline_document_perf)