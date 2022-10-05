import sqlite3
import numpy as np
import matplotlib.pyplot as plt





connction_cach1 = sqlite3.connect("../performance_dbs/cach1.db")
connction_cach2 = sqlite3.connect("../performance_dbs/cach2.db")
cursor_cach1 = connction_cach1.cursor()
cursor_cach2 = connction_cach2.cursor()
# show all tables
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# [('pipeline',), ('pipeline_perf',), ('pipeline_component',), ('pipeline_document',), ('pipeline_document_perf',)]


# show all data
pipeline_perf_cach1 = cursor_cach1.execute("SELECT * FROM pipeline_perf;").fetchall()
pipeline_perf_cach2 = cursor_cach2.execute("SELECT * FROM pipeline_perf;").fetchall()

pipeline_document_perf_cach1 = cursor_cach1.execute("SELECT * FROM pipeline_document_perf;").fetchall()
pipeline_document_perf_cach2 = cursor_cach2.execute("SELECT * FROM pipeline_document_perf;").fetchall()
print(pipeline_document_perf_cach1)
print(pipeline_document_perf_cach2)
print("durationComponentTotal: ", pipeline_document_perf_cach1[0][6])
print("durationComponentTotal: ", pipeline_document_perf_cach2[0][6])







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
