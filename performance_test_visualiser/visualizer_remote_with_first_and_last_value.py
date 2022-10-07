import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import os
from sklearn.model_selection import train_test_split



class Visualiser:
    def __init__(self, myindex):
        self.figur = plt.figure(figsize=(40, 20))
        self.dirPath = ""
        self.local = ""
        self.l_websocket = ""
        self.l_websocket_tests_list = ""
        self.pipeline_document_perfs = []
        self.bestTests = []
        self.labels = []
        self.myindex = myindex
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
        """    #################################################################################################    """

        self.toPlotTests = [ "0010", "0025", "0050", "0075", "0090", "0100"]
        #hier werden die Anzahl der Token (die Namen der Ordner) zum Ploten ausgewählt.
        """    #################################################################################################    """



    def setDir(self, dirPath):
        # print("dir = ", dirPath)
        self.entries = os.listdir(dirPath)
        # print(self.entries)
        self.remote = dirPath+"/remote"
        self.l_websocket = dirPath+"/remote/websocket"
        self.addTestsLocal()
        return

    def addTestsLocal(self):
        self.l_websocket_tests_list = os.listdir(self.l_websocket)
        tests_list = self.l_websocket_tests_list
        for test in tests_list:
            path_ = self.l_websocket + "/" + test
            files_counter = len(os.listdir(path_))
            pipeline_document_perf_ = []

            for i in range(files_counter):
                path__ = path_ + "/" + str(i) + "/performance.db"
                try:
                    connction = sqlite3.connect(path__)
                    cursor = connction.cursor()
                    pipeline_document_perf = cursor.execute("SELECT * FROM pipeline_document_perf;").fetchall()
                    pipeline_document_perf_.append(pipeline_document_perf)
                    # print(path__)
                except:
                    # print(path__ + " file not found")
                    pass
            self.pipeline_document_perfs.append(
                {"test": test, "pipeline_document_perf": pipeline_document_perf_})
            #print("Path: "+path_,", number of files: " +str(files_counter))
        self.getBestTestLocal()
        return

    def getBestTestLocal(self):
        for perf in self.pipeline_document_perfs:
            try:
                besti = self.bestTest(perf);
                self.bestTests.append(besti)
            except:
                pass

        #self.bestTest(self.pipeline_document_perfs[0])

        return
    def bestTest(self, perf):
        """
        Die Funktion sucht die beste Performance-Test aus
        und gibt informationen über den Test aus
        :param perf:
        :return:
        """
        print("test: " +perf['test'])
        lines = []
        # labels list
        try:
            for i in perf['pipeline_document_perf'][0]:
                lable = i[8]
                if len(self.labels)<len(perf['pipeline_document_perf'][0]):
                    self.labels.append(lable)

            #print(self.labels)
            for i_perf in perf['pipeline_document_perf']:
                temp_y = []
                for j in i_perf:
                    temp_y.append(j[self.myindex])
                lines.append(temp_y)


            #print(lines)
            x = self.labels
            #print(x)


            # x = np.array(x[1:])
            # x = np.array(x[1:-1]) ###########################_
            x = np.array(x)
            x = list(map(lambda x: x / 10000, x))
            x_train = ""
            x_test = ""
            lx = ""
            mAndC_s = [] # hier werden slope m und intercept c gespeichert
            y_s = []
            y_pred_s = []
            x_tests =[]
            x_trains=[]
            y_trains=[]
            y_tests=[]
            for line in lines:
                # y = np.array(line[1:])
                # y = np.array(line[1:-1])
                y = np.array(line) ###########################_
                y = list(map(lambda x: x / 10000, y))
                y_s.append(y)
                x_train, x_test, y_train, y_test = train_test_split(x, y, random_state=16)
                x_tests.append(x_test)
                x_trains.append(x_train)
                y_trains.append(y_train)
                y_tests.append(y_test)
                lx = len(x_train)
                mAndC = self.gradientDescentCalcu(lx, x_train, y_train)
                mAndC_s.append(mAndC)

            #print(mAndC_s)

            #print(mAndC_s)
            sorted_mAndC_s = sorted(list(map(lambda x: x['m'], mAndC_s)))

            print("Sorted m: ",sorted_mAndC_s)
            print(sorted_mAndC_s[int(len(sorted_mAndC_s)/2)])
            m_avg = []
            if len(sorted_mAndC_s)>2 and len(sorted_mAndC_s)%2==0:
                m_avg = sorted_mAndC_s[int(len(sorted_mAndC_s)/2)]
                print("index AVG m: ", int(len(sorted_mAndC_s)/2))
            elif len(sorted_mAndC_s)==1 or  len(sorted_mAndC_s)==2:
                m_avg = sorted_mAndC_s[0]
                print("index AVG m: ", 0)
            else:
                m_avg = sorted_mAndC_s[int(len(sorted_mAndC_s)/2)+1]
                print("index AVG m: ", int(len(sorted_mAndC_s)/2)+1)


            print("AVG: m: ", m_avg)
            m_min = min(list(map(lambda x: x['m'], mAndC_s)))
            print("Min m: ", m_min)

            m_max = max(list(map(lambda x: x['m'], mAndC_s)))

            new_mAndC_s_with_min = []
            for mc in mAndC_s:
                if mc['m']==m_avg:
                    new_mAndC_s_with_min.append(mc)
                    #print(str(mc)+" ##############")
                #if mc['m']==m_max:
                 #   new_mAndC_s_with_min_max.append(mc)

            index = mAndC_s.index(new_mAndC_s_with_min[0])

            m_c = new_mAndC_s_with_min[0]
            y_pred = np.dot(m_c["m"], x_tests[index]) + m_c["c"]
            y_pred_s.append(y_pred)

            result = {}
            result["mc"]= new_mAndC_s_with_min[0]
            result["test"] = perf['test']
            result["y"]= lines[index]
            result["x"] =self.labels
            result["test_number"] =index
            result["number_of_test"]= len(lines)
            result["y_pred"]= y_pred
            result["x_test"]= x_tests[index]
            result["x_test"]= x_tests[index]
            print(mAndC_s)
            print(result)
            print("best test number", index)
            print("number of test", len(lines))
            print()
            #print(len(lines[index]))
            #print(len(self.labels))

        except:
            pass

        return result



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




    def plotGradientDescent(self):
        tests_ =[]
        for i in self.bestTests:
            if i['test'] in self.toPlotTests:
                tests_.append(i)

        for test1 in tests_:
            plt.plot(test1['x_test'], test1['y_pred'],
                         # color='none',
                         label= str(test1['test'])+", m = "+str(int(test1['mc']['m'])/10000),
                         linestyle='dashed',
                         linewidth=3,
                         marker='o',
                         markersize=10,
                         # markerfacecolor='blue',
                         # markeredgecolor='blue'
                         )


        plt.title(self.all[self.myindex]+": remote  => Gradient descent \n mit den ersten und letzten Wert", fontsize=40, fontweight="bold", pad=40)

        plt.legend(loc='upper left', fontsize=15)
        plt.xticks(np.arange(min(self.labels)/10000, max(self.labels)/10000, step=0.4))
        plt.xlabel(
            "-------------------------------------------- Document size -------------------------------------------->",
            fontsize=16, fontweight="bold", labelpad=30)
        plt.ylabel("--------- " + self.all[self.myindex] + " --------->", fontsize=16, fontweight="bold", labelpad=30)
        # plt.show()

        plt.savefig('results/gradientDescent_remote_with_first_and_last_value.pdf')

    def plot(self):
        tests_ =[]
        for i in self.bestTests:
            if i['test'] in self.toPlotTests:
                tests_.append(i)

        for test1 in tests_:
            plt.plot(
                #test1['x'][1:],
                test1['x'],
                # test1['x'][1:-1],  ###########################_
                # test1['y'][1:],
                test1['y'],
                # test1['y'][1:-1],  ###########################_
                         # color='none',
                         label= str(test1['test'])+", m = "+str(int(test1['mc']['m'])/10000),
                         linestyle='dashed',
                         linewidth=3,
                         marker='o',
                         markersize=10,
                         # markerfacecolor='blue',
                         # markeredgecolor='blue'
                         )


        plt.title(self.all[self.myindex]+": romte \n mit den ersten und letzten Wert", fontsize=40, fontweight="bold", pad=40)

        plt.legend(loc='upper left', fontsize=15)
        plt.xticks(np.arange(min(self.labels), max(self.labels), step=4000))
        plt.xlabel(
            "-------------------------------------------- Document size -------------------------------------------->",
            fontsize=16, fontweight="bold", labelpad=30)
        plt.ylabel("--------- " + self.all[self.myindex] + " --------->", fontsize=16, fontweight="bold", labelpad=30)
        # plt.show()

        plt.savefig('results/plot_remote_with_first_and_last_value.pdf')





visualiser = Visualiser(6)

visualiser.setDir("../performance_dbs")
visualiser.plot()

visualiser_ = Visualiser(6)

visualiser_.setDir("../performance_dbs")
visualiser_.plotGradientDescent()



# ab Anzahl von 275 gibt das aus
"""
Caused by: java.net.SocketException: No buffer space available (maximum connections reached?): connect
"""