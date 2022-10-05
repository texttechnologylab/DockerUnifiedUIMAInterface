import sqlite3
import numpy as np
import matplotlib.pyplot as plt

ws_connction = sqlite3.connect("../websocket_small_packets_of_rokens_opened_client_test.db")
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


"""

plt.figure(figsize=(30, 15))

plt.title(all[myindex], fontsize=40, fontweight="bold", pad=40)

plt.plot(labels, ws_values, label='WS', linestyle='--')
plt.plot(labels, rest_values, label='REST', linestyle='-.')
plt.legend(loc='upper left', fontsize=15)
plt.xticks(np.arange(min(labels), max(labels), step=4000))
plt.xlabel("-------------------------------------------- Document size -------------------------------------------->", fontsize=16, fontweight="bold", labelpad=30)
plt.ylabel("--------- "+ all[myindex]+" --------->", fontsize=16, fontweight="bold", labelpad=30)
plt.show()

"""









class Linear_Regression:
    def __init__(self, X, y):
        self.X = X
        self.y = y
        self.a = [0, 0]

    def update_coeffs(self, learningrate):
        y_pred = self.predict()
        y = self.y
        b = len(y)
        self.a[0] = self.a[0] - (learningrate * ((1 / b) *
                                                 np.sum(y_pred - y)))

        self.a[1] = self.a[1] - (learningrate * ((1 / b) *
                                                 np.sum((y_pred - y) * self.X)))

    def predict(self, X=[]):
        y_pred = np.array([])
        if not X: X = self.X
        a = self.a
        for x in X:
            y_pred = np.append(y_pred, a[0] + (a[1] * x))

        return y_pred

    def get_current_accuracy(self, y_pred):
        t, e = y_pred, self.y
        s = len(y_pred)
        return 1 - sum(
            [
                abs(t[i] - e[i]) / e[i]
                for i in range(s)
                if e[i] != 0]
        ) / s

    def compute_cost(self, y_pred):
        b = len(self.y)
        J = (1 / 2 * b) * (np.sum(y_pred - self.y) ** 2)
        return J

    def plot_best_fit(self, y_pred, fig):
        f = plt.figure(fig)
        plt.scatter(self.X, self.y, color='r')
        plt.plot(self.X, y_pred, color='y')
        f.show()


def main():
    X = np.array(
        labels
        # [1, 2, 3, 4, 5, 6, 7]
        # ValueError: x and y must be the same size

    )
    y = np.array(
        ws_values
        # [1, 222, 333, 453, 532, 362, 700]
    )

    regr = Linear_Regression(X, y)

    iterations = 0
    steps = 90
    learningrate = 0.1
    costs = []

    y_pred = regr.predict()
    regr.plot_best_fit(y_pred, 'Initial best fit line')

    while 1:
        y_pred = regr.predict()
        cst = regr.compute_cost(y_pred)
        costs.append(cst)
        regr.update_coeffs(learningrate)

        iterations += 1
        if iterations % steps == 0:
            print(iterations, "epochs elapsed")
            print("current accuracy is :",
                  regr.get_current_accuracy(y_pred))
            break

    # final best-fit line
    regr.plot_best_fit(y_pred, 'Final Best Fit Line')

    h = plt.figure('Verification')
    plt.plot(range(iterations), costs, color='r')
    h.show()

    # regr.predict([i for i in range(10)])


if __name__ == '__main__':
    main()