import os
import subprocess
import threading
import argparse

# print("wd: ", os.path.abspath(os.curdir))


ap = argparse.ArgumentParser()
ap.add_argument('--num', type=int, default=4, help="number of tools")
parsed_args = ap.parse_args()

num = parsed_args.num


def main():
    t1 = threading.Thread(target=lambda:subprocess.run(['python3', 'main.py', '--inputs', '[]', '--outputs', '["de.org.sentence"]', '--port','9714']))
    t1.start()
    t2 = threading.Thread(target=lambda:subprocess.run(['python3', 'main.py', '--inputs', '[]', '--outputs', '["de.org.token"]', '--port','9715']))
    t2.start()
    t3 = threading.Thread(target=lambda:subprocess.run(['python3', 'main.py', '--inputs', '["de.org.sentence"]', '--outputs', '[]', '--port','9716']))
    t3.start()
    t4 = threading.Thread(target=lambda:subprocess.run(['python3', 'main.py', '--inputs', '["de.org.sentence", "de.org.token"]', '--outputs', '[]', '--port','9717']))
    t4.start()


def main2():
    for i in range(num):
        t1 = threading.Thread(target=lambda:subprocess.run(['python3', 'main.py', '--inputs', '[]', '--outputs', '[]', '--port',str(9714+i)]))
        t1.start()

main2()