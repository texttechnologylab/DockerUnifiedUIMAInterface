import os
import subprocess
import threading
import time
print()
print("wd: ", os.path.abspath(os.curdir))
t1 = threading.Thread(target=lambda:subprocess.run(['python3', 'main.py', '--inputs', '[]', '--outputs', '["de.org.sentence"]', '--port','9714']))
t1.start()
time.sleep(1)
t2 = threading.Thread(target=lambda:subprocess.run(['python3', 'main.py', '--inputs', '[]', '--outputs', '["de.org.token"]', '--port','9715']))
t2.start()
time.sleep(1)
t3 = threading.Thread(target=lambda:subprocess.run(['python3', 'main.py', '--inputs', '["de.org.sentence"]', '--outputs', '[]', '--port','9716']))
t3.start()
time.sleep(1)
t4 = threading.Thread(target=lambda:subprocess.run(['python3', 'main.py', '--inputs', '["de.org.sentence", "de.org.token"]', '--outputs', '[]', '--port','9717']))
t4.start()
