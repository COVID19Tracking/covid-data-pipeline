#
# working file for testing git support  .  should be a unit test
#
import sys
import os
import subprocess
import time

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))

from util import monitor_start, monitor_check

def test_monitor():

    print(f"sys.argv={sys.argv}")

    if len(sys.argv) == 1:
        print("add arg")
        subprocess.call(["python.exe", "test_monitor.py", "--monitor"])
    elif sys.argv[1] == "--monitor":
        print("start parent")
        monitor_start("--monitor")
        print("exit parent")
    elif sys.argv[1] == "--guarded":
        print("start child")
        while True:
            time.sleep(10)
            print("check for change")
            if monitor_check(): break
        print("end child")



if __name__ == "__main__":
    test_monitor()

