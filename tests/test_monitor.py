#
# working file for testing git support  .  should be a unit test
#
import subprocess
import time
import sys

from src import check_path
check_path()

from shared.util_git import monitor_start, monitor_check

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

