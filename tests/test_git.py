#
# working file for testing git support  .  should be a unit test
#
import sys
import os

from src import check_path
check_path()

from shared.util_git import pull, push, isbehind

def test_status():

    xdir = sys.path[0]

    x = isbehind(xdir)
    print(f"isbehind = {x}")


if __name__ == "__main__":
    test_status()

