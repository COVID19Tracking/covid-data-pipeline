#
# working file for testing git support  .  should be a unit test
#
import sys
import os

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))

from code.shared.util_git import pull, push, isbehind

def test_status():

    xdir = sys.path[0]

    x = isbehind(xdir)
    print(f"isbehind = {x}")


if __name__ == "__main__":
    test_status()

