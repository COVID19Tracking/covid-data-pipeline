#
# working file for testing git support  .  should be a unit test
#
import sys
import os

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))

from util import git_pull, git_push, git_isbehind

def test_status():

    xdir = "C:\\Users\\josh\\Documents\\Code\\COVID19\\\covid-data-pipeline"

    x = git_isbehind(xdir)
    print(f"isbehind = {x}")


if __name__ == "__main__":
    test_status()

