#
# working file for testing git support  .  should be a unit test
#
import sys
import os

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))

from util import github_pull, github_push, github_status

def test_status():

    xdir = "C:\\Users\\josh\\Documents\\Code\\COVID19\\covid-tracking"

    github_status(xdir)


if __name__ == "__main__":
    test_status()

