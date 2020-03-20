#
# working file for testing the sources  .  should be a unit test
#
import sys
import os

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))

from url_source import UrlSource, get_available_sources

def test_load():

    sources = get_available_sources()

    for x in sources:
        print(f"load {x.name}")
        x.load()

if __name__ == "__main__":
    test_load()

