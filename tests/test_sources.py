#
# working file for testing the sources  .  should be a unit test
#
import sys
import os

from loguru import logger

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))

from directory_cache import DirectoryCache
from url_source import UrlSource
from url_source_manager import UrlSourceManager

def test_load():

    cache_sources = DirectoryCache("c:\\temp\\sources")

    manager = UrlSourceManager(cache_sources)
    sources = manager.update_sources("test")

    logger.info("if your source was loaded and it didn't print error, it should be fine")


if __name__ == "__main__":
    test_load()

