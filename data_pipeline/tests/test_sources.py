#
# working file for testing the sources  .  should be a unit test
#
from loguru import logger

from ..code.directory_cache import DirectoryCache
from ..code.url_source import UrlSource
from ..code.url_source_manager import UrlSourceManager

def test_load():

    cache_sources = DirectoryCache("c:\\temp\\sources")

    manager = UrlSourceManager(cache_sources)
    manager.update_sources("test")

    logger.info("if your source was loaded and it didn't print error, it should be fine")


if __name__ == "__main__":
    test_load()

