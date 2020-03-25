#
# working file for testing the sources  .  should be a unit test
#
from loguru import logger

from src import check_path
check_path()

from shared.directory_cache import DirectoryCache
from sources.url_source import UrlSource
from sources.url_source_manager import UrlSourceManager

def test_load():

    cache_sources = DirectoryCache("c:\\temp\\sources")

    manager = UrlSourceManager(cache_sources)
    manager.update_sources("test")

    logger.info("if your source was loaded and it didn't print error, it should be fine")


if __name__ == "__main__":
    test_load()

