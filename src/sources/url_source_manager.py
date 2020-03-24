# overall url_source manager
#
#   manage persistent sate

from loguru import logger

from sources.url_source import UrlSource, UrlSources
from sources.url_source_parsers import sources_config
from sources.url_source_validator import UrlSourceValidator

from shared.directory_cache import DirectoryCache
from transform.change_list import ChangeList

class UrlSourceManager():

    def __init__(self, cache: DirectoryCache):
        self.cache = cache
        self.change_list = None

    def update_sources(self, mode: str) -> UrlSources:

        self.change_list = ChangeList(self.cache)
        self.change_list.load()

        self.change_list.start_run()

        sources = UrlSources()
        sources.scan(sources_config)
        sources.read(self.cache, "sources.txt")
        logger.info(f"  found {len(sources.items)} sources")
        
        validator = UrlSourceValidator()
        for src in sources.items:
            if not src.check_mode(mode):
                continue
            
            src.update_from_remote()
            src.write_parsed(src.name, self.cache)

            if validator.validate(src):
                src.status = "valid"
                logger.info(f"     {src.name}: save")
                src.write(src.name, self.cache, self.change_list)
                logger.info(f"     {src.name}: updated from remote")
            else:
                src.status = "invalid"
                validator.display_status()
                if src.read(src.name, self.cache):
                    logger.warning(f"     {src.name}: use local cache")
                else:
                    self.change_list.record_failed(src.name, "source", src.endpoint, "no local cache")
        
        sources.update_status() 
        sources.write(self.cache, "sources.txt")

        self.change_list.finish_run()
        return sources