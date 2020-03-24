#
# Obsolete
#


import re
from shared.directory_cache import DirectoryCache

def clean_content(content: bytes) -> bytes:

    if content == None: return None

    content = re.sub(b'<input type="hidden" .* />', b'', content)

    #CA
    content = re.sub(b'formDigestElement.value = .*', b'', content)

    #CO
    content = re.sub(b'"beacon":"bam.nr-data.net".*}', b'}', content)
    #CO_data
    content = re.sub(b'nonce=".*"', b'', content)

    #CT
    content = re.sub(b'<meta name="VIcurrentDateTime" content="637193341564631315"', b'<meta name="VIcurrentDateTime" content=""', content)

    #NJ
    content = re.sub(b'<script async type="text/javascript" src="/_Incapsula_Resource?SWJIYLWA=.*">', b'<script>', content)

    #WA
    content = re.sub(b'gomenu[0-9a-z]+', b'gomenu1234', content)
    content = re.sub(b'megamenu[0-9a-z]+', b'megamenu1234', content)
    
    # content = re.sub(b'<script.*>.*?</script>', b'', content, count=1000, flags=re.DOTALL)
    return content



class HTMLCompare:

    def __init__(self, cache: DirectoryCache):
        self.cache = cache
        self.content_a = b''
        self.content_b = b''

        self.is_identical = False
        self.is_re = False

    def load_saved_versions(self, key: str):
        " loads A/B version, returns true if they are identical "

        self.content_a = self.cache.load(key, "A")
        self.content_b = self.cache.load(key, "B")

        self.is_identical = False
        self.is_re = False
        
        if self.content_a == None or self.content_b == None: 
            return
        
        if self.content_a == self.content_b: 
            self.is_identical = True
        else:
            self.is_re = clean_content(self.content_a) == clean_content(self.content_b)


    def is_different(self, new_content: bytes, old_content: bytes) -> bool:

        if self.is_identical:
            return new_content != old_content

        if self.is_re:
            return clean_content(new_content) != clean_content(old_content)

        raise Exception("Cannot compare because clean fails to make content the same")
