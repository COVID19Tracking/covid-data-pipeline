# html helpers
from lxml import html

def make_source_link(kind: str, stage: str, name: str) -> html.Element:
    d = html.Element("div")
    if kind == stage:
        a = html.Element("a")
        # "http://covid19-api.exemplartech.com/github-data/raw/AZ.html
        a.href = f"../{stage}/{name}"
        a.text = stage
        d.append(a)
    else:
        d.text = stage
    d.tail = " > "        
    return d

def make_source_links(kind: str, name: str, source: str):

    div = html.Element("div")
    div.attrib["class"] = "source"
            
    kind = kind.lower()
    d = make_source_link(kind, "extract", name)
    div.append(d)
    d = make_source_link(kind, "clean", name)
    div.append(d)
    d = make_source_link(kind, "raw", name)
    div.append(d)
    d = make_source_link(kind, source, name)
    div.append(d)

    return div        

