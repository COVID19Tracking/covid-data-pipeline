from typing import List, Union
from lxml import html, etree
import re
from loguru import logger

def is_guid(xid: str) -> bool:
    if xid == None: return False
    return re.match("[a-zA-Z#]*[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", xid) != None

def regularize_attrib(elem: html.Element, n: str):
    v = elem.attrib[n]
    if v == None: return
    
    if n == "id":
        if is_guid(v): elem.attrib[n] == "[guid]"
    elif n == "href":
        if v.startswith("#") and is_guid(v): 
            elem.attrib[n] = "#"
        elif "urldefensepoint.com" in v:
            elem.attrib[n] = "http://urldefensepoint.com"
        elif v.startswith("https://twitter.com") and elem.text != None and elem.text.startswith("@"):
            elem.getparent().remove(elem)
    elif n == "src":
        if v.startswith("https://www.youtube.com/"):
            elem.attrib[n] = "https://www.youtube.com"


def clean_attributes(elem: html.Element):
    
    to_delete = []
    for n in elem.attrib:
        if n in ["a", "div", "id", "href", "src", "iframe"]:
            regularize_attrib(elem, n)
            continue

        if n in ["cellpadding", "cellspacing", "width", "height", "align", "valign", "border"]:
            to_delete.append(n)
            continue
        if n in ["class", "style", "onload", "target", "onmouseout", "onmouseover", "onclick", "onkeydown"]:
            to_delete.append(n)
            continue
        if n in ["role", "scrolling", "tabindex"]:
            to_delete.append(n)
            continue
        if n in ["webpartid", "webpartid2", "allowfullscreen", "rel", "accesskey", "focusable", "bgcolor"]:
            to_delete.append(n)
            continue
        if n.startswith("data-") or n.startswith("aria-"):
            to_delete.append(n)
            continue
        #logger.error(f"unexpected attribute: {n}")

    for n in to_delete:
        del elem.attrib[n]

def clean_element(elem : html.Element):

    tag = elem.tag
    if tag in ["script", "noscript", "style", "meta", "input", "link", "img", "font"]:
        elem.getparent().remove(elem)
        return
    if tag == etree.Comment:
        elem.getparent().remove(elem)
        return
    if tag == etree.ProcessingInstruction:
        elem.getparent().remove(elem)
        return

    if tag == "svg":
        while len(elem): del elem[0]
    else:
        for ch in elem: clean_element(ch)

    clean_attributes(elem)



class HtmlCleaner:

    def __init__(self):
        pass


    def Clean(self, content: bytes) -> bytes:
        " Remove all layout/display informattion from HTML "
        
        doc = html.fromstring(content)
        clean_element(doc)
        content = html.tostring(doc, pretty_print=True)
        return content



    
