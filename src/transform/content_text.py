# support for content in text
from lxml import html
from loguru import logger
import re

class ContentText():

    def __init__(self, elem: html.Element, text: str, tail: str):
        self.elem = elem

        self.text = text
        self.tail = tail
        self.child_text = self.__extract_child(elem)

    def __extract_child(self, elem: html.Element) -> str:

        if elem.tag == "table": return ""        
        if elem.tag == "iframe": return ""        
        #if elem.tag == "div": return ""        
        if len(self.elem) == 0: 
            t1 = self.elem.text
            t3 = self.elem.tail
            t1 = t1.strip() if t1 != None else ""
            t3 = t3.strip() if t3 != None else ""
            return (t1 + " " + t3).strip()

        result = ""
        for ch in elem:
            t1 = ch.text
            t2 = self.__extract_child(ch) if len(ch) > 0 else ""
            t3 = ch.tail

            t1 = t1.strip() if t1 != None else ""
            t3 = t3.strip() if t3 != None else ""
            
            if t1 != "": result += " " + t1
            if t2 != "": result += " " + t2
            if t3 != "": result += " " + t3

        return result.strip()

    def contains_data(self) -> bool:
        if re.search("[Cc]ase", self.text): return True
        if re.search("[Cc]ase", self.child_text): return True
        if re.search("[Cc]ase", self.tail): return True

    def as_element(self) -> html.Element:
        div = html.Element("div")

        xid = self.elem.attrib.get("id")
        if xid != None: div.attrib["id"] = xid

        div.text = self.text
        if self.child_text != "":
            div.text += " " + self.child_text
        if self.tail != "":
            div.text += " " + self.tail
        return div

def make_content_text(elem: html.Element) -> ContentText:
    text, tail = elem.text, elem.tail
    if text == None: text = ""
    if tail == None or tail == "\n": tail = ""

    text = text.replace("\n", "").strip()
    tail = tail.replace("\n", "").strip()

    if text == "" and tail == "": 
        #logger.info(f"make_content_text SKIP >>\n{html.tostring(elem)}<<\n")
        return None

    # should_check = False
    # if text != "":
    #     if " case" in text or \
    #        " potential " in text or \
    #         re.search("[0-9]", text): should_check = True
    # if tail != "":
    #     if " case" in tail or \
    #        " potential " in tail or \
    #         re.search("[0-9]", tail): should_check = True
    # if not should_check: 
    #     #logger.info(f"make_content_text SKIP >>\n{html.tostring(elem)}<<\n")
    #     return None

    #logger.info(f"make_content_text MATCH >>\n{html.tostring(elem)}<<\n")
    return ContentText(elem, text, tail)
