# OBSOLETE
import os
from loguru import logger
from typing import List
from lxml import html, etree
from unidecode import unidecode
import re

from shared.directory_cache import DirectoryCache

class ContentTable():
    """
    ContentTable represents from an HTML table

    contains both a simplied HTML table (new_element) and a list for rows (rows).
    the rows are the text content of the table 
    """

    def __init__(self, element: html.Element, fail_on_unexpected_tags = True):

        self.orig_element = element
        self.fail_on_unexpected_tags = fail_on_unexpected_tags 

        self.id = None

        self._new_element = html.Element("table")
        self.caption = ""
        self.rows = []

        self._extract_content()

    def contains_data(self) -> bool:
        " simple test if a table contains anything that looks like data "
        for r in self.rows:
            for c in r:
                if re.search("^[ 0-9,]+", c): return True
                if re.search(":[ 0-9,]+,", c): return True
        return False

    def reformat(self) -> html.Element:
        " get the newly created element "
        return self._new_element

    def _extract_content(self):
        """ 
        Pull information from HTML table 

        1. Ignore TH/TD distinction
        2. remove content that only changes presentation
        3. assume script/comment tags do not contain data
        
        creates a new element fragment and List[List[Str]]
        embedded UL are converted into a comma delimited string
        """        

        #print(f"input table ===>{html.tostring(self.orig_element)}<<====\n")            

        self.id = self.orig_element.get("id")
        if self.id != None:
            self._new_element.attrib["id"] = self.id

        tr_temp = html.Element("tr")
        for x in self.orig_element:
            #print(f"row ===>{html.tostring(x)}<<====\n")            
            
            # -- handle TD that are missing surrounding TR
            if x.tag == "td":
                logger.warning(f"misplaced TD: {html.tostring(x)}")
                tr_temp.append(x)
                continue
                #self._extract_td(x) 
            elif len(tr_temp) > 0:
                self._extract_tr(tr_temp)
                tr_temp = html.Element("tr")

            if x.tag == "tr":
                self._extract_tr(x) 
            elif x.tag == "thead" or x.tag == "tbody" or x.tag == "tfoot":
                for y in x:
                    if y.tag == "tr":
                        self._extract_tr(y) 
                    elif self.fail_on_unexpected_tags:
                        raise Exception(f"unexpected tag in tr: {y.tag}")
                    else:
                        logger.warning(f"unexpected tag in tr: {html.tostring(y)}")
            elif x.tag == "colgroup":
                # logger.warning(f"colgroup: {html.tostring(x)}")
                pass
            elif x.tag == "caption":
                self._extract_caption(x)
            elif self.fail_on_unexpected_tags:
                logger.warning(f"unexpected tag in table: {html.tostring(x)}")
                raise Exception(f"unexpected tag in table: {x.tag}")
            else:
                logger.warning(f"unexpected tag: {html.tostring(x)}")
        
        #print(f"output table ===>{html.tostring(self.new_element)}<<====\n")            

    def _extract_caption(self, caption: html.Element):
        elem, s = self._extract_any(caption)        
        self.caption = s
        self._new_element.append(elem)

    def _extract_tr(self, tr: html.Element):
        " extract a row "

        #print(f"tr ===>{html.tostring(tr)}<<====\n")            

        elem = html.Element("tr")
        elem.text = ""
        elem.tail = ""
        cells = []

        for x in tr:
            if x.tag != "td" and x.tag != "th":
                if x.tag == etree.Comment: continue
                if x.tag == "script": continue

                logger.warning(f"  adding td around {html.tostring(x)}")   
                ch_elem = html.Element("td")
                bad_elem, val = self._extract_any(x)
                if bad_elem != None:
                    ch_elem.append(bad_elem)
                else:
                    ch_elem.text = val
            else:
                ch_elem, val = self._extract_any(x)
                if ch_elem == None: ch_elem = html.Element(x.tag)
        
            ch_elem.tail = ""
            elem.append(ch_elem)
            cells.append(val)

        self._new_element.append(elem)
        self.rows.append(cells)

    def _extract_any(self, x: html.Element) -> [html.Element, str]:
        " extract/simplify an HTML element (recursive) "
        
        #print(f"extract any ===>{html.tostring(x)}<<====\n")

        # nested tables are special because we are processing a flattend list so ignore them.
        if x.tag == "table": return html.Element("table"), "[TABLE]"

        # lists are special because we want to build up a comma seperated list
        if x.tag == "ul": return self._extract_list(x)

        if x.tag == etree.Comment: return etree.Comment(), ""
        
        # no children --> text element
        if len(x) == 0:
            if x.text == None:
                return None, ""
            elem, val = x, self._extract_text(x.text)
            return elem, val

        elem = html.Element(x.tag)
        items = []
        if x.text != None:
            elem.text = x.text
            items.append(x.text)

        for y in x:
            #ignore/strip out layout tags
            if y.tag == etree.Comment: continue
            if y.tag in ["script", "noscript", "br", "hr", "input", "button", "svg", "img", "form"]: continue

            if y.tag in ["span", "div", "h3", "h2", "h1", "small", "strong", "em", "sup", "i", 
                "a", "b", "u", "p", "ul", "label", "sub"]:
                elem_ch, s = self._extract_any(y)
                if elem_ch != None:
                    if len(x) == 1:
                        if s != None and s != "":
                            elem.text = s
                    else:
                        elem.append(elem_ch)
                if s != None and s != "":
                    items.append(s)
            elif y.tag == "table" or y.tag == "iframe":
                elem.append(html.Element(y.tag))
                items.append(f"[{y.tag.upper()}]")
            else:
                logger.warning(f"unexpected tag {y.tag} ===>{html.tostring(y)}<<====\n")
                elem_ch, s = self._extract_any(y)
                if elem_ch != None:
                    if len(x) == 1:
                        if s != None and s != "":
                            elem.text = s
                    else:
                        elem.append(elem_ch)
                if s != None and s != "":
                    items.append(s)

        val = " ".join(items)
        return elem, val

    def _extract_list(self, x: html.Element) -> [html.Element, str]:
        " extract a list "
        #print(f"list ===>{html.tostring(x)}<<====\n")

        elem = html.Element("ul")        
        result = []
        for y in x:
            #print(f"li ===>{html.tostring(y)}<<====\n")
            if y.tag != "li": raise Exception(f"Unexpected tag: {y.tag}")
            if len(y) == 0: continue
            ch_elem, s = self._extract_any(y)
            elem.append(ch_elem) 

            if s != None:
                s = s.replace(",", "_comma_")
            result.append(s)
        val = ", ".join(result)

        return elem, val

    def _extract_text(self, s: str) -> str:
        " filter out specific items with non-ascii chars "
        if s == None: return s
        return unidecode(s).strip()             

