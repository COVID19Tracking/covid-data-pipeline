# OBSOLETE

import os
from loguru import logger
from typing import List
from lxml import html, etree
from unidecode import unidecode
import re
from copy import deepcopy

from shared.directory_cache import DirectoryCache
from transform.content_table import ContentTable
from transform.change_list import ChangeList

class PageParser():

    def __init__(self, work_dir: str):
        self.cache = DirectoryCache(work_dir) 

    def strip_attributes(self, x: html.Element):
        x.attrib.clear()
        for y in x:
            self.strip_attributes(y)


    def extract_tables(self, tables: List) -> List[ContentTable]:
        result = []

        for t in tables:
            self.strip_attributes(t)
            x = ContentTable(t)
            result.append(x)
        return result

    def write_as_text(self, foutput, name: str, tables: List[ContentTable]):        
        foutput.write(f"{name}\n")
        for i in range(len(tables)):
            foutput.write(f"{name}\tTable {i+1}\n")
            t = tables[i]
            for r in t.rows:
                foutput.write(f"{name}\tTable {i+1}")
                for c in r:
                    foutput.write("\t")
                    if c != None:
                        c2 = unidecode(c)
                        foutput.write(c2)
                foutput.write(f"\n")
        foutput.write(f"\n")
        

    def write_as_html(self, foutput, name: str, url: str, tables: List[ContentTable], html_doc: html.Element):

        s = html.Element("div")
        h = html.Element("h1")
        h.text = name
        s.append(h)

        m = html.Element("div")
        m.text = self.cache.read_date_time_str(name+ ".html")
        s.append(m) 

        for t in tables:
            s.append(t.new_element)

        x = html.Element("br")        
        s.append(x)
        a = html.Element("a")
        a.attrib["href"] = url
        a.text = url
        s.append(a)

        h = html.Element("html")
        h.append(html.Element("body"))
        h[0].append(deepcopy(s))
        foutput.write(html.tostring(h, pretty_print=True))

        html_doc.append(s)
        html_doc.append(html.Element("hr"))

    def write_miss_to_html(self, name: str, url: str, msg: str, html_doc: html.Element):

        s = html.Element("div")
        h = html.Element("h1")
        h.text = name
        s.append(h)

        m = html.Element("div")
        m.text = self.cache.read_date_time_str(name + ".html")
        s.append(m) 

        m = html.Element("span")
        m.text = msg
        s.append(m) 

        x = html.Element("br")        
        s.append(x)
        a = html.Element("a")
        a.attrib["href"] = url
        a.text = url
        s.append(a)

        html_doc.append(s)
        html_doc.append(html.Element("hr"))

    def process_all(self):


        cl = ChangeList(self.cache)
        url_dict = cl.read_urls_as_dict()

        ignore_list = ["main_sheet", "WV"]
        missing_list = ["WI", "VA", "UT", "TN", "SC", "RI", "PA", "NJ", "NE", "ND", "ND_data",
            "NC", "MS", "MO", "MN", "KY", "KS",
            "IA", "HI", "GA", "DC_data",  "AZ", "AL" ]
        table_list = ["WI_data", "WA", "VT", "TX", "SD", "SC_data", "OR", 
            "OK", "OH", "NY", "NV", "NM", "NJ_data", "NH", "NC_data", "MT", "MN_data", 
            "MI", "ME", "MD", "MA_data", "MA", "CO", 
            "LA", "IN", "IL", "ID", "FL", "DE", "DC", "CT", "CO_data", "CA", "AR", "AK_data", "AK"]

        out_file = os.path.join(self.cache.work_dir, "results.txt")

        old_date = self.cache.read_old_date()

        foutput = open(out_file, "w")
        foutput.write(f"Data Scanned at\t{old_date}\n")
        foutput.write(f"STATE RESULTS\n\n")

        html_out_dir = os.path.join(self.cache.work_dir, "tables")
        if not os.path.exists(html_out_dir): os.makedirs(html_out_dir)

        html_out_file = os.path.join(self.cache.work_dir, "tables", f"combined.html")
        html_doc = html.Element("html")
        html_doc.append(html.Element("body"))
        html_doc[0].append(html.Element("span"))
        html_doc[0][0].text = f"data scraped at {old_date}"
        html_doc[0].append(html.Element("hr"))

        for fn in self.cache.list_html_files():
            x = fn.replace(".html", "")
            if x in ignore_list: continue

            logger.info(f"=================| {fn}")
            content = self.cache.read(fn)

            tree = html.fromstring(content)
            tables = tree.xpath('//table')
            if len(tables) > 0:
                if x in missing_list: 
                    foutput.write(f"{x}\t*** Found unexpected tables\n\n")
                    logger.warning(f"  found {len(tables)} unexpected tables")
                xlist = self.extract_tables(tables)
    
                xlist2 = [x for x in xlist if x.contains_data()]
                if len(xlist2) == 0:
                    foutput.write(f"{x}\tNo data tables\n\n")
                    self.write_miss_to_html(x, url_dict[x], "No data tables", html_doc)
                    continue
                
                #xlist = self.remove_nondata_tables(xlist)
    
                self.write_as_text(foutput, x, xlist2)

                html_out_file = os.path.join(self.cache.work_dir, "tables", f"{x}.html")
                with open(html_out_file, "wb") as foutput2:
                    self.write_as_html(foutput2, x, url_dict[x], xlist2, html_doc)

            else:
                if x in table_list: 
                    foutput.write(f"{x}\t*** Missing expected tables\n\n")
                    logger.warning(f"  missing tables")
                else:
                    foutput.write(f"{x}\tNo tables in data\n\n")
                self.write_miss_to_html(x, url_dict[x], "No tables", html_doc)


            html_out_file = os.path.join(self.cache.work_dir, "tables", f"combined.html")
            with open(html_out_file, "wb") as foutput2:
                foutput2.write(html.tostring(html_doc, pretty_print=True))

            #inventory = tree.xpath('//div[@class="inventory-listings"]')
            #self.logger.info(len(inventory[0]))



# --------------------
def main():

    parser = PageParser("c:\\Exemplar\\Corona19\\2020-03-08")
    parser.process_all()

if __name__ == "__main__":
    main()
