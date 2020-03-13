# run through all the A/B pairs and confirm that normalization works

import os
from loguru import logger
from typing import List, Union
import re
from lxml import html, etree
from unidecode import unidecode

from cache import PageCache
from regularize import regularize

def remove_identical_nodes(elem1: html.Element, elem2: html.Element) -> bool:
    
    s1 = html.tostring(elem1)
    s2 = html.tostring(elem2)
    if s1 == s2: return True

    to_remove = []
    n = min(len(elem1), len(elem2))
    for i in range(n):
        is_same = remove_identical_nodes(elem1[i], elem2[i])
        if is_same: to_remove.append(i)
    
    to_remove.reverse()
    for i in to_remove:
        elem1.remove(elem1[i])
        elem2.remove(elem2[i])

    return False



class PageCompare():

    def __init__(self, work_dir: str):
        self.cache = PageCache(work_dir) 

    def process_all(self):

        ignore_list = ["main_sheet", "WV"]

        html_out_dir = os.path.join(self.cache.work_dir, "compare")
        if not os.path.exists(html_out_dir): os.makedirs(html_out_dir)


        for fn in self.cache.list_html_files():
            x = fn.replace(".html", "")
            if x in ignore_list: continue


            content_a = self.cache.load(fn, "A")
            content_b = self.cache.load(fn, "B")
            if content_a == content_b:
                logger.info(f"=================| {fn}")
                logger.info("   data is SAME")
                continue

            fn_a = os.path.join(html_out_dir, f"{x}_A.html")
            fn_b = os.path.join(html_out_dir, f"{x}_B.html")
            if os.path.exists(fn_a): os.remove(fn_a)
            if os.path.exists(fn_b): os.remove(fn_b)

            content_a = regularize(content_a)
            content_b = regularize(content_b)
            
            if content_a == content_b:
                logger.info(f"=================| {fn}")
                logger.info("   data is FIXED")
                continue

            doc_a = html.fromstring(content_a)
            doc_b = html.fromstring(content_b)

            remove_identical_nodes(doc_a, doc_b)
            str_a = html.tostring(doc_a, pretty_print=True)
            str_b = html.tostring(doc_b, pretty_print=True)

            logger.info(f"=================| {fn}")
            logger.info("   data is different")

            with open(fn_a, "wb") as f: f.write(str_a)
            with open(fn_b, "wb") as f: f.write(str_b)



# --------------------
def main():

    comparer = PageCompare("c:\\Exemplar\\Corona19\\2020-03-08")
    comparer.process_all()

if __name__ == "__main__":
    main()
