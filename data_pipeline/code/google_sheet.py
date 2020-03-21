#
# Parse a google sheet 
#
import os
from typing import List, Dict
from loguru import logger

from lxml import html, etree
#from lxml.etree import tostring

import pandas as pd


class GoogleSheet():

    def __init__(self, content: bytes):
        self.tree = html.fromstring(content)
        self.menus = self._get_menu(self.tree) 

        #names = [ x for x in self.menus ]
        #logger.info(f"  google sheet tabs: {names}")

    def get_tab(self, name: str) -> pd.DataFrame:
        " gets the a tab as a data frame"

        if not name in self.menus:
            names = [ x for x in self.menus ]
            raise Exception(f"invalid tab name {name}, valid names are {names}")

        x_id =  self.menus[name]
        sheet = self.tree.get_element_by_id(x_id)[0][0]
        df = self._htmltable_to_dataframe(sheet)
        return df

    def _get_menu(self, tree: etree) -> Dict[str, str]:
        " gets the tabs from a google sheet "
        try:
            xmenu = tree.get_element_by_id("sheet-menu")
        except Exception as ex:
            logger.error(ex)
            raise Exception("Could not find menu")

        #if xmenu == None:
        #    raise Exception("Could not find menu")

        menu = {}
        for x in xmenu:
            if x.tag == "li":
                x_id = x.attrib["id"].replace("sheet-button-", "")
                x_label = x[0].text
                menu[x_label] = x_id
        return menu

    def _htmltable_to_dataframe(self, table: etree) -> pd.DataFrame:
        " converts a google sheet tab into a data frame"
        names = []
        data = []
        cnt = 0
        for row in table[1]:
            if cnt == 0:
                for col in row:
                    names.append(col.text)
                    data.append([])
            elif cnt == 1:
                pass # freeze-bar
            else:
                i = 0
                for col in row:
                    if len(col) == 0: 
                        val = col.text                        
                    elif col[0].tag == 'a':
                        val = col[0].get("href")
                    else:
                        val = html.tostring(col)
                    data[i].append(val) 
                    i += 1
            cnt += 1

        xcols = {}
        for n, d in zip(names, data):
            if n is None: continue
            xcols[n] = d

        df = pd.DataFrame(xcols)
        return df




