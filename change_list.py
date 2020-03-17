import os
import json
from loguru import logger
from typing import Dict, Tuple, List
from datetime import datetime
from lxml import html

from util import format_datetime_for_display, format_datetime_difference, \
    convert_json_to_python, convert_python_to_json

from directory_cache import DirectoryCache

class ChangeList:
    """ maintains a list of changes for a run """

    __slots__ = (
        'cache',

        'start_date',
        'end_date',
        'previous_date',
        'time_lapsed',
        'error_message',
        'complete',

        '_items',
        '_lookup',

        'last_timestamp'
    )

    def __init__(self, cache: DirectoryCache):
        self.cache = cache

        self.start_date = datetime.utcnow()
        self.end_date = self.start_date
        self.previous_date = self.start_date
        self.time_lapsed = self.end_date - self.start_date
        self.error_message = None
        self.complete = False

        self.last_timestamp = ""

        self._items = []
        self._lookup = {}
        
    def start_run(self):
        self._read_json()

        self.previous_date = self.start_date
        self.start_date = datetime.utcnow()
        self.end_date = self.start_date
        self.time_lapsed = self.end_date - self.start_date
        
        self.error_message = None
        self.complete = False
        for x in self._items: x["complete"] = False


    def save_progress(self):
        self.end_date = datetime.utcnow()
        self.time_lapsed = self.end_date - self.start_date

        self._write_json()
        self._write_text()
        self._write_urls()

    def abort_run(self, ex: Exception):
        self.error_message = str(ex)

    def finish_run(self):
        self.complete = True
        self.save_progress()

    def get_minutes_since_last_check(self, name: str) -> float:
        " get time since last check in minutes "
        
        idx = self._lookup.get(name)
        if idx == None: return 100000.0

        x = self._items[idx]
        checked_date = x["checked"]
        if checked_date == None: return 100000.0
        delta = self.start_date - checked_date

        return delta.total_seconds() / 60.0

    def update_status(self, name: str, source: str, status: str, xurl: str, msg: str) -> Tuple[Dict, Dict, str]:
        
        xnow = datetime.utcnow()

        if msg == "": msg = None
        idx = self._lookup.get(name)
        if idx == None:         
            x = None   
            y = { "name": name, "source": source, "status": status, "url": xurl, "msg": msg, "complete": True,
                  "added": xnow, "checked": None, "updated": None, "failed": None }
            self._lookup[name] = len(self._items)
            self._items.append(y)
        else:
            x = self._items[idx]
            y = x.copy()
            y["status"] = status
            y["source"] = source
            y["url"] = xurl
            y["msg"] = msg
            y["complete"] = True
            self._items[idx] = y
            
        return y, x, xnow

    def record_failed(self, name: str, source: str, xurl: str, msg: str):

        status = "FAILED"
        logger.error(f"     {name}: {status}")
        logger.error(f"     {name}: url={xurl} msg={msg}")

        y, x, xnow = self.update_status(name, source, status, xurl, msg)
        if x != None and x["status"] != "FAILED":
            y["failed"] = xnow


    def record_unchanged(self, name: str, source: str, xurl: str, msg: str = ""):

        status = "unchanged"
        logger.info(f"  {name}: {status}")

        y, _, xnow = self.update_status(name, source, status, xurl, msg)
        y["checked"] = xnow
        y["failed"] = None

    def record_skip(self, name: str, source: str, xurl: str, msg: str = ""):

        status = "skip"
        logger.info(f"  {name}: {status} {msg}")

        self.update_status(name, source, status, xurl, msg)

    def record_duplicate(self, name: str, source: str, xurl: str, msg: str = ""):

        status = "duplicate"
        logger.info(f"  {name}: {status} {msg}")

        self.update_status(name, source, status, xurl, msg)

    def temporary_skip(self, name: str, source: str, xurl: str, msg: str = ""):
        " make as complete but don't change state "
        status = "skip"
        logger.info(f"  {name}: {status} {msg}")

        y, x, _ = self.update_status(name, source, status, xurl, msg)
        if x != None:
            y["status"] = x["status"]
            y["msg"] = x["msg"]


    def record_changed(self, name: str, source: str, xurl: str, msg: str = ""):

        status = "CHANGED"
        logger.warning(f"    {name}: {status}")

        y, _, xnow = self.update_status(name, source, status, xurl, msg)
        y["checked"] = xnow
        y["updated"] = xnow
        y["failed"] = None


        self.last_timestamp = xnow

    def _remove_text_files(self):
        for n in ["change_list.txt", "urls.txt"]:
            fn = os.path.join(self.cache.work_dir, n)
            if os.path.exists(fn): os.remove(fn)

    def _write_text(self):
        fn = os.path.join(self.cache.work_dir, "change_list.txt")
        
        def write_block(f, status: str):
            f.write(f"====== {status} ======\n")
            for i in range(len(self._items)):
                x = self._items[i]
                name, source, status, xurl, msg = x["name"], x.get("source"), x["status"], x["url"], x["msg"]
                if source == None: source = ""
                if msg == None: msg = ""
                if s != status: continue
                f.write(f"{name}\t{status}\t{source}\t{xurl}\t{msg}\n")
            f.write(f"\n")

        with open(fn, "w") as f_changes:
            f_changes.write(f"STATE CHANGE LIST\n\n")
            f_changes.write(f"  start\t{self.start_date}\n")
            f_changes.write(f"  end\t{self.end_date}\n")
            f_changes.write(f"  previous\t{self.previous_date}\n")
            f_changes.write(f"  lapsed\t{self.time_lapsed}\n")
            f_changes.write(f"\n")

            status = {}
            for x in self._items:
                s = x["status"]
                cnt = status.get(s)
                if cnt == None: cnt = 0
                status[s] = cnt + 1

            names = [x for x in status]
            names.sort()

            f_changes.write(f"STATUS COUNTS:\n")
            for s in names:
                f_changes.write(f"  {s}\t{status[s]}\n")
            f_changes.write(f"\n")

            for s in names:
                write_block(f_changes, s)

    # -----------------------

    def _add_html_info_row(self, t: html.Element, label: str, val: str, cls: str = None):
        tr = html.Element("tr")

        td = html.Element("td")
        td.text = label
        if cls != None: td.attrib["class"] = cls
        tr.append(td)

        td = html.Element("td")
        td.text = val
        if cls != None: td.attrib["class"] = cls
        tr.append(td)

        tr.tail = "\n      "
        t.append(tr)        

    def _fill_info_table(self, t: html.Element):

        self._add_html_info_row(t, "Started At", format_datetime_for_display(self.start_date))
        self._add_html_info_row(t, "Ended At", format_datetime_for_display(self.end_date))
        self._add_html_info_row(t, "Lapse Time (mins)", str(self.time_lapsed))
        self._add_html_info_row(t, "Previous Run At", format_datetime_for_display(self.previous_date))
        self._add_html_info_row(t, "Error Message", self.error_message, 
            "err" if self.error_message else None)
        t[-1].tail = "\n    "

    def _make_source_link(self, kind: str, stage: str, name: str) -> html.Element:
        d = html.Element("div")
        if kind == stage:
            a = html.Element("a")
            # "http://covid19-api.exemplartech.com/github-data/raw/AZ.html
            a.href = f"../{stage}/{name}"
            a.text = stage
            d.append(a)
        else:
            d.text = stage
        d.tail = " => "        
        return d

    def _make_source_links(self, kind: str, name: str, source: str):

        div = html.Element("div")
        div.attrib["class"] = "source"
                
        kind = kind.lower()
        d = self._make_source_link(kind, "extract", name)
        div.append(d)
        d = self._make_source_link(kind, "clean", name)
        div.append(d)
        d = self._make_source_link(kind, "raw", name)
        div.append(d)
        d = self._make_source_link(kind, source, name)
        div.append(d)

        return div        


    def _add_data_row(self, t: html.Element, x: Dict, kind: str):

    # {
    #   "name": "AK.html",
    #   "status": "unchanged",
    #   "url": "http://dhss.alaska.gov/dph/Epi/id/Pages/COVID-19/default.aspx",
    #   "msg": null,
    #   "complete": true,
    #   "added": "2020-03-13T06:17:50.550545",
    #   "checked": "2020-03-16T22:00:07.143700",
    #   "updated": "2020-03-16T21:40:10.611841",
    #   "failed": null,
    #   "source": "google-states"
    # }
        prefix = "\n      "

        tr = html.Element("tr")
        tr.tail = prefix

        # Name
        name = x["name"]
        td = html.Element("td")
        td.tail = prefix
        a = html.Element("a")
        a.attrib["href"] = name
        a.attrib["target"] = "_blank"
        a.text = name.replace(".html", "")
        td.append(a)
        tr.append(td)
        t.append(tr)

        # Status
        status = x["status"]
        err_msg =x["failed"]

        td = html.Element("td")
        td.tail = prefix
        td.attrib["class"] = status
        td.text = status
        if err_msg != None:
            td.attrib["tooltip"] = err_msg
        tr.append(td)

        # Last Changed
        updated = x["updated"]
        td = html.Element("td")
        td.tail = prefix
        td.text = format_datetime_for_display(updated)
        tr.append(td)

        # Delta
        td = html.Element("td")
        td.tail = prefix
        td.text = format_time_difference(self.start_date, updated) if status != "CHANGED" else ""
        tr.append(td)
        t.append(tr)

        # Live Page
        url = x["url"]
        td = html.Element("td")
        td.tail = prefix
        a = html.Element("a")
        a.attrib["href"] = url
        a.attrib["target"] = "_blank"
        a.text = url
        td.append(a)
        tr.append(td)

        # Pipeline        
        source = x.get("source")
        if source == None: source = "google-states"
        td = html.Element("td")
        td.tail = prefix[:-2]
        div = self._make_source_links(kind, name, source)
        td.append(div)
        tr.append(td)

        t.append(tr)


    def _fill_data_table(self, t: html.Element, kind: str):

        items = self._items
        for x in items:
            self._add_data_row(t, x, kind)
        t[-1].tail = "\n    "
        return t         


    def write_html_to_cache(self, cache: DirectoryCache, kind: str):
        
        title = f"{kind} COVID data - {format_datetime_for_display(self.start_date)}"
        doc = html.fromstring(f"""
<html>
  <head>
        <title>{title}</title>
        <link rel="stylesheet" href="epydoc.css" type="text/css" />
  </head>
  <body>
    <h3>{title}</h3>
    <table id="data" class="data-table">
        <tr><th>Name</th><th>Status</th><th>Changed At</th><th>Delta<th/><th>Live Page</th><th>Pipeline</th></tr>    
    </table>
    <br>
    <hr>
    <table id="info" class="info-table">
        <tr><th colspan="2">Run Information</th></tr>
    </table>
  </body>
</html>
"""
)        
        t_info = doc.get_element_by_id("info")
        t_data = doc.get_element_by_id("data")
        
        self._fill_info_table(t_info)
        self._fill_data_table(t_data, kind)

        fn = os.path.join(cache.work_dir, "index.html")
        with open(fn, "wb") as f_changes:
            f_changes.write(html.tostring(doc))

    # -----------------------------

    def _make_json(self):
        result = {}        
        result["start_date"] = self.start_date 
        result["end_date"] = self.end_date
        result["previous_date"] = self.previous_date
        result["time_lapsed"] = str(self.time_lapsed)
        result["complete"] = self.complete 
        result["error_message"] = self.error_message 

        result["items"] = [x for x in self._items] 

        convert_python_to_json(result)
        return result

    def _write_json(self):
        logger.info(f"time lapsed {self.time_lapsed}")

        result = self._make_json()

        fn = os.path.join(self.cache.work_dir, "change_list.json")
        fn_temp = os.path.join(self.cache.work_dir, "change_list.json.tmp")
        with open(fn_temp, "w") as f_changes:
            json.dump(result, f_changes, indent=2)
        if os.path.exists(fn): os.remove(fn)
        os.rename(fn_temp, fn)

    # {
    #   "name": "AK.html",
    #   "status": "unchanged",
    #   "url": "http://dhss.alaska.gov/dph/Epi/id/Pages/COVID-19/default.aspx",
    #   "msg": null,
    #   "complete": true,
    #   "added": "2020-03-13T06:17:50.550545",
    #   "checked": "2020-03-16T22:00:07.143700",
    #   "updated": "2020-03-16T21:40:10.611841",
    #   "failed": null,
    #   "source": "google-states"
    # }



    def _read_json(self):
        fn = os.path.join(self.cache.work_dir, "change_list.json")

        self._items = []
        if not os.path.exists(fn): return

        with open(fn, "r") as f_changes:
            result = json.load(f_changes)

        convert_json_to_python(result)

        self.start_date = result["start_date"]  
        self.end_date = result["end_date"]
        self.previous_date = result["previous_date"] 
        self.time_lapsed = self.end_date - self.start_date
        self.error_message = result["error_message"] 

        self._items = result["items"]

        self._lookup = { }
        for idx in range(len(self._items)): 
            n = self._items[idx]["name"]
            self._lookup[n] = idx

    def _write_urls(self):
        fn = os.path.join(self.cache.work_dir, "urls.txt")
        with open(fn, "w") as furl:
            furl.write("Name\tUrl\n")
            for x in self._items:
                name, xurl = x["name"], x["url"]
                furl.write(f"{name}\t{xurl}\n")


    def read_urls_as_dict(self) -> Dict:
        result = {}
        with open(os.path.join(self.cache.work_dir, "urls.txt")) as f:
            lines = f.readlines()
            for x in lines[1:]:
                name, xurl = x.split("\t")
                name = name.replace(".html", "")
                result[name] = xurl[:-1]
        return result

