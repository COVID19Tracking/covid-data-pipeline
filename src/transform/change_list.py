import os
import json
from loguru import logger
from typing import Dict, Tuple, List
from datetime import datetime
from lxml import html

from shared.directory_cache import DirectoryCache
from shared.util import convert_json_to_python, convert_python_to_json
from shared import udatetime
from transform import html_helpers

class ChangeItem:
    " status data about a link "

    __slots__ = (
        "name", "source", "status", "url", 
        "msg", "complete",
        "added", "checked", "updated", "failed"
    )

    def __init__(self, vals: Dict = None):
        self.name = ""
        self.source = ""
        self.status =  ""
        self.url = ""
        self.msg = ""
        self.complete = False,
                
        self.added:datetime = None
        self.checked:datetime = None
        self.updated:datetime = None
        self.failed:datetime = None

        if vals != None:
            self.from_dict(vals)

    def to_dict(self) -> Dict:
        y = { "name": self.name, "source": self.source, "status": self.status, 
            "url": self.url, "msg": self.msg, "complete": self.complete,
            "added": udatetime.require_utc(self.added), 
            "checked": udatetime.require_utc(self.checked), 
            "updated": udatetime.require_utc(self.updated), 
            "failed": udatetime.require_utc(self.failed) 
        }
        return y

    def copy(self):
        x = self.to_dict()
        return ChangeItem(x)

    def from_dict(self, y: Dict):
        self.name = y["name"]
        self.source = y.get("source")
        self.status =  y["status"]
        self.url = y["url"]
        self.msg = y["msg"]
        self.complete = y["complete"]
                
        self.added = udatetime.require_utc(y["added"])
        self.checked = udatetime.require_utc(y["checked"])
        self.updated = udatetime.require_utc(y["updated"])
        self.failed = udatetime.require_utc(y["failed"])


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

        '_is_loaded',
        '_items',
        '_lookup',

        'last_timestamp'
    )

    def __init__(self, cache: DirectoryCache):
        self.cache = cache

        self.start_date = udatetime.now_as_utc()
        self.end_date = self.start_date
        self.previous_date = self.start_date
        self.time_lapsed = self.end_date - self.start_date
        self.error_message = None
        self.complete = False

        self.last_timestamp = ""

        self._is_loaded = False

        self._items = []
        self._lookup = {}

    def load(self):
        if self._is_loaded: return
        self._read_json()
        self._is_loaded = True

    def start_run(self):
        self.load()

        self.previous_date = self.start_date
        self.start_date = udatetime.now_as_utc()
        self.end_date = self.start_date
        self.time_lapsed = self.end_date - self.start_date
        
        self.error_message = None
        self.complete = False
        for x in self._items: x.complete = False


    def save_progress(self):
        self.end_date = udatetime.now_as_utc()
        self.time_lapsed = self.end_date - self.start_date

        self._write_json()
        self._write_text()
        self._write_urls()

    def abort_run(self, ex: Exception):
        self.error_message = str(ex)

    def finish_run(self):
        self.complete = True
        self.save_progress()

    def get_item(self, name: str) -> ChangeItem:
        idx = self._lookup.get(name)
        if idx == None: return None
        return self._items[idx]

    def get_minutes_since_last_check(self, name: str) -> float:
        " get time since last check in minutes "
        
        idx = self._lookup.get(name)
        if idx == None: return 100000.0

        x = self._items[idx]
        checked_date = udatetime.require_utc(x.checked)
        if checked_date == None: return 100000.0
        delta = self.start_date - checked_date

        return delta.total_seconds() / 60.0

    def update_status(self, name: str, source: str, status: str, xurl: str, msg: str) -> Tuple[ChangeItem, ChangeItem, str]:
        
        xnow = udatetime.now_as_utc()

        if msg == "": msg = None
        idx = self._lookup.get(name)
        if idx == None:         
            x = None               
            y = ChangeItem({ "name": name, "source": source, "status": status, "url": xurl, "msg": msg, "complete": True,
                  "added": xnow, "checked": None, "updated": None, "failed": None })
            self._lookup[name] = len(self._items)
            self._items.append(y)
        else:
            x = self._items[idx]
            y = x.copy()
            y.status = status
            y.source = source
            y.url = xurl
            y.msg = msg
            y.complete = True
            self._items[idx] = y
            
        return y, x, xnow

    def record_failed(self, name: str, source: str, xurl: str, msg: str):

        status = "FAILED"
        logger.error(f"     {name}: {status}")
        logger.error(f"     {name}: url={xurl} msg={msg}")

        y, x, xnow = self.update_status(name, source, status, xurl, msg)
        if x != None and x.status != "FAILED":
            y.failed = xnow


    def record_unchanged(self, name: str, source: str, xurl: str, msg: str = ""):

        status = "unchanged"
        logger.info(f"  {name}: {status}")

        y, _, xnow = self.update_status(name, source, status, xurl, msg)
        y.checked = xnow
        y.failed = None

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
            y.status = x.status
            y.msg = x.msg

    def record_changed(self, name: str, source: str, xurl: str, msg: str = ""):

        status = "CHANGED"
        logger.warning(f"    {name}: {status}")

        y, _, xnow = self.update_status(name, source, status, xurl, msg)
        y.checked = xnow
        y.updated = xnow
        y.failed = None

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
                if x.source == None: x.source = ""
                if x.msg == None: x.msg = ""
                if s != status: continue
                f.write(f"{x.name}\t{x.status}\t{x.source}\t{x.url}\t{x.msg}\n")
            f.write(f"\n")

        with open(fn, "w") as f_changes:
            f_changes.write(f"CHANGE LIST\n\n")
            f_changes.write(f"  start\t{udatetime.to_displayformat(self.start_date)}\n")
            f_changes.write(f"  end\t{udatetime.to_displayformat(self.end_date)}\n")
            f_changes.write(f"  previous\t{udatetime.to_displayformat(self.previous_date)}\n")
            f_changes.write(f"  lapsed\t{self.time_lapsed}\n")
            f_changes.write(f"\n")

            status = {}
            for x in self._items:
                s = x.status
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

        self._add_html_info_row(t, "Started At", udatetime.to_displayformat(self.start_date))
        self._add_html_info_row(t, "Ended At", udatetime.to_displayformat(self.end_date))
        self._add_html_info_row(t, "Lapse Time (mins)", str(self.time_lapsed))
        self._add_html_info_row(t, "Previous Run At", udatetime.to_displayformat(self.previous_date))
        self._add_html_info_row(t, "Error Message", self.error_message, 
            "err" if self.error_message else None)
        t[-1].tail = "\n    "


    def _add_data_row(self, t: html.Element, x: ChangeItem, kind: str):

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

        name = x.name
        status = x.status

        if name == "main_sheet.html": return
        if name.endswith("_data.html") and status == "duplicate": return

        prefix = "\n      "

        tr = html.Element("tr")
        tr.tail = prefix

        # Name
        td = html.Element("td")
        td.tail = prefix
        a = html.Element("a")
        a.attrib["href"] = name
        a.text = name.replace(".html", "")
        td.append(a)
        tr.append(td)
        t.append(tr)

        # Status
        td = html.Element("td")
        td.tail = prefix
        td.attrib["class"] = status
        td.text = status
        tr.append(td)

        # Last Changed
        updated_at = x.updated
        failed_at = x.failed
        td = html.Element("td")
        td.tail = prefix
        if failed_at != None:
            td.attrib["class"] = "failed"
            td.text = udatetime.to_displayformat(failed_at)
        else:
            td.text = udatetime.to_displayformat(updated_at)
        tr.append(td)

        # Delta
        td = html.Element("td")
        td.tail = prefix

        v = updated_at if failed_at == None else failed_at
        td.text = udatetime.format_difference(self.start_date, v) if status != "CHANGED" else ""
        tr.append(td)
        t.append(tr)

        # Live Page
        url = x.url
        td = html.Element("td")
        td.tail = prefix
        a = html.Element("a")
        a.attrib["href"] = url
        if len(url) < 80:
            a.text = url
        else:
            a.text = url[0: 80] + " ..."
            a.attrib["class"] = "tooltip"
            s = html.Element("span")
            s.text = url
            s.attrib["class"] = "tooltiptext"
            a.append(s)
        td.append(a)
        tr.append(td)

        # Pipeline        
        source = x.source
        if source == None: source = "google-states"
        td = html.Element("td")
        td.tail = prefix[:-2]
        div = html_helpers.make_source_links(kind, name, source)
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
        
        title = f"{kind} COVID data - {udatetime.to_displayformat(self.start_date)}"
        doc = html.fromstring(f"""
<html>
  <head>
        <title>{title}</title>
        <link rel="stylesheet" href="../style.css" type="text/css" />
  </head>
  <body>
    <h3>{title}</h3>
    <table id="data" class="data-table">
        <tr><th>Name</th><th>Status</th><th>Changed At</th><th>Delta</th><th>Source</th><th>Pipeline</th></tr>    
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

        result["items"] = [ x.to_dict() for x in self._items ] 

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
        if not os.path.exists(fn):
            logger.warning(f"could not load {fn}") 
            return

        logger.info(f"read {fn}") 

        with open(fn, "r") as f_changes:
            result = json.load(f_changes)

        convert_json_to_python(result)

        self.start_date = result["start_date"]  
        self.end_date = result["end_date"]
        self.previous_date = result["previous_date"] 
        self.time_lapsed = self.end_date - self.start_date
        self.error_message = result["error_message"] 

        self._items = [ChangeItem(x) for x in result["items"]]

        logger.info(f"  found {len(self._items)} items") 

        self._lookup = { }
        for idx in range(len(self._items)): 
            n = self._items[idx].name
            self._lookup[n] = idx

    def _write_urls(self):
        fn = os.path.join(self.cache.work_dir, "urls.txt")
        with open(fn, "w") as furl:
            furl.write("Name\tUrl\n")
            for x in self._items:
                name, xurl = x.name, x.url
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

