# date utilities
#
# rules:
#   1. always use dates with timezone=UTC
#   2. internal state uses UTC
#   3. end-user visible dates use ET
#

from datetime import datetime, timezone, timedelta
import pytz
import re
import os

eastern_tz = pytz.timezone("US/Eastern")

#
# Do not use utcnow.  It returns a naive date at current time in England.
#

def now_as_utc() -> datetime:
    """ get current time as a tz-aware datetime 
    if you are on CT and your local time is 4pm, the UTC time is 9pm 
    """
    xnow = datetime.now().astimezone(pytz.UTC)
    return xnow

def now_as_eastern() -> datetime:
    """ get current time on east-coast as a tz-aware datetime 
    """
    xnow = datetime.now().astimezone(eastern_tz)
    return xnow


def now_as_local() -> datetime:
    """ get current time as a tz-aware datetime 
    """
    xnow = datetime.now().astimezone()
    return xnow

def file_modified_at(xpath: str) -> datetime:
    """ get modification date of file """

    #print(xpath)
    mtime = os.path.getmtime(xpath)
    mtime = datetime.fromtimestamp(mtime).as_timezone().as_timezone(pytz.UTC)
    return mtime

def file_age(xpath: str) -> float:
    """ get age of a file in minutes """

    #print(xpath)
    mtime = os.path.getmtime(xpath)
    mtime = datetime.fromtimestamp(mtime)

    xnow = datetime.now()
    xdelta = (xnow - mtime).seconds / 60.0

    return xdelta

def to_filenameformat(dt: datetime) -> str:
    " format a date for use in file names "
    if dt == None: return None
    require_utc(dt)
    return dt.strftime('%Y%m%d-%H%M%SZ')

def to_logformat(dt: datetime) -> str:
    " format a date for use in logging messages "
    if dt == None: return "[none]"
    require_utc(dt)
    return dt.astimezone(eastern_tz).strftime('%Y-%m-%d %H:%M:%S %Z')

def to_displayformat(dt: datetime) -> str:
    " format a date to display to user "
    if dt == None: return ""
    require_utc(dt)
    return dt.astimezone(eastern_tz).strftime('%Y-%m-%d %H:%M:%S %Z')

def from_json(s: str) -> datetime:
    " import a date from json, expected to be UTC "
    if type(s) != str: raise Exception(f"value ({s}) is a str")
    return datetime.fromisoformat(s).astimezone(pytz.UTC)

def to_json(dt: datetime) -> str:
    " export a date from json in isoformat "
    if dt == None: return None
    require_utc(dt)
    return dt.isoformat()

def from_local_naive(dt: datetime) -> datetime:
    " convert a  naive date in local time to tz-UTC"
    if dt.tzinfo != None:
        raise Exception(f"value ({dt} {dt.tzinfo}) already has tz")
    #logger.warning("converting local naive date to tz-UTC date")
    return dt.astimezone().astimezone(pytz.UTC)

def format_difference(dt1: datetime, dt2: datetime) -> str:
    " format the difference between two dates for display"
    if dt1 == None or dt2 == None: return ""
    require_utc(dt1)
    require_utc(dt2)

    if dt1 < dt2: return "NOW"
    delta = dt1 - dt2
    if delta.days != 0:
        return "OLD"
    else:
        sec = delta.seconds
        h = sec // (60*60)
        m = (sec - h * 60*60) // 60
        return f"{h:02d}:{m:02d}"

def is_isoformated(s: str) -> False:
    " test if value is an iso-formatted string"
    if type(s) != str: return False
    #2020-03-13T06:17:50.204477
    return re.match("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{6})?", s)

def format_mins(x : float) -> str:
    if x < 60.0:
        return f"{x:.0f} mins"
    x /= 60.0
    if x < 24.0:
        return f"{x:.1f} hours"
    return f"{x:.1f} days"

def require_utc(dt: datetime) -> datetime:
    " require value to be an tz-UTC date "
    if dt == None: return None
    if type(dt) != datetime:
        if type(dt) == str:
            if is_isoformated(dt):
                raise Exception(f"value ({dt}) is a str containing an isoformated date")
        raise Exception(f"type ({type(dt)}) is not datetime")
    if dt.tzname() != "UTC":
        raise Exception(f"value ({dt}, dt.tzname={dt.tzname()}) is not UTC")
    return dt
