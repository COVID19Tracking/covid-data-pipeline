import sys
import os

from src import check_path
check_path()

from datetime import datetime
from dateutil import tz
import pytz

from shared import udatetime

def test_1():

    print("==== test_1")
    print("")

    edt = pytz.timezone("US/Eastern")
    cdt = pytz.timezone("US/Central")
    local = datetime.now().astimezone() 
    print(f"local tz = {local} {local.tzinfo}")

    print("=== now ===")
    dt = datetime.now()
    print(f"has tz={dt.tzinfo}")
    print(f"naive={dt}")
    print(f"utc={dt.astimezone(pytz.UTC)}")
    print(f"edt={dt.astimezone(edt)}")
    print(f"cdt={dt.astimezone(cdt)}")
    print(f"local={dt.astimezone()}")

    print("=== utc now ===")
    dt = datetime.utcnow()
    print(f"naive={dt}")
    print(f"utc={dt.astimezone(pytz.UTC)}")
    print(f"edt={dt.astimezone(edt)}")
    print(f"cdt={dt.astimezone(cdt)}")
    print(f"local={dt.astimezone()}")

    print("")
    print("")

def test_2():

    print("==== test_2")
    print("")
    local = datetime.now().astimezone() 
    print(f"local = {local} {local.tzinfo}")

    local2 = udatetime.now_as_local()
    print(f"udatetime.now_as_local = {local2} {local.tzinfo}")
    assert(local2.tzinfo == local.tzinfo)
    assert((local2.hour - local.hour) < 1)

    print("")
    print("")

def test_3():

    print("==== test_3")
    print("")
    local = datetime.now().astimezone() 
    print(f"local = {local} {local.tzinfo}")

    utc = udatetime.now_as_utc()
    print(f"udatetime.now_as_utc = {utc} {utc.tzinfo}")
    assert(utc.tzinfo == pytz.UTC)

    local2 = utc.astimezone(local.tzinfo)
    print(f"udatetime.now_as_utc in local time = {local2} {local2.tzinfo}")
    assert((local2.hour - local.hour) < 1)

    print("")
    print("")


if __name__ == "__main__":
    test_1()
    test_2()
    test_3()
