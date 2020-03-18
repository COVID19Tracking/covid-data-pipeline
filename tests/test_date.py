from datetime import datetime
from dateutil import tz
import pytz

def test_1():

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

if __name__ == "__main__":
    test_1()
