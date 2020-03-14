import re

def is_guid(xid: str) -> bool:
    return re.match("[a-zA-Z]*[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", xid) != None

xid = "errb7391988-6a42-4168-b4d9-a7f93e49c0a5"
print(f"{xid}: {is_guid(xid)}")
