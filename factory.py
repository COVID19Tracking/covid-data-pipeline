from typing import List
from logging import Logger

from .scanner import InventoryScanner, ScannerState, PageCache

from .scanner_aag import ScannerAAG
from .scanner_ford_direct import ScannerFordDirect

def get_list_of_scanners() -> List[str]:
    items = [
        "AAG",
        "FordDirect"
    ]
    return items

def get_scanner(cache: PageCache, state: ScannerState, xlogger: Logger) -> InventoryScanner:

    if state.name == "AAG":
        return ScannerAAG(cache, state, xlogger)
    if state.name == "FordDirect":
        return ScannerFordDirect(cache, state, xlogger)

    scanners = get_list_of_scanners()
    raise NotImplementedError(f"No scanner named {state.name}, validate names are " + ", ".join(scanners))
