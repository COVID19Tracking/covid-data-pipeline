import os
import sys

if not os.path.isdir(os.path.join(sys.path[0], "data_pipeline", "tests")):
    raise Exception("Tests must be run from base dir of repo")

