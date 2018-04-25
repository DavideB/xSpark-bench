"""
Plot module to generate figure from the log of the application and worker's logs.

This module can generate two type of figure: Overview, Worker. The Overview figure is the overview
 of the all application with the aggregated cpu metrics from the workers. The Worker figure is the
 detail of the execution of one worker in which each stage as its real progress and estimated progress
 including the allocation of cpu.

"""

import glob
import json
import math
import os
import shutil
from pathlib import Path
from sys import platform

from config import CONFIG_DICT
config={}
config = CONFIG_DICT
with open("config.json", 'w') as file:
    json.dump(config, file, indent = 4)



