##imports

import pickle
from datetime import date
import os
import pandas as pd
from jproperties import Properties




class error_handler:

    def __init__(self):
        self.cache_loc = None

    def cache(self, loc):
        self.cache_loc = loc
        return self.cache_loc
