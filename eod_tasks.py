from data_handler import *
from datetime import datetime
import datetime as dt

class eod_tasks:

    @staticmethod
    def run_pickle():
        if ((dt.datetime.now().time() >= dt.time(15, 45, 00)) and (dt.datetime.now().time() <= dt.time(21, 30, 00))):
            loc = 'nifty100.csv'
            list = data_handler.get_nifty100_list(loc)
            data_handler.auto_update_eod_pickle(list)

eod_tasks.run_pickle()