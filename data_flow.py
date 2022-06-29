### Imports ###

from datetime import datetime
import time
from access_token import *
from data_preprocess import data_preprocess
import logging
import datetime as dt
import pandas as pd
logging.basicConfig(level=logging.INFO)


class data_flow:

    socket_opened = False

    def __init__(self):

        self.__alice = alice_ins().get_aliceblue_ins()

    def __event_handler_quote_update(self, message):

        live_data = data_preprocess.get_explicit_live_data(message)
        return live_data

    def __open_callback(self):

        data_flow.socket_opened = True

    def __error_callback(self, error):

        print(error)

    def __start_websocket(self):

        self.__alice.start_websocket(subscribe_callback=self.__event_handler_quote_update,
                                     socket_open_callback=self.__open_callback,
                                     socket_error_callback=self.__error_callback,
                                     run_in_background=True)

    def subscribe_nifty100_stocks(self):
        
        try:
            self.__start_websocket()
        except:
            print("Error in socket connection!!!!!")
        else:
            ticker_loc = 'nifty100.csv'
            nifty100_list = []  # list to store tickers as instrument dataytype
            print('Connecting to web socket.....')
            time.sleep(10)
            while not data_flow.socket_opened:
                print("connection failed. Reconnecting...")
                time.sleep(20)

            # nifty 100 list of instrument type to be passed for subscription
            nifty100_ticker = data_handler.get_tickers_list(ticker_loc)
            for ticker in nifty100_ticker:
                nifty100_list.append(
                    self.__alice.get_instrument_by_symbol('NSE', ticker))

            # subscibe to nifty 100 stocks between market hours
            while data_flow.socket_opened:

                if((dt.datetime.now().time() >= dt.time(9, 15, 00)) and (dt.datetime.now().time() <= dt.time(21, 30, 00))):
                    self.__alice.subscribe(
                        nifty100_list, LiveFeedType.FULL_SNAPQUOTE)


    def get_hsticker_data(self, interval):

        try:
            self.__start_websocket()
        except:
            print("Error in socket connection.")
        else:
            ticker_loc = 'nifty100.csv'
            nifty100_list = []  # list to store tickers as instrument dataytype
            print('Connecting to web socket.....')
            time.sleep(10)
            while not data_flow.socket_opened:
                print("connection failed. Reconnecting...")
                time.sleep(20)

            # nifty 100 list of instrument type to be passed for subscription
            nifty100_ticker = data_handler.get_tickers_list(ticker_loc)
            for ticker in nifty100_ticker:
                nifty100_list.append(
                    self.__alice.get_instrument_by_symbol('NSE', ticker))

            from_datetime = datetime.now() -timedelta(10) #starting date
            to_datetime = datetime.now() #ending date
            interval = interval
            indices = True
            hist_df = pd.DataFrame(data_handler.get_historical_data(nifty100_list, from_datetime, to_datetime,
                                                                    interval, indices))

            hist_df.index = hist_df['data']
            hist_df = hist_df.drop('date', axis=1)

        return hist_df



#### Class test area ####
# 1.subscribe_nifty100_stocks function test
#data_flow().subscribe_nifty100_stocks()
interval = '15_MIN' #['DAY', '1_HR', '15_MIN']
data_flow.get_hsticker_data(interval)