

##### A class meant for all kinds of data preprocessing that is required ####

class data_preprocess:

    ### A static method that fetches the live data dictionary and only resends the necessary key ##
    @staticmethod
    def get_explicit_live_data(message):
        live_data = {}
        live_data[message['instrument'].symbol] = {"Open": message['open'],
                                                   "High": message["high"],
                                                   "Low": message["low"],
                                                   "LTP": message["ltp"],
                                                   "close": message["close"],
                                                   "exchange_time_stamp": message["exchange_time_stamp"],
                                                   "Vwap": message["atp"]}

        return live_data
    @staticmethod
    def create_candle_data(message,time_limit):
        live_data = {}
        live_data[message['instrument'].symbol] = {"LTP": message["ltp"],
                                                   "exchange_time_stamp": message["exchange_time_stamp"],
                                                    "atp": message["atp"]}

    @staticmethod
    def get_timeseries_data(message,time_limit):
        live_data = {}
        live_data[message['instrument'].symbol] = {"Open": message['open'],
                                                   "High": message["high"],
                                                   "Low": message["low"],
                                                   "LTP": message["ltp"],
                                                   "close": message["close"],
                                                   "exchange_time_stamp": message["exchange_time_stamp"],
                                                   "Vwap": message["atp"]}

### Class test area ####
# 1. test get_explicit_live_data
# from collections import namedtuple
# Instrument = namedtuple('Instrument', ['exchange', 'token', 'symbol',
#                                       'name', 'expiry', 'lot_size'])
# message = {'exchange': 'NSE', 'token': 14466, 'ltp': 221.05, 'ltt': 1655284029, 'ltq': 1, 'volume': 43435, 'best_bid_price': 220.75, 'best_bid_quantity': 17, 'best_ask_price': 221.05, 'best_ask_quantity': 11, 'total_buy_quantity': 76201, 'total_sell_quantity': 54988, 'atp': 219.56,
#            'exchange_time_stamp': 1655284056, 'open': 216.5, 'high': 224.0, 'low': 214.35, 'close': 215.6, 'yearly_high': 314.0, 'yearly_low': 183.2, 'instrument': Instrument(exchange='NSE', token=14466, symbol='KIRLFER', name='KIRLOSKAR FERROUS IND LTD', expiry=None, lot_size=None)}
# print(data_preprocess.get_explicit_live_data(message))