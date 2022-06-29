# imports

import csv
import pickle
from datetime import date
import os
import pandas as pd
from jproperties import Properties
from error_handler import *
import requests, json
import dateutil.parser
from datetime import datetime, timedelta

#### A class to handle read,write, append data to csv or pcikle file(not meant for preprocessing) #####
### Utility class mostly ###

class data_handler:

    # ## static method to fetch credentials from excel ###

    @staticmethod
    def get_credential_from_excel(loc):

        rows = []
        excel_dict = {}

        # reading csv file

        with open(loc, 'r') as csvfile:

            # creating a csv reader object

            csvreader = csv.reader(csvfile)

            # extracting each data row one by one

            for row in csvreader:
                rows.append(row)

            # ## create the credentials dictionary to be passed

            for row in rows:
                for i in range(0, len(row)):
                    if i == 0:
                        key = row[i]
                    else:
                        value = row[i]
                excel_dict.update({key: value})

        return excel_dict

    # ## static method to fetch nifty 100 stock list ####

    @staticmethod
    def get_tickers_list(loc):

        rows = []

        # reading csv file

        with open(loc, 'r') as csvfile:
            # creating a csv reader object

            csvreader = csv.reader(csvfile)

            # extracting field names through first row

            fields = next(csvreader)

            # extracting each data row one by one

            for row in csvreader:
                rows.append(row[2])

        return rows

    # creates the file and the folder(only the dated folder) where we need to save the data
    # for additional folder creation, please input the folder name to be created
    @staticmethod
    def create_path():
        # current date
        dt = str(date.today())

        # Directory
        directory = dt
        # print(directory)

        # Parent Directory path
        parent_dir = "Data"

        # Path
        path = (parent_dir + '/' + dt)
        # print(path)
        try:
            os.mkdir(path)
        except OSError as error:
            if error.winerror == 183:
                pass
            # elif error.winerror == 3:
            #     parent_dir = input("specified folder not found.\nEnter the folder name to create one:")
            #     os.mkdir(parent_dir)
            #     path = (parent_dir + '/' + dt)
            #     os.mkdir(path)
            #     pass
            else:
                print(error)
        # tic_data_path = ('{}/{}'.format(path, tic_name))
        # tic_fh = open(tic_data_path, 'w')
        return path

    ## creates the pickle file EOD##

    @staticmethod
    def create_pickle_file(tic_name):
        dt = str(date.today())
        path = data_handler.create_path()
        pickle_path = path + '/' + 'Pickle_data'
        try:
            os.mkdir(pickle_path)
        except OSError as error:
            if error.winerror == 183:
                pass
            else:
                print(error)
        tic_data_path = ('{}/{}.pkl'.format(pickle_path, tic_name))
        # tic_fh = open(tic_data_path, 'w')
        return tic_data_path  # ,tic_fh

    @staticmethod
    def create_metadata(tic_name):
        dt = str(date.today())
        path = data_handler.create_path()
        data_path = path + '/' + 'Metadata'
        try:
            os.mkdir(data_path)
        except OSError as error:
            if error.winerror == 183:
                pass
            else:
                print(error)
        tic_data_path = ('{}/{}.pkl'.format(data_path, tic_name))
        # tic_fh = open(tic_data_path, 'w')
        return tic_data_path  # ,tic_fh

    # A method that reads the data file saved as pickle
    @staticmethod
    def read_pickle_file(loc):

        try:
            tic_fh = open(loc, 'rb')
            data = []
            with open(loc, 'rb') as fr:
                try:
                    while True:
                        data.append(pickle.load(fr))
                except EOFError:
                    pass
                # print(data)
            return data
        except:
            print('Ticker does not exist.\nError fetching data from file: {}'.format(loc))

    @staticmethod
    def stock_metadata(message):
        try:
            for key, value in message.items():
                data_df = pd.Series(value).to_frame().transpose()
                try:
                    temp_df = pd.read_pickle(data_handler.create_metadata(key))
                    new_data_df = pd.concat([data_df, temp_df])
                except:
                    new_data_df = data_df
                    pass
                key_list = ['Open', 'High', 'Low', 'LTP', 'close', 'exchange_time_stamp', 'Vwap']
                final_data = new_data_df[key_list]
                final_data.to_pickle(data_handler.create_metadata(key))
        except:
            print('Data fetching interrupted....')



    ### A static method to store live data in pickle format stock wise after eod ###

    @staticmethod
    def auto_update_eod_pickle(tic_list):
        for tic_name in tic_list:
            csv_file_path = data_handler.create_csv_path(tic_name)
            try:
                eod_df = data_handler.read_csv(tic_name)
                # print(eod_df)
                eod_df.to_pickle(data_handler.create_pickle_file(tic_name))
            except EOFError:
                print("Data fetching interrupted {}".format(tic_name))



    ### A static method to create csv file stock wise ###
    @staticmethod
    def create_csv_path(tic_name):
        dt = str(date.today())
        path = data_handler.create_path()
        csv_folder = path + '/' + 'CSV_Data'
        try:
            os.mkdir(csv_folder)
        except OSError as error:
            if error.winerror == 183:
                pass
            elif error.winerror == 3:
                parent_dir = input("specified folder not found.\nEnter the folder name to create one:")
                os.mkdir(parent_dir)
                path = (parent_dir + '/' + dt)
                os.mkdir(path)
                csv_folder = path + '/' + 'CSV_Data'
                pass
            else:
                print(error)
        csv_data_path = ('{}/{}.csv'.format(csv_folder, tic_name))
        return csv_data_path

    ### A static method to read csv file stock wise ###
    @staticmethod
    def read_csv(tic_name):
        tic_loc = data_handler.create_csv_path(tic_name)
        tic_df = pd.read_csv(tic_loc)
        return tic_df

    ### A static method to write to a  csv file stock wise ###
    @staticmethod
    def write_to_csv(message):
        try:
            for key, value in message.items():
                data_df = pd.Series(value).to_frame().transpose()
                # print(data_df)
                try:
                    prev_data = data_handler.read_csv(key)
                    
                    #error_handler.cache_loc(data_handler.cache_file_handler(prev_data))
                    new_data_df = pd.concat([data_df, prev_data])

                    # print(new_data_df)

                except:
                    #new_data_df = pd.concat([pd.read_pickle(error_handler.cache_loc.cache_loc), data_df])
                    new_data_df = data_df
                    data_handler.create_csv_path(key)
                    #print('pointer is here')
                    pass
                csv_tic_path = data_handler.create_csv_path(key)
                csv_fh = open(csv_tic_path, 'w')
                key_list = ['Open', 'High', 'Low', 'LTP', 'close', 'exchange_time_stamp', 'Vwap']
                final_data = new_data_df[key_list]
                final_data.to_csv(csv_tic_path)
                print('Appending data to {}.csv'.format(key))
        except:
            print('Unable to read data....')



    #### A static method to read the environment properties file ####
    @staticmethod
    def read_properties():
        configs = Properties()
        with open('env.properties', 'rb') as read_prop:
            configs.load(read_prop)
        return configs


    @staticmethod
    def get_historical_data(instrument, from_datetime, to_datetime, interval, indices=False):
        params = {"token": instrument.token,
                  "exchange": instrument.indices if not indices else "NSE_INDICES",
                  "starttime": str(int(from_datetime.timestamp())),
                  "endtime": str(int(from_datetime.timestamp())),
                  "candletype": 3 if interval.upper() == 'Day' else (
                      2 if interval.upper().split("_")[1] == "HR" else 1),
                  "data_duration": None if interval.upper() == 'Day' else interval.split('_')[0]}
        lst = requests.get(
            f" https://ant.aliceblueonline.com/api/v1/charts/tdv?", params=params).json()['data']['candles']

        records = []
        for i in lst:
            record = {'date': dateutil.parser.parse(i[0]), 'open': i[1], 'high': i[2], 'low': i[3], 'close': i[4],
                      'volume': i[5]}
            records.append(record)
        return records



    # @staticmethod
    # def cache_file_handler(df):
    #     path = data_handler.create_path()
    #     cache_folder = path + '/' + 'cache'
    #     try:
    #         os.mkdir(cache_folder)
    #     except OSError as error:
    #         if error.winerror == 183:
    #             pass
    #         else:
    #             print(error)
    #     cache_data_path = ('{}/cache.pkl'.format(cache_folder))
    #     df.to_pickle(cache_data_path)
    #     return cache_data_path

## #####Test area to check working of class######
# 1. test for excel credentials
# loc = 'login-credentials.csv'
# excel_dict = data_handler.get_credential_from_excel(loc)
# print(excel_dict)
# data_handler.get_credential_from_excel(loc)

# 2. test for ticker list from csvfile
# loc = 'nifty100.csv'
# print(data_handler.get_nifty100_list(loc))

# 3. test auto_update_eod_pickle
# loc = 'nifty100.csv'
# list = data_handler.get_nifty100_list(loc)

# data_handler.auto_update_eod_pickle(list)

# 4. Test read_pickle_file(tic_name)
# Run the #3 test and then run the test with tic_name = 'ITC'

# 5. test create_pickle_file(tic_name)
# create_pickle_file('RELIANCE')
# create_pickle_file('RELIANCE')

# 6. test write_to_csv
# message = {'KIRLFER': {'Open': 216.5, 'High': 224.0, 'Low': 214.35, 'LTP': 221.05,
#                    'close': 215.6, 'exchange_time_stamp': 1655284056, 'Vwap': 219.56}}
# data_handler.write_to_csv(message)

# 7. Test read_csv_tic
#message = {'KIRLFER': {'Open': 216.5, 'High': 224.0, 'Low': 214.35, 'LTP': 221.05,'close': 215.6, 'exchange_time_stamp': 1655284056, 'Vwap': 219.56}}
#data_handler.write_to_csv(message)
#print(data_handler.read_csv('KIRLFER'))


 #7. Test cache_file_handler
#message = {'KIRLFER': {'Open': 216.5, 'High': 224.0, 'Low': 214.35, 'LTP': 221.05,'close': 215.6, 'exchange_time_stamp': 1655284056, 'Vwap': 219.56}}
#data_handler.write_to_csv(message)
#print(data_handler.read_csv('KIRLFER'))
#print(data_handler.read_pickle_file(data_handler.cache_file_handler(data_handler.read_csv('KIRLFER'))))