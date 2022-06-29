# upload csv file to s3

from io import StringIO
import pandas as pd
import logging
import boto3
from botocore.exceptions import ClientError
from data_handler import *


class s3_handler:

    @staticmethod
    def upload_to_s3():

        configs = data_handler.read_properties()

        # read access and secret key from anv.properties file
        access_key = configs.get("access_key_s3").data
        secret_key = configs.get("secret_key_s3").data

        hc = pd.read_csv('data_file_source/stocks_nyse.csv')

        s3 = boto3.client('s3', aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)

        csv_buf = StringIO()

        hc.to_csv(csv_buf, header=True, index=False)

        csv_buf.seek(0)

        try:

            response = s3.put_object(Bucket='archive-stock-data',
                                     Body=csv_buf.getvalue(), Key='stockData')

        except ClientError as e:
            logging.error(e)
            print('An error ocurred')

        print('File successfully uploaded')
        print(response)


######## Class test area #######
# uploading stock data file to AWS s3

# s3_handler.upload_to_s3()

# Expected output:
# File successfully uploaded
