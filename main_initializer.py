from s3_handler import s3_handler
from data_flow import *

# Defining main function
def main():
    data_flow().subscribe_nifty100_stocks()


# Using the special variable
# __name__
if __name__ == "__main__":
    main()


# uploading stock data file to AWS s3
# s3_handler.upload_to_s3()
