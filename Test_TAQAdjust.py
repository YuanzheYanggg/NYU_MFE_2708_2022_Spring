import unittest
import TAQAdjust
import os
import pandas as pd


class Test_TAQAdjust(unittest.TestCase):

    def test1(self):
        # we create sub-folders trades_test and quotes_test as our test case
        # it is time-saving especially under the circumstance of huge dataset
        # we comment out this part of code because we already compiled and run it on our local device
        # and the result is already saved to local files

        # for grader:
        # please create trade_test and quotes_test in selected dates from trades and quotes folder
        # uncomment the code below and change the corresponding dir path
        # and generate your own result to your local files
        '''
        relative_path = "./data"

        trade_path = os.path.join(relative_path, "trades_test")
        quote_path = os.path.join(relative_path, "quotes_test")
        trade_dir = os.listdir(trade_path)
        quote_dir = os.listdir(quote_path)
        factor_df = pd.read_csv("S&P500_factors.csv")
        splitting_df = pd.read_csv("splitting_info.csv")
        for trade in trade_dir:
            if not trade.startswith("."):
                date = trade
                TAQAdjust_obj = TAQAdjust(date, factor_df, splitting_df)
                TAQAdjust_obj.retrieve_trade()

        for quote in quote_dir:
            if not quote.startswith("."):
                date = quote
                TAQAdjust_obj = TAQAdjust(date, factor_df,splitting_df)
                TAQAdjust_obj.retrieve_quote()
        TAQAdjust.concat_date()
        '''

        # test if adjust price according to factor works

        '''
        we know that ticker named TXT experienced a change in factor at 2007-08-27
        2007-08-24	TXT	2.0	2.0	1
        2007-08-27	TXT	1.0	1.0	1
        this is get from S&P500_factors.py
        
        In fact,
        you can find all the splitting dates for all stocks in splitting_info
        Ticker,Splitting_date
        ABC,2007-08-01
        AGN,2007-06-25
        ESRX,2007-06-25
        GILD,2007-06-25
        KHD,2007-09-10
        MS,2007-07-02
        MTW,2007-09-11
        NE,2007-08-29
        NVDA,2007-09-11
        OMC,2007-06-26
        TXT,2007-08-27
        TYC,2007-07-02
        YUM,2007-06-27
        '''

        # we chose TXT as our test ticker
        # so our trade_test and quote_test should definitely include 2007-08-24 and 2007-08-27

        # test trade
        trade_df = pd.read_parquet("./data/trades_test/Daily_trade/TXT.parquet.gzip")
        trade_df.sort_values(by=["Date","Millis"], inplace=True)
        before = trade_df.loc[trade_df.Date == pd.to_datetime("2007-08-24")]
        after = trade_df.loc[trade_df.Date == pd.to_datetime("2007-08-27")]
        # since we know that the converter vector is changing from 2 to 1
        # this is saying for all the historical dates eg. 2007-08-24
        # we should have original price is twice larger as the adjusted price
        self.assertAlmostEquals(before.iloc[0]["Price"], 113.290001, 3)
        self.assertAlmostEquals(before.iloc[0]["Adjusted_price"], 56.645000, 3)
        # since the adjusted price is halved, in order to keep the same market value, we need to double the size for TXT
        self.assertAlmostEquals(before.iloc[0]["Size"], 7100, 3)
        self.assertAlmostEquals(before.iloc[0]["Adjusted_size"], 14200.0, 3)


        # for dates after the splitting date
        # the original price and adjusted price should be the same because no more conversion is needed
        # every price is now on the same measure
        self.assertAlmostEquals(after.iloc[0]["Price"], 57.340000, 3)
        self.assertAlmostEquals(after.iloc[0]["Adjusted_price"], 57.340000, 3)
        self.assertAlmostEquals(after.iloc[0]["Size"], 300, 3)
        self.assertAlmostEquals(after.iloc[0]["Adjusted_size"], 300.0, 3)

        # test quote
        quote_df = pd.read_parquet("./data/quotes_test/Daily_quote/TXT.parquet.gzip")
        before = quote_df.loc[quote_df.Date == pd.to_datetime("2007-08-24")]
        after = quote_df.loc[quote_df.Date == pd.to_datetime("2007-08-27")]
        # since we know that the converter vector is changing from 2 to 1
        # this is saying for all the historical dates eg. 2007-08-24
        # we should have original price is twice larger as the adjusted price
        self.assertAlmostEquals(before.iloc[0]["Bid_price"], 113.059998, 3)
        self.assertAlmostEquals(before.iloc[0]["Ask_price"], 113.500000, 3)
        self.assertAlmostEquals(before.iloc[0]["Adjusted_bid_price"], 56.529999, 3)
        self.assertAlmostEquals(before.iloc[0]["Adjusted_ask_price"], 56.750000, 3)

        # since the adjusted price is halved, in order to keep the same market value, we need to double the size for TXT
        self.assertAlmostEquals(before.iloc[0]["Ask_size"], 1, 3)
        self.assertAlmostEquals(before.iloc[0]["Bid_size"], 9, 3)
        self.assertAlmostEquals(before.iloc[0]["Adjusted_bid_size"], 18.0, 3)
        self.assertAlmostEquals(before.iloc[0]["Adjusted_ask_size"], 2.0, 3)

        # for dates after the splitting date
        # the original price and adjusted price should be the same because no more conversion is needed
        # every price is now on the same measure
        self.assertAlmostEquals(after.iloc[0]["Bid_price"], 57.340000, 3)
        self.assertAlmostEquals(after.iloc[0]["Ask_price"], 57.340000, 3)
        self.assertAlmostEquals(after.iloc[0]["Adjusted_bid_price"], 57.340000, 3)
        self.assertAlmostEquals(after.iloc[0]["Adjusted_ask_price"], 57.340000, 3)
        # since the adjusted price is halved, in order to keep the same market value, we need to double the size for TXT
        self.assertAlmostEquals(after.iloc[0]["Ask_size"], 16, 3)
        self.assertAlmostEquals(after.iloc[0]["Bid_size"], 13, 3)
        self.assertAlmostEquals(after.iloc[0]["Adjusted_bid_size"], 13.0, 3)
        self.assertAlmostEquals(after.iloc[0]["Adjusted_ask_size"], 16.0, 3)


if __name__ == "__main__":
    unittest.main()
