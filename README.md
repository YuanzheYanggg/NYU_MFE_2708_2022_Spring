# NYU_MFE_2708_2022_Spring
A code repo for class project



## This project mainly focused on processing and analyzing tick-size data on both trades and quotes of NYSE during 20070620 and 20070920


### The functionalities include 

                            1. Filter out only S&P 500 listed tickers

                            2. Adjust the stock price and size for stock splitting/ stock buyback and so on
                            
                            3. Clean the tick data according to historical rolling window to remove outlier
                            
                            4. Computing different statistical measures on both cleaned and uncleaned data and compare
                            
                            5. Test the auto-correlation between stock returns to compute minimum return resample frequence that will hardly be affected by bid-ask bounce (Through both Ljung-box test and Dickey-Fuller test)
                            
                            6: Result visualization by matplotlib for all previous functionalities
                            
                            7: Portfolio optimization through CVXOPT using historical data during 20070620 - 20070920, and calculate portfolio turnover rate
                            
                 
# In order to successfully deploy our code on your local device, we strongly suggest you follow following instruction:

1)	If you are a mac user, you can use the script file named Unzip.py in our code to help you unzip all the zipped date files for both trades and quotes. If you are a Windows user, you can alternatively do it by unzip all the files in a batch using file unzipper.
2)	Be careful about the directory of data files used in our code. You should have both trades and quotes folders sitting inside a main folder named data, and data should be set under your working directory.
3)	In order to make it easier for user/grader to test our code, we use relative path in all places of our code whenever we use directory path. Namely, you just need to have the correct structure of storing data, you should be fine.
4)	The basic workflow of our code is: TAQ_Filter -> TAQ_Adjust -> TAQ_Cleaner -> TAQ_Stat -> TAQ_AutoCorrelation -> TAQ_Plot. Note that you do not need to run all the script to see the result. We provide a main.py which is the main file for you to go through all the process and exhibit the result.
5)	Since it is a very large dataset, the whole project might blow up your storage space. Prepare more than 100 GB storage space in order to run the project. If you do not have enough space, you can delete all the raw data files (E.g. data/trades/20070620 …) and only save the concatenated parquet.gzip file after your ran TAQ_Adjust. Since all the following steps will not depend on original data but instead on the parquet.gzip data.
                   
