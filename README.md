# NYU_MFE_2708_2022_Spring
A code repo for class project

This project mainly focused on processing and analyzing tick-size data on both trades and quotes of NYSE during 20070620 and 20070920
The functionalities include 1. Filter out only S&P 500 listed tickers
                            2. Adjust the stock price and size for stock splitting/ stock buyback and so on
                            3. Clean the tick data according to historical rolling window to remove outlier
                            4. Computing different statistical measures on both cleaned and uncleaned data and compare
                            5. Test the auto-correlation between stock returns to compute minimum return resample frequence that will hardly be affected by bid-ask bounce (Through both Ljung-box test and Dickey-Fuller test)
                            6: Result visualization by matplotlib for all previous functionalities
                            7: Portfolio optimization through CVXOPT using historical data during 20070620 - 20070920, and calculate portfolio turnover rate
                    
