import constants as const
import yfinance as yf
import pandas as pd
import multitasking
import numpy as np
from managers.DatabaseManager import MongoManager
class DownloadManager:
    def __init__(self) -> None:
        self.stocks = const.STOCKS_TO_TRACK
        self.database = MongoManager()
        self.last_timestamp_dict = self.database.get_last_timestamp()
        self.shared_dict = {j: False for j in self.stocks}
        
    @multitasking.task
    def download_and_update_data(self,stock,current_timestamp,last_traded_timestamp,flag):
        """
        Downloads all the data for required number of days by 
        keeping in account for maximum data that can be downloaded
        from the yfinance at a time. 

        Args:
            stock (str): Ticker symbol for stock
            current_timestamp (pd.Timestamp): Current time
            last_traded_timestamp (pd.Timestamp): Timestamp till the stock prices for ticker are available
            flag (bool): Wheather the last time stamp is available or not
        """
        
        
        ticker = yf.Ticker(stock)
        num_iterations,remainder = divmod((current_timestamp - last_traded_timestamp).days,const.CONSTANT_DAYS_PER_REQUEST)
        
        
        data = pd.DataFrame()
        for i in range(num_iterations+1):
            if i==num_iterations:
                if remainder!=0:
                    days = remainder-1
                    pass
                else:
                    break
            else:
                days = const.CONSTANT_DAYS_PER_REQUEST
                
            start_date = last_traded_timestamp + pd.Timedelta(days=1)
            end_date = start_date + pd.Timedelta(days=days)
            history = ticker.history(interval=const.CONSTANT_TIME_INTERVAL,start=start_date,end=end_date)
            if len(history)!=0:
                data = pd.concat([data,history],axis=0)
                data.sort_index(ascending=True,inplace=True)
                last_traded_timestamp = data.index[-1]
                
                const.LOGGER.info(f"{stock} : Download Successful for start date : {start_date} -> end date : {end_date}")
                
            else:
                const.LOGGER.error(f"{stock} : Download Failed for start date {start_date} -> end date : {end_date}")
                pass
            
        if len(data)==0:
            self.shared_dict[stock] = True
            return
        
        data.index = data.index.tz_localize(None)
        data = data.apply(self._convert_to_dictionary,axis=1).to_list()
        self.database.insert_many(stock,data)
        
        if flag:
            self.database.update_one(const.CONSTANT_TIMESTAMP_COLLECTION,{'ticker': stock},{'$set': {'last_recorded_timestamp': last_traded_timestamp}})
            
            const.LOGGER.info(f"{stock} : Updated Last Traded Timestamp to {last_traded_timestamp}")
            
        else:
            self.database.insert_one(const.CONSTANT_TIMESTAMP_COLLECTION,{'ticker': stock,   'last_recorded_timestamp': last_traded_timestamp})
            
            const.LOGGER.info(f"{stock} : Added Last Traded Timestamp - {last_traded_timestamp}")
            
        self.shared_dict[stock] = True
        return
        

                        
            
    def _convert_to_dictionary(self,row):
        """
        Convert pandas rows to dictionary with each column name as
        key and each row data as value.
        To be used with pandas apply function.

        Args:
            row (pandas_row): a row of data

        Returns:
            dict: Converting each row to dict for mongodb database
        """
        return {
            "Timestamp": row.name,
            "Open": row.Open,
            "High": row.High,
            "Low": row.Low,
            "Close": row.Close,
            "Volume": row.Volume
        }
        
    def download(self):
        
        """
        Downloads the data from yfinance library and updates the mongodb
        database. Also updates the last timestamp in the database.
        """
        
        const.LOGGER.info(f"Starting Download")
        
        current_timestamp = pd.Timestamp.today(tz=None)
        
        for stock in self.stocks:
            
            print(stock)
            if stock in self.last_timestamp_dict.keys():
                print("Last Timestamp found")
                flag = True
                ticker_last_traded = self.last_timestamp_dict[stock]['last_recorded_timestamp']
            else:
                flag = False
                ticker_last_traded = current_timestamp - pd.Timedelta(days=const.CONSTANT_MAX_SIZE_LIMIT)
            
            
            self.download_and_update_data(stock,
                                          current_timestamp,
                                          ticker_last_traded,
                                          flag)
            
        while not np.array(list(self.shared_dict.values())).all():
            continue
            
        return
            
                 
                
    