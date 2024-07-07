import pandas as pd
import constants as const
import os
from managers.DatabaseManager import MongoManager

class MongoToParquet:
    def __init__(self):
        self.get_single_day_query = {
            '$expr': {
                '$eq': [
                    {
                        '$dateToString': {
                            'format': '%Y-%m-%d', 
                            'date': '$Timestamp'
                        }
                    }, 'date'
                ]
            }
        }
        
        self.get_all_dates_query = [
            {
                '$project': {
                    'date': {
                        '$dateToString': {
                            'format': '%Y-%m-%d', 
                            'date': '$Timestamp'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': '$date', 
                    'TotalNumber': {
                        '$sum': 1
                    }
                }
            }, {
                '$match': {
                    'TotalNumber': {
                        '$gt': 200
                    }
                }
            }, {
                '$sort': {
                    '_id': 1
                }
            }
        ]
        self.database_manager = MongoManager()
        self.last_timestamp_dict = self.database_manager.get_last_timestamp()
        self.save_dir = const.PARQUET_FOLDER
        
    
    def create_folder_if_not_exist(self,collection_name):
        if not os.path.exists(os.path.join(self.save_dir,collection_name)):
            os.makedirs(os.path.join(self.save_dir,collection_name))
        
    
    def save(self,collection_name):
        
        available_time_stamps = self.database_manager.aggregate(
            collection_name,
            self.get_all_dates_query
        )
        available_time_stamps = pd.DataFrame(available_time_stamps)
        try:
            last_date = self.last_timestamp_dict[collection_name]['last_parquet_date']
            last_date_idx = available_time_stamps.query(f'_id == {last_date}').index
            available_time_stamps = available_time_stamps.iloc[last_date_idx+1:]
        except Exception as e:
            # const.LOGGER.error
            print(f"{collection_name} : Unable to Read last parquet date. Exited with error {e}")
            
        self.create_folder_if_not_exist(collection_name)
        
        for time_stamp in available_time_stamps['_id']:
            
            self.get_single_day_query['$expr']['$eq'][1] = time_stamp
            stock_prices = self.database_manager.find_all(
                collection_name,
                self.get_single_day_query
            )
            stock_prices = pd.DataFrame(stock_prices)
            # print(stock_prices)
            stock_prices.drop("_id",axis=1,inplace=True)
            # const.LOGGER.info
            print(f"{collection_name} : Writing {time_stamp}.parquet.gzip file.")
            stock_prices.to_parquet(
                f"{self.save_dir}/{collection_name}/{time_stamp}.parquet.gzip",
                compression="gzip"
            )
            print(f"{collection_name} : Updated last timestamp for writing parquet data")
            self.database_manager.update_one("last_timestamp",{'ticker': collection_name},{'$set': {'last_parquet_date': time_stamp}})
        
    def save_all(self):
        available_collections = self.database_manager.get_all_collections(name=True)
        for collection in available_collections:
            if collection in self.last_timestamp_dict.keys():
                self.save(collection_name=collection)
            
        
        
        

# if __name__ == "__main__":
#     converter = MongoToParquet()
#     converter.convert()
        
        
    
    
        