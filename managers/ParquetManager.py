import pandas as pd
import constants as const
from managers.DatabaseManager import DatabaseManager

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
        self.database_manager = DatabaseManager()
        self.last_timestamp_dict = self.database_manager.get_last_timestamp()
        
        
    
    def convert(self,collection_name):
        
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
        
        for time_stamp in available_time_stamps['_id']:
            
            self.get_single_day_query['$expr']['$eq'][1] = time_stamp
            print(self.get_single_day_query)
            stock_prices = self.database_manager.find_all(
                collection_name,
                self.get_single_day_query
            )
            stock_prices = pd.DataFrame(stock_prices)
            return stock_prices
        
    def convert_all(self):
        available_collections = self.database_manager.get_all_collections(name=True)
        for collection in available_collections:
            if collection in self.last_timestamp_dict.keys():
                stock_prices = self.convert(collection_name=collection)
                return stock_prices
            
        
        
        

# if __name__ == "__main__":
#     converter = MongoToParquet()
#     converter.convert()
        
        
    
    
        