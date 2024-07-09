import constants as const
class Indicators:
    def __init__(self):
        pass
    def generate_query(self):
        pass
    def execute(self):
        pass


class BollingerBands:

    def __init__(self,database,window_size,standard_difference) -> None:
        self.database = database
        self.window_size = (window_size - 1) * -1
        self.standard_difference = standard_difference

    def generate_query(self,collection_name,date):
        query = \
        [
            {
                '$match': {
                    'Timestamp': {
                        '$gt': date
                    }
                }
            },
            {
                '$sort': {
                    'Timestamp': 1
                }
            }, {
                '$setWindowFields': {
                    'sortBy': {
                        'Timestamp': 1
                    }, 
                    'output': {
                        'sma': {
                            '$avg': '$Close', 
                            'window': {
                                'documents': [
                                    self.window_size, 0
                                ]
                            }
                        }, 
                        'stdDev': {
                            '$stdDevPop': '$Close', 
                            'window': {
                                'documents': [
                                    self.window_size, 0
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'upperBand': {
                        '$add': [
                            '$sma', {
                                '$multiply': [
                                    '$stdDev', self.standard_difference
                                ]
                            }
                        ]
                    }, 
                    'lowerBand': {
                        '$subtract': [
                            '$sma', {
                                '$multiply': [
                                    '$stdDev', self.standard_difference
                                ]
                            }
                        ]
                    }
                }
            }, {
                '$merge': {
                    'into': collection_name, 
                    'whenMatched': [
                        {'$addFields': {
                            f'bollingerBands_{self.window_size}_{self.standard_difference}': {
                                'upperBand': '$upperBand', 
                                'lowerBand': '$lowerBand'
                            }
                        }}
                    ], 
                    'whenNotMatched': 'discard'
                }
            }
        ]
        
        return query
    
    def execute(self,collection_name):

        date = self.database[collection_name].find_one({
            f"boollingerBands_{self.window_size}_{self.standard_difference}": {"$exists": True}
        }, sort=[('Timestamp', -1)])['Timestamp']
        const.LOGGER.info(f"{collection_name} : BOLLINGER BANDS ({self.window_size},{self.standard_difference}) : Obtained Last Date : {date}")
        query = self.generate_query(collection_name,date)
        self.database[collection_name].aggregate(query)
        const.LOGGER.info(f"{collection_name} : BOLLINGER BANDS ({self.window_size},{self.standard_difference}) : Updated Database.")
        

