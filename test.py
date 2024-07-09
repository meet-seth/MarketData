import json
from pymongo import MongoClient
from datetime import datetime
import pandas as pd

client = MongoClient()

database = client['market_data']
collection = "INFY.NS"

dt = None
win = None
std = None

def generate_query(collection_name,dt,win,std):
    win = (win - 1) * -1
    query  = \
    [
        {
            '$match': {
                'Timestamp': {
                    '$gt': dt
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
                                win, 0
                            ]
                        }
                    }, 
                    'stdDev': {
                        '$stdDevPop': '$Close', 
                        'window': {
                            'documents': [
                                win, 0
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
                                '$stdDev', std
                            ]
                        }
                    ]
                }, 
                'lowerBand': {
                    '$subtract': [
                        '$sma', {
                            '$multiply': [
                                '$stdDev', std
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
                        'bollingerBands': {
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


dt = datetime(2024, 7, 1)
win = 20
std = 2

query = generate_query(collection,dt,win,std)

database[collection].aggregate(query)