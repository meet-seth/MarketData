import pymongo
import constants as const

class DatabaseManager:
    
    def __init__(self):
        """
        Database Manager that is responsible for connecting to
        the MongoDB database, creates a database from constans file
        and has functions to perfrom CRUD operations on the created
        database.
        
        Methods:

            get_last_timestamp: Retrives last recorded timestamp from each ticker data
            insert_one: Insert one data document into specified collection in database
            insert_many: Insert multiple data documents into specified collection in database
            find_one: Find one data document that matches some criteria from a specified collection
            find_all: Find all the data documents that matches some criteria from a specified collection
            update_one: Updates one data document that matches the criteria with input data document in a specified collection
            update_many: Updates all data documents that match the criteria with input data document in a specified collection
            delete_one: Deletes one data document from a specified collection that matches some criteria
            delete_many: Deletes all the data documents from a specified collection that matches some criteria.
        
        
        """
        self.client = pymongo.MongoClient()
        self.database = self.client[const.CONSTANT_DATABASE_NAME]
        
    
    def get_last_timestamp(self):
        
        """
        Retrieves the last recorded timestamp for each ticker
        and returns a dictionary in the form {ticker: timestamp}

        Returns:
            dict: Dictionary of the form {ticker: last_recorded_timestamp}
        """
        
        last_recorded_timestamp = dict()
        
        for i in self.database[const.CONSTANT_TIMESTAMP_COLLECTION].find():
            last_recorded_timestamp[i['ticker']] = i['last_recorded_timestamp']
            
        return last_recorded_timestamp
        
    def insert_one(self,collection_name,data_to_insert):
        """
        Insert one document into a specified collection_name

        Args:
            collection_name (string): Name of collection
            data_to_insert (dict): Data to insert into the collection 
        """
        self.database[collection_name].insert_one(data_to_insert)
        
    
    def insert_many(self,collection_name,data_to_insert):
        """
        Insert multiple documents into a specific collection_name

        Args:
            collection_name (string): Name of collection
            data_to_insert (list of dictionaries): List of dictionaries of data to insert
        """
        self.database[collection_name].insert_many(data_to_insert)
        
    def find_one(self,collection_name,criteria):
        """
        Find one occurrence of data that matches the criteria
        from collection_name

        Args:
            collection_name (string): Name of collection
            criteria (dict): Dictionary as per monogodb standards

        Returns:
            mongodb cursor: Result of find_one operation
        """
        result = self.database[collection_name].find_one(criteria)
        return result
        
    def find_all(self,collection_name,criteria):
        """
        Find all the occurrences of data that match the criteria 
        from collection_name

        Args:
            collection_name (string): Name of collection
            criteria (dict): Dictionary as per mongodb standards
            
        Returns:
            monogdb cursor: Result of find operation 
        """
        result = self.database[collection_name].find(criteria)
        return result
    
    def update_one(self,collection_name,criteria,data_to_replace):
        """
        Updates the first occurrence of data found in collection_name 
        based on criteria with data_to_replace.

        Args:
            collection_name (string): Name of collection
            criteria (dict): Dict as per mongodb standards to find the data
            data_to_replace (dict): Data to replace the matched data with.
        """
        self.database[collection_name].update_one(criteria,data_to_replace)
        
    def update_many(self,collection_name,criteria,data_to_replace):
        """
        Updates all the occurrences of data found in collection_name
        based on criteria with data_to_replace.

        Args:
            collection_name (string): Name of collection
            criteria (dict): Dictionary as per mongodb standards to find the data
            data_to_replace (dict): Data to replace the matched data with.
        """
        self.database[collection_name].update_many(criteria,data_to_replace)
        
    def delete_one(self,collection_name,criteria):
        """
        Delete first occurrance of data that matches the criteria.

        Args:
            collection_name (string): Name of collection
            criteria (dict): Dictionary as per mongodb standards
        """
        self.database[collection_name].delete_one(criteria)
        
    def delete_many(self,collection_name,criteria):
        """
        Delete all the occurrances of data that matches the criteria.

        Args:
            collection_name (string): Name of the collection
            criteria (dict): Dictionary as per mongodb standards
        """
        self.database[collection_name].delete_many(criteria)