import ssl
from tkinter import E
from typing import Collection
import pymongo
import json
import pandas as pd
import sys
from adult_exception.adult_exception import AdultException as MongoDBException

class MongoDBOperation:
    def __init__(self, user_name=None,password=None):
        try:
                # Creating initial object to fetch mongoDB credentials
            if user_name is None or password is None:
                credentials = {
                    'user_name': 'pythonbyomkar',
                    'password': "abraka_dabra"
                }
                self.__user_name = credentials['user_name']
                self.__password = credentials['password']
            else:
                self.__user_name = user_name
                self.__password = password

        except Exception as e:
            mongo_db_exception = MongoDBException(
                "Failed to instantiate mongo_db_object in module [{0}] class [{1}] method [{2}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__, "__init__"))
            
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e

    
    def get_mongo_db_url(self):
        """
        return: mongo_db_url
        """
        try:
            url=""
            return url

        except Exception as e:
            mongo_db_exception=MongoDBException(
                "Failed to fetch mongo_db url in moduel [{0}] class [{1}] method [{2}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                        self.get_mongo_db_url.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 

    def get_database_client_object(self):
        """
        Return pymongoClient object to perform action with MongoDB.
        """
        try:
            url="{0}{1}".format(self.__user_name,self.__password)
            client = pymongo.MongoClient(url,ssl_cert_reqs=ssl.CERT_NONE) # creating database client object.
            return client
        except Exception as e:
            mongo_db_exception=MongoDBException(
                "Failed to fetch data base client object in module [{0}] class [{1}] method [{2}]"
                .format(MongoDBOperation.__module__.__str__(),MongoDBOperation.__name__,
                self.get_databse_client_object.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e

    def close_database_client_object(self, obj_name):
        """
        param obj_name: pymongo client object name.
    
        """
        try:
            obj_name.close()
            return True
        
        except Exception as e:
            mongo_db_exception = MongoDBException(
                "Failed to close data base client object in module [{0}] class [{}] method [{}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.close_database_client_object.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 

    def is_database_present(self, client, db_name):
        """
        param client :  pymongo client 
        param db_name : Database Name
        
        """
        try: 
            if db_name in client.list_database_name():
                return True
            else:
                return False
        except Exception as e:
            mongo_db_exception = MongoDBException(
                "Faild while checking the database in moduel [{0}] class [{1}] method [{2}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.is_database_present.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 

    def create_database(self, client, db_name):
        """
        param client : Object of database
        param db_name : Database Name
        """
        try:
            return client[db_name]

        except Exception as e:
            mongo_db_exception = MongoDBException (
                "Failed to create databse in module [{0}] class [{}] method [{2}]"
                .format(MongoDBOperation.__moduel__.__str__(), MongoDBOperation.__name__,
                self.create_database.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e

    def create_collection_in_database(self,databse,collection_name):
        """
        param databse : database
        param collection_name : collection name
        
        """
        try: 
            return databse[collection_name]
        except Exception as e:
            mongo_db_exception = MongoDBException(
                "Failed while creating collection in database module [{}] class [{}] method [{}]"
                .format(MongoDBOperation.__module__.__str__(),MongoDBOperation.__name__,
                self.create_collection_in_database.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e

    def is_collection_is_present(self, collection_name, database):    
        """ 
        param collection_name: Collection Name 
        param databse : Database
        """
        try:
            collection_list= database.list_collection_name()

            if collection_name in collection_list:
                return True
            else:
                return False

        except Exception as e:
            mongo_db_exception=MongoDBException(
                "Failed while checking collection in databse in moduel [{}] class [{}] method [{}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.is_collection_is_present.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e

    def get_collection(self, collection_name, database):
        """
        param collection_name : collection name
        param databse : database
        """
        try:
            collection = self.create_collection_in_databse(database, collection_name)
            return collection 
        except Exception as e:
            mongo_db_exception=MongoDBException(
                "Failed while creating collection in database in moduel [{}] class [{}] method[{}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.get_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e

    def is_record_present(self,db_name, collection_name, record):
        """
        param collection_name: Collection Name 
        param databse: Database Name
        """
        try:
            client = self.get_database_client_object() # client object 
            database = self.create_database(client, db_name) # database object
            collection = self.get_collection(collection_name, database)
            record_found = collection.find(record)
            if record_found.count() > 0 :
                client.close()
                return True
            else: 
                client.close()
                return False
        except Exception as e:
            mongo_db_exception = MongoDBException(
                "Failed while checking the record in module [{0}] class [{}] method [{}]"
                .format(MongoDBOperation.__module__.__str__(),MongoDBOperation.__name__,
                self.is_record_present.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e

    def create_record(self, collection, data):
        """ 
        param collection: collection
        param data : data to be inserted.
        """
        try:
            collection.insert_one(data) # insertion of record in collection.

        except Exception as e:
            mongo_db_exception= MongoDBException(
                "Failed while inserting the record in module [{}] class [{}] method[{}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.create_record.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e

    def create_records(self, collection, data):
        """ 
        param collection: collection
        param data: many records 
        """
        try:
            collection.insert_many(data) # insertion of many records in collection.

        except Exception as e:
            mongo_db_exception=MongoDBException(
                "Failed whiel inserting multiple recoreds in module [{}] class [{}] method [{}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.create_records.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e
    
    def insert_record_in_collection(self, db_name, collection_name, record):
        """ 
        param db_name: Database Name
        param colletion_name: collection name
        param records : Records 
        """
        try:
            no_of_row_inserted=0
            client = self.get_database_client_object()
            database = self.create_database(client, db_name)
            collection = self.get_collection(collection_name, database)
            if not self.is_record_present(db_name, collection_name, record):
                no_of_row_inserted=self.create_records(colletion=collection, data=record)
            client.close()
            return no_of_row_inserted
        except Exception as e:
            mongo_db_exception=MongoDBException(
                "Failed to insert the records in collection in module [{0}] class [{}] method [{}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.insert_record_in_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 

    def drop_collection(self, db_name, collection_name):
        """ 
        param db_name: database name 
        param collection_name: colletion name
        
        """
        try:
            client = self.get_database_client_object()
            database = self.create_database(client, db_name)
            if self.is_collection_is_present(collection_name, database):
                collection_name = self.get_collection(collection_name, database)
                collection_name.drop()
            return True
        except Exception as e:
            mongo_db_exception= MongoDBException(
                "Failed to drop collection in module [{}] class [{}] method [{}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.drop_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 

    def insert_records_in_collection(self, db_name, collection_name, records):
        """ 
        param db_name : Database Name
        param collection_name : Collection Name 
        param records : Records 
        """
        try:
            no_of_row_inserted = 0
            client = self.get_database_client_object()
            database = self.create_database(client, db_name)
            collection = self.get_collection(collection_name,database)
            for record in records:
                if not self.is_record_present(db_name, collection_name, record):
                    no_of_row_inserted+= self.create_record(collection=collection, data=record)
            client.close()
            return no_of_row_inserted
        except Exception as e:
            mongo_db_exception=MongoDBException(
                "Failed while inserting the records in collection in module [{0}] class [{1}] method [{2}]"
                .format(MongoDBOperation.__module__.__str__(),MongoDBOperation.__name__,
                self.insert_records_in_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 

    def insert_dataframe_into_collection(self, db_name, collection_name, data_frame):
        """ 
        param db_name : Database Name
        param collection_name: Collection Name
        param data_fram : Data Frame 
        """
        try:
            data_frame.reset_index(drop=True,inplace=True)
            records=list(json.loads(data_frame.T.to_json()).values())
            client = self.get_database_client_object()
            database = self.create_database(client,db_name)
            collection = self.get_collection(collection_name, database)
            collection.inset_many(records)
            return len(records)
        except Exception as e:
            mongo_db_exception=MongoDBException(
                "Failed to inset DataFrame in collection in module [{}] class [{}] method [{}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.inset_datafram_into_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 

    def get_record(self, database_name, collection_name,query=None):
        """
        param database_name: Database Name
        param collection_name: Collection Name
        param query = Query to search the record
        """
        try:
            client = self.get_database_client_object()
            database = self.create_database(client, database_name)
            collection = self.get_collection(collection_name=collection_name,database=database)
            record = collection.find_one(query)
            return record 
        except Exception as e:
            mongo_db_exception=MongoDBException(
                "Failed in retriving record in collection moduel [{0}] class [{}] method [{}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.get_record.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 
    
    def get_min_value_of_column(self, database_name, collection_name, query, column):
        """
        param database_name : Database Name
        param collection_name : collection Name
        param query : Query to find the record 
        param column: Column name
        """
        try:
            client=self.get_database_client_object()
            database = self.create_database(client,database_name)
            collection= self.get_collection(collection_name, database)
            min_value= collection.find(query).sort(column, pymongo.ASCENDING).limit(1)
            value=[min_val for min_val in min_value]
            if len(value) > 0:
                if column in value[0]:
                    return value[0][column]
                else:
                    return None
            else:
                return None

        except Exception as e:
            mongo_db_exception=MongoDBException(
                "Failed while receiving min value from in collection module [{}] class [{}] method [{}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.get_min_value_of_column.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 

    def get_max_value_of_column(self, database_name, collection_name, query, column):
        """
        param database_name : Database Name
        param collection_name : Collection Name
        param query : Query to get the records
        param column : Column  
        """
        try:
            client = self.get_database_client_object()
            database= self.create_database(client, database_name)
            collection = self.get_collection(collection_name, database)
            max_value = collection.find(query).sort(column,pymongo.DESCENDING).limit(1)
            value = [max_val for max_val in max_value]
            if len(value)>0:
                if column in value[0]:
                    return value[0][column]
                else:
                    return None
            else:
                return None
        except Exception as e:
            mongo_db_exception= MongoDBException(
                "Failed while receiving the Max value in module [{0}] class [{1}] method [{2}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.get_max_value_of_column.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 

    def get_records(self, database_name, collection_name, query=None):
        """
        param database_name : Database Name
        param collection_name : Collection Name
        parm query : Query to get the records.
        """
        try:
            client = self.get_database_client_object()
            database = self.create_database(client, database_name)
            collection = self.get_collection(collection_name=collection_name, database=database)
            records = collection.find(query)
            return records 
        except Exception as e:
            mongo_db_exception= MongoDBException(
                "Failed while receiving the records in module [{0}] class [{1}] method [{2}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.get_records.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 

    def update_record_in_collection(self, database_name, collection_name, query, new_value):
        """ 
        param database_name : Database Name
        param collection_name : collection Name 
        param query : Query to find the records
        param new_value : new value with which record need to be updated.
        """
        try:
            client=self.get_database_client_object()
            database = self.create_database(client, database_name)
            collection = self.get_collection(collection_name= collection_name, database=database)
            update_query = {'$set': new_value}
            result = collection.update_one(query, update_query)
            client.close()
            return result.raw_result["nModified"]
        except Exception as e:
            mongo_db_exception= MongoDBException(
                "Failed while updating the record in module [{0}] class [{1}] method [{2}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.update_record_in_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 

    def get_dataframe_of_collection(self, db_name, collection_name, query=None):
        """
        param db_name : database name
        param collection_name : collection name
        param query: query to get the records 
        """
        try:
            client = self.get_database_client_object()
            database = self.create_database(client,db_name)
            collection = self.get_collection(collection_name=collection_name, database=database)
            if query is None:
                query={}
            df = pd.DataFrame(list(collection.find(query)))
            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"], axis=1)
            return df.copy()
        except Exception as e:
            mongo_db_exception= MongoDBException(
                "Failed while updating the record in module [{0}] class [{1}] method [{2}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.get_dataframe_of_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e 
    
    def remove_record(self,db_name,collection_name, query):
        """
        param db_name : database name
        param collection_name : collection name
        param query: query to get the records         
        """
        try:
            client = self.get_database_client_object()
            database = self.create_database(client,db_name)
            collection = self.get_collection(collection_name=collection_name, database=database)
            collection.delete_one(query)
            return True        
        except Exception as e:
            mongo_db_exception= MongoDBException(
                "Failed while updating the record in module [{0}] class [{1}] method [{2}]"
                .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                self.remove_record.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e),sys)) from e






            


        




        
    

        