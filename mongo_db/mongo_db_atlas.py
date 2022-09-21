import ssl
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

    def get_databse_client_object(self):
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
        
        """

        