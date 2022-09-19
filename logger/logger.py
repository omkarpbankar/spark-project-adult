import os
from datetime import datetime
import uuid
import sys
from salary_exception.salary_exception import SalaryExceptoin as AppLoggerException
from mongo_db.mongo_db_atlas import MongoDBOperation

class AppLogger:
    def _init__(self,project_id,log_databse,log_collection_name, executed_by,
                execution_id, is_log_enabled=True):

        try:
            self.project_id=project_id
            self.log_databse=log_databse
            self.log_collection_name=log_collection_name
            self.executed_by=executed_by
            self.execution_id=execution_id
            self.is_log_enabled=is_log_enabled
            self.mongo_db_object=MongoDBOperation()

        except Exception as e:
            app_logger_exception= AppLoggerException(
                                    "Error ocurred in module [{0}] class [{1}] methond [{2}]"
                                    .format(MongoDBOperation.__module__.__str__(),MongoDBOperation.__name__,
                                    "__init__"))
            raise Exception(app_logger_exception.error_message_detail(str(e),sys)) from e

    def log(self,log_message):
        try:
            if not self.is_log_enabled:
                return 0
            log_data = {
                'execution_id': self.execution_id,
                'message': log_message,
                'executed_by': self.executed_by,
                'project_id': self.project_id,
                'updated_date_time': datetime.now().strftime('%H:%M:%S')
            }
            
            self.mongo_db_object.insert_record_in_collection(
                self.log_databse, self.log_collection_name,log_data)
        
        except Exception as e:
            app_logger_exception= AppLoggerException(
                "Error occured in module [{0}] class [{1}] methond [{2}]"
                .format(MongoDBOperation.__module__.__str__(),MongoDBOperation.__name__,
                        self.log.__name__))
            raise Exception(app_logger_exception.error_message_detail(str(e),sys)) from e
        
    





