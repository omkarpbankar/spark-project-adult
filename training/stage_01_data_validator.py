import os
from pkgutil import get_data
import re
import sys

import pandas as pd
from utility import get_time, read_params, create_directory_path, values_from_schema_function
from utility import get_logger_object_of_training, get_data, get_time

from adult_exception.adult_exception import AdultException as GenericException
import argparse
import shutil

log_collection_name="data_validation"

class DataValidator:
    def __int__(self, config, logger, is_logging_enable=True):
        try:
            self.config= config
            self.logger=logger
            self.logger.is_log_enable=is_logging_enable
            self.file_path = self.config['data_source']['Training_Batch_Files']
            self.good_file_path=self.config['artifacts']['training_data']['good_file_path']
            self.bad_file_path=self.config['artifacts']['training_data']['bad_file_path']
            self.archive_bad_file_path=self.config['artifacts']['training_data']['archive_bad_file_path']
            self.training_schema_file=self.config['config']['schema_training']

        except Exception as e:
            generic_exception=GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__, DataValidator.__name__,
                self.__init__.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e


    def value_from_schema():
        """
        return tuple (sample_file_name, column_name,number_of_column)
        """
        try:
            return values_from_schema_function(self.training_schema_file)
        except Exception as e:


def validation_main(config_path: str, datasource:str, is_logging_enable=True, execution_id=None,
                    executed_by=None)-> None:
    
    try:
        logger=get_logger_object_of_training(config_path=config_path, collection_name=log_collection_name,
                                            execution_id=execution_id,executed_by=executed_by)
        logger.is_log_enabled=is_logging_enable
        config=read_params(config_path)
        logger.log("data validation started.")
        data_validator = DataValidator(config=config, logger=logger, is_logging_enable=is_logging_enable)
        pattern, length_of_date_stamp_in_file, length_of_time_stamp_in_file, column_names, number_of_columns=\
            data_validator.value_from_schema()
    except Exception as e:




if __name__=="__main__":
    args=argparse.ArgumentParser()
    args.add_argument("--config","-c", default=os.path.join("config","params.yaml"))
    args.add_argument("--datasource","-d", default=None)
    parsed_args= args.parse_args()
    print("Started")
    validation_main(config_path=parsed_args.config, datasource= parsed_args.datasource)


