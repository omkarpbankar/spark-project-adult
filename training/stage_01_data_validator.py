import os
from pkgutil import get_data
import re
import sys

import pandas as pd
from utility import get_time, read_params, create_directory_path, values_from_schema_function
from utility import get_logger_object_of_training, get_date, get_time

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


    def value_from_schema(self):
        """
        return tuple (sample_file_name, column_name,number_of_column)
        """
        try:
            return values_from_schema_function(self.training_schema_file)
        except Exception as e:
            generic_exception=GenericException(
                "Error occured in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__,DataValidator.__name__,
                        self.value_from_schema.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e

    def create_good_bad_archive_bad_file_path(self):
        try:
            create_directory_path(self.good_file_path)
            create_directory_path(self.bad_file_path)
            create_directory_path(self.archive_bad_file_path, is_recreated=False)
        except Exception as e:
            generic_exception= GenericException(
                "Error occured in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__, DataValidator.__name__,
                self.create_good_bad_archive_bad_file_path.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e

    def file_name_regular_expression(self):
        
        return "['HealthPrem']+['\_'']+[\d_]+[\d]+\.csv"

    def validate_file_name(self):
        try:
            self.create_good_bad_archive_bad_file_path()
            pattern, length_of_date_stamp_in_file, length_of_time_stamp_in_file, column_names, number_of_columns = \
                self.value_from_schema
            file_name_reg_pattern=self.file_name_regular_expresion()
            self.logger.log("Validating file names.")
            files=os.listdir(self.file_path)
            for file in files:
                file_path=os.path.join(self.file_path,file)
                split_at_dot=re.split('.csv',file)
                split_at_dot=(re.split('_',split_at_dot[0]))
                if re.match(file_name_reg_pattern,file) and len(split_at_dot[1]) == length_of_date_stamp_in_file \
                        and len(split_at_dot[2]) == length_of_time_stamp_in_file:
                    destination_file_path=os.path.join(self.good_file_path,file)
                    self.logger.log(f"file name : {file} matched hence moving file to good file path{destination_file_path}")
                    shutil.move(file_path,destination_file_path)
                else:
                    destination_file_path=os.path.join(self.bad_file_path,file)
                    self.logger.log(f"file name : {file} does not matched hence moving file to bad file path{destination_file_path}")
                    shutil.move(file_path,destination_file_path)
        except Exception as e:                
            generic_exception = GenericException(
                "Error occured in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__, DataValidator.__name__,
                        self.validate_file_name.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e


    def validate_no_of_column(self, no_of_columns):
        """
        Description:
        if number of columns matches then file will be move to good file path else it will be move to bad file path.
        ============================================================================================================
        param no_of_columns: int number of column must be present in each file. 
        return: nothing

        """
        try:
            self.logger.log("Validating number of columns in input file")
            files=os.listdir(self.good_file_path)
            for file in files:
                file_path=os.path.join(self.good_file_path,file)
                df=pd.read_csv(file_path)
                if df.shape[1] != no_of_columns:
                    destination_file_path=os.path.join(self.bad_file_path,file)
                    self.logger.log(f"file: {file} has incorrect number of columns hence moving file to bad file path {destination_file_path}")
                    shutil.move(file_path,destination_file_path)
        except Exception as e:
            generic_exception=GenericException(
                "Error occured in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__,DataValidator.__name__,
                self.validate_no_of_column.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e    

    def validate_missing_values_in_whole_column(self):
        try:
            self.logger.log("Missing Values Validation Started!")
            for file in os.listdir(self.good_file_path):
                csv=pd.read_csv(os.path.join(self.good_file_path,file))
                count=0
                for columns in csv:
                    if (len(csv[columns])-csv[columns].count())==len(csv[columns]):
                        count+=1
                        shutil.move(os.path.join(self.good_file_path,file),self.bad_file_path)
                        self.logger.log(
                            f"Invalid column length for the file!! File move to bad folder :: {file}")
                        break
                if count==0:
                    print(csv.columns)
                    csv.rename(columns={"Unnamed: 0":"Premium"}, inplace=True)
                    csv.to_csv(os.path.join(self.good_file_path,file), index=None, header=True)
        except Exception as e:
            generic_exception= GenericException(
                "Error occured in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__,DataValidator.__name__,
                    self.validate_missing_values_in_whole_column.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e  

    def archive_bad_files(self):
        try:
            folder_name= f"bad_files_{get_date().replace('-','_')}_{get_time().replace(':','_')}"
            archive_directory_path= os.path.join(self.archive_bad_file_path,folder_name)
            create_directory_path(archive_directory_path)
            for file in os.listdir(self.bad_file_path):
                source_file_path=os.path.join(self.bad_file_path,file)
                shutil.move(source_file_path,archive_directory_path)

        except Exception as e:
            generic_exception=GenericException(
                "Error occured in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__,DataValidator.__name__,
                self.archive_bad_files.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e      





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
        data_validator.validate_file_name()
        data_validator.validate_no_of_column(no_of_columns=number_of_columns)
        data_validator.validate_missing_values_in_whole_column()
        data_validator.archive_bad_files()
    except Exception as e:




if __name__== "__main__":
    args=argparse.ArgumentParser()
    args.add_argument("--config","-c", default=os.path.join("config","params.yaml"))
    args.add_argument("--datasource","-d", default=None)
    parsed_args= args.parse_args()
    print("Started")
    validation_main(config_path=parsed_args.config, datasource= parsed_args.datasource)


