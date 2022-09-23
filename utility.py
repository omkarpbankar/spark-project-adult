import json 
from datetime import datetime
from tkinter import E
import yaml
import uuid

import os
import shutil
from logger.logger import AppLogger

def get_time()->str:
    """
    Return the current time in Hours, Minutes and Seconds.

    """
    return datetime.now().strftime("%H:%M:%S").__str__()

def get_date()->str:
    """
    Return the current date.

    """
    return datetime.now().date().__str__()


def read_params(config_path:str)->dict:
    """
    param config_path: Path of config.yaml file 
    return: dictionary of config parameters.

    """
    with open(config_path) as yaml_file:
        config=yaml.safe_load(yaml_file)
    return config


def create_directory_path(path:str,is_recreated=True)-> bool:
    """
    param path: 
    param is_recreated: Default it will delete the existing directory yet you can pass
                        it's value to 'False' if you do not want to remove existing directory.
    return: 'True' or 'False'

    """
    try:
        if is_recreated:
            if os.path.exists(path):
                shutil.rmtree(path,ignore_errors=False) # Remove existing directory if is_recreated is True.
        os.makedirs(path,exist_ok=True)                 # If directory exists it will not alter directory.
        return True 
    except Exception as e:
        raise e 

def clean_data_source_dir(path, logger=None,is_logging_enable=True):
    try:
        if not os.path.exists(path):
            os.mkdir(path)
        for file in os.listdir(path):
            if '.gitignore' in file:
                pass
            logger.log(f"{os.path.join(path,file)} file will be deleted.")
            os.remove(os.path.join(path,file))
            logger.log(f"{os.path.join(path,file)} file has been deleted")
    except Exception as e:
        raise E

def get_logger_object_of_training(config_path:str, collection_name:str,execution_id=None, 
                                    executed_by= None)->AppLogger:
    config=read_params(config_path)
    database_name=config['log_database']['training_database_name']
    if execution_id is None:
        execution_id=str(uuid.uuid4())
    if executed_by is None:
        executed_by="Omkar Bankar"
    logger=AppLogger(project_id=2,log_databse=database_name, log_collection_name=collection_name,
                    execution_id=execution_id, executed_by=executed_by)
    return logger



def get_logger_object_of_prediction(config_path:str, collection_name:str, execution_id=None,
                                    executed_by=None)->AppLogger:
    config=read_params(config_path)
    database_name=config['log_database']['prediction_database_name']
    if execution_id is None:
        execution_id=str(uuid.uuid4())
    if executed_by is None:
        executed_by="Omkar Bankar"
    logger=AppLogger(project_id=2,log_databse=database_name, log_collection_name=collection_name,
                    execution_id=execution_id, executed_by=executed_by)
    return logger

def values_from_schema_function(schema_path):
    try:
        with open(schema_path,'r') as r:
            dic= json.load(r)
        
        pattern=dic['SampleFileName']
        length_of_date_stamp_in_file = dic["LenghthOfDateStampInFile"]
        length_of_time_stamp_in_file = dic["LengthOfTimeStampInFile"]
        column_name = dic["ColName"]
        number_of_columns= dic["NumberOfColumns"]
        
        return pattern, length_of_date_stamp_in_file, length_of_time_stamp_in_file, column_name, number_of_columns
    except ValueError:
        raise ValueError
    
    except KeyError:
        raise KeyError

    except Exception as e:
        raise e





        
