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


        
