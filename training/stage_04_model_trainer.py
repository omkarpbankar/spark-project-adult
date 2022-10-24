import random
import sys

import os
import argparse

from pyspark.sql.types import IntegerType, StringType, FloatType
from sklearn.metrics import r2_score, mean_squared_error

from utility import create_directory_path, read_params, get_logger_object_of_training
import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor 
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

from adult_exception.adult_exception import AdultException as GenericException 

from streaming.spark_manager.spark_manager import SparkManager

log_collection_name="training_model"


def train_main(config_path:str, datasource: str, is_logging_enable=True, execution_id=None, executed_by=None):
    try:
        logger=get_logger_object_of_training(config_path=config_path, collection_name=log_collection_name,
                                            executed_by= execution_id, executed_by=executed_by)
        logger.is_log_enable=is_logging_enable
        logger.log("Training begin")
        config=read_params(config_path)
        model_trainer=



    except Exception as e:
        print(e)


if __name__=="__main__":
    args=argparse.ArgumentParser()
    args.add_argument("--config",'-c',default=os.path.join('config','params.yaml'))
    args.add_argument('--datasource','-d', default=None)
    parsed_args=args.parse_args()
    print(parsed_args.config)
    print(parsed_args.datasource)
    train_main(config_path=parsed_args.config, datasource=parsed_args.datasource)