from email import header
import random
import sys

import os
import argparse
from xxlimited import Str

from pyspark.sql.types import IntegerType, StringType, FloatType
from regex import E
from sklearn.metrics import r2_score, mean_squared_error

from utility import create_directory_path, read_params, get_logger_object_of_training
import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor 
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

from adult_exception.adult_exception import AdultException as GenericException 

from streaming.spark_manager.spark_manager import SparkManager

log_collection_name="training_model"

class DataPreProcessing:

    def __init__(self,logger, is_log_enable=True, data_frame=None, pipeline_path=None):
        try:
            self.logger=logger,
            self.logger.is_log_enable=is_log_enable
            self.data_frame=data_frame
            self.stages=[]
            self.pipeline_path=pipeline_path

        except Exception as e:
            generic_exception=GenericException(
                "Error occured in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__, DataPreProcessing.__name__, 
                self.__init__.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e

    def update_dataframe_schema(self, schema_defination:dict):

        try:
            print(self.data_frame.printSchema())
            if self.data_frame is None:
                raise Exception("Update the attribute dataframe.")
            for column, datatype in schema_defination.items():
                self.logger.log(f"Updated datatype of column : {column} to {str(datatype)}")
                self.data_frame=self.data_frame.withColumn(column, self.data_frame[column].cast(datatype))

        except Exception as e:
            generic_expection = GenericException(
                "Error occured in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__, DataPreProcessing.__name__, 
                self.update_dataframe_schema.__name__))
            raise Exception(generic_expection.error_message_detail(str(e),sys)) from e     

    def encode_categorical_column(self, input_columns:list):
        try:
            string_indexer=StringIndexer(inputCols=input_columns,
                                        outputCols=[f"{column}_encoder" for column in input_columns])
            self.stages.append(string_indexer)
            one_hot_encoder = OneHotEncoder(inputCols=string_indexer.getOutputCols(),
                                            outputCols=[f"{column}_encoder" for column in input_columns])
            self.stages.append(one_hot_encoder)

        except Exception as e:
            generic_exception=GenericException(
                "Error ocurred in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__, DataPreProcessing.__name__,
                        self.encode_categorical_column.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e 

    def create_input_features(self, required_column:list):
        "
        
        "
        try:
            vector_assembler = VectorAssembler(inputCols=required_column, outputCol="input_features")
            self.stages.append(vector_assembler)
        except Exception as e:
            generic_exception=GenericException(
                "Error ocurred in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__, DataPreProcessing.__name__,
                        self.create_input_features.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e 

    def get_prepared_dataset(self):
        try:
            schema_defination={"age": IntegerType(),
                                "workclass": StringType(),
                                "fnlwgt": IntegerType(),
                                "education": StringType(),
                                "education-num" : IntegerType(),
                                "marital-status": StringType(),
                                "occupation": StringType(),
                                "relationship": StringType(),
                                "race": StringType(),
                                "sex": StringType(),
                                "capital-gain": IntegerType(),
                                "capital-loss": IntegerType(),
                                "hours-per-week": IntegerType(),
                                "country": StringType(),
                                "salary": StringType()}

            self.update_dataframe_schema(schema_defination=schema_defination)
            self.encode_categorical_column(input_column=['workclass','marital-status','occupation',
                                            'relationship','race','sex','country','salary'])
            required_column=['age','workclass','fnlwgt','education-num','marital-status','occupation',
                            'relationship','race','sex','capital-gain','capital-loss','hours-per-week','country'] # rechek it is wrong for time being
            self.create_input_features(required_column=required_column)
            pipeline = Pipeline(stages=self.stages)
            pipeline_fitted_obj = pipeline.fit(self.data_frame)
            self.data_frame=pipeline_fitted_obj.transform(self.data_frame)
            # os.remove(path=self.pipline_path)
            create_directory_path(self.pipeline_path, is_recreate=True)
            
            



class ModelTrainer:

    def __init__(self, config, logger, is_log_enable):
        try:
            self.logger = logger
            self.logger.is_log_enable= is_log_enable
            self.config= config
            self.training_file_path=self.config['artifacts']['training_data']['training_file_from_db']
            self.master_csv=self.config['artifacts']['training_data']['master_csv']
            self.target_column=self.config['target_columns']['columns']
            self.test_size=self.config['base']['random_state']
            self.random_state=self.config['base']['random_state']
            self.plot=self.config['artifacts']['training_data']['plots']
            self.pipeline_path=self.config['artifacts']['training_data']['pipeline_path']
            self.model_path=self.config['artifacts']['model']['model_path']
            self.null_value_file_path=self.config['artifacts']['training_data']['null_value_info_file_path']
            
            self.spark=SparkManager().get_spark_session_object()

        except Exception as e:
            generic_exception=GenericException(
                "Error occured in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__,ModelTrainer.__name__, self.__init__.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e


    def get_dataframe(self):
        try:
            master_file_path=os.path.join(self.training_file_path,self.master_csv)
            
            return self.spark.read.csv(master_file_path,header=True, inferSchema=True)
        
        except Exception as e:
            generic_exception=GenericException(
                "Error occured in module [{0}] class [{1}] method [{2}]"
                .format(self.__module__, ModelTrainer.__name__,
                self.get_dataframe.__name__))
            raise Exception(generic_exception.error_message_detail(str(e),sys)) from e

    def data_preparation(self):
        try:
            data_frame=self.get_dataframe()
            preprocessing=DataPreProcessing(logger=self.logger,
                                            is_log_enable=self.logger.is_log_enable,
                                            data_frame=data_frame,
                                            pipeline_path=self.pipeline_path)


    def begin_training(self):
        try: 
            train_df, test_df= self.data_preparation()


def train_main(config_path:str, datasource: str, is_logging_enable=True, execution_id=None, executed_by=None):
    try:
        logger=get_logger_object_of_training(config_path=config_path, collection_name=log_collection_name,
                                            executed_by= execution_id, executed_by=executed_by)
        logger.is_log_enable=is_logging_enable
        logger.log("Training begin")
        config=read_params(config_path)
        model_trainer=ModelTrainer(config=config ,logger=logger, is_log_enable=is_logging_enable)
        model_trainer.begin_training()




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