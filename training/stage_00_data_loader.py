import os 
import sys
import shutil
from utility import read_params
import argparse
from utility import get_logger_object_of_training 
from utility import clean_data_source_dir
from adult_exception.adult_exception import AdultException as GenericException

log_collection_name="data_loager"

def loader_main(config_path:str, datasource:str, is_logging_enabled=True, execution_id=None,executed_by=None)->None:
    """
    parms config_path: confingaration path for params.yaml
    parms datasource: datasource path 
    parms is_logging_enabled: default is 'True'
    parms execution_id: default is 'None' System will generate it.
    parms executed_by: default is 'None' System will consider default user.

    This function deletes the existing training files and additional training files from local.
    Furthermore, it download training and additional training files from cloud storage to local.
    """
    try:
        logger=get_logger_object_of_training(config_path=config_path,collection_name=log_collection_name,
                                            execution_id=execution_id,executed_by=executed_by)
        logger.is_log_enabled=is_logging_enabled
        logger.log("Starting data loading operation.\nReading configuration file.")

        config=read_params(config_path)

        downloader_path=config['data_download']['cloud_training_directory_path']
        download_path=config['data_source']['Training_Batch_Files']

        logger.log("Configuration details has been fetched from configuration file")
        # Removing training and additional training files from local.
        logger.log(f"Cleaning local directory [{download_path}].")

        clean_data_source_dir(download_path,logger=logger,
                              is_logging_enable=is_logging_enabled)     # Removing existing file from local.

        logger.log(f"Cleaning completed.[{download_path}] Directory has been clean. ")
        # Downloading training and additional training files from cloud into local system. 
        logger.log("Data will be downloaded from cloud storage into local.")

        for file in os.listdir(downloader_path):
            if '.dvc' in file or '.gitignore' in file:
                continue
            print(f"Source directory: {downloader_path} File: {file} is being copied into destination directory: {download_path}"
                  f"file: {file}")
            shutil.copy(os.path.join(downloader_path, file), os.path.join(download_path, file))

        logger.log("Data has been downloaded from cloud storage into local.")
    
    except Exception as e:
        generic_expression = GenericException(
            "Error occured in module [{0}] method [{0}]"
            .format(loader_main.__module__,
                    loader_main.__name__))
        raise Exception(generic_expression.error_message_details(str(e),sys)) from e


if __name__ == "__main__":
    args=argparse.ArgumentParser()
    args.add_argument("--config","-c", default=os.path.join("confg","params.yaml"))
    args.add_argument("--datasource","-d", default=None)
    parsed_args=args.parse_args()
    print('stage_00_data_loader')
    