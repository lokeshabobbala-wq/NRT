"""
AWS Glue Job: Data Validation & Conversion

Key Features:
- Reads data files from S3 (CSV, Excel)
- Applies data quality checks and transformations
- Uses a configuration file from S3 for processing rules
- Logs execution details and errors
- Stores processed data back in S3
- Integrates with AWS services: S3, Glue, RDS, SNS, CloudWatch

Components:
1. **Initialization**: Sets up job parameters, AWS clients, and logging
2. **Main Execution**: Reads config, validates data, and applies transformations
3. **Data Processing**: Processes CSV/Excel files with data quality checks
4. **Error Handling & Logging**: Captures errors and logs execution status
5. **Audit & Notifications**: Updates logs and triggers notifications if needed
"""
from botocore.exceptions import BotoCoreError, ClientError
from awsglue.utils import getResolvedOptions
from xlwt import Workbook
from pyspark.sql.functions import col, count, collect_list, size, lit, unix_timestamp, from_unixtime, when, concat, udf
from pyspark.sql import functions as F
from pyspark.sql import Window, DataFrame, SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, StructField, DoubleType
import pandas as pd
import numpy as np
from functools import reduce
import datetime as dt
from datetime import date
import time
import boto3
import json
import os
import traceback
import sys
import gzip
import io
import logging
import getpass
import pyxlsb
from urllib.parse import urlparse
import datetime
import openpyxl
import ast
import csv
# Import the custom Logging class from common_utility_funtional.py,
# which is stored in S3 at: s3://sc360-dev-ww-bucket/CommonScripts/common_utility_funtional.py
# This handles logging setup, info, error, and audit-level logging within the AWS Glue job.
from common_utility_funtional import Logging

# Import the common_lib class from common_utility_technical.py,
# which is stored in S3 at: s3://sc360-dev-ww-bucket/CommonScripts/common_utility_technical.py
# located in the same S3 path, containing core reusable functions for data validation and processing logic.
from common_utility_technical import common_lib


class DataConversionValidation(common_lib):
    """
    AWS Glue Job for Data Validation & Conversion
    This class handles data extraction, validation, transformation, and logging
    before storing processed data in an S3 bucket.
    
    Inherits from:
        common_lib (from common_utility_technical): 
            Provides shared utility functions for data handling and validation 
            that are reused across multiple technical components.
    """
    # Initialization method
    def __init__(self, file_key, file_name, batch_id, data_pipeline_name, pipeline_run_id, exec_env,sns_arn,resourcearn,secretarn,database,schema,Job_name,region,loggroupname,job_run_id):
        """
        Initialize job parameters and AWS clients.
        
        Args:
            file_key (str): Unique identifier for the input file.
            file_name (str): Name of the file to be processed.
            batch_id (str): Batch ID for tracking execution.
            data_pipeline_name (str): Name of the data pipeline.
            pipeline_run_id (str): Unique ID for this execution instance.
            exec_env (str): Execution environment (e.g., dev, prod).
            sns_arn (str): AWS SNS topic for notifications.
            resourcearn (str): AWS resource ARN for database access.
            secretarn (str): AWS Secrets Manager ARN.
            database (str): Database name for logging audit data.
            schema (str): Schema name in the database.
            job_name (str): AWS Glue Job name.
            region (str): AWS region where job is executed.
            loggroupname (str): AWS CloudWatch log group.
            job_run_id (str): Unique run ID for this execution.
        """
        try:
            # Initialize the arguments to the script
            self.execution_start_time = time.strftime('%Y-%m-%d %H:%M:%S')
            self.file_key = str(file_key).strip()
            self.file_name = str(file_name).strip()
            self.batch_id = str(batch_id).strip()
            self.data_pipeline_name = str(data_pipeline_name).strip()
            self.pipeline_run_id = str(pipeline_run_id).strip()
            self.exec_env = str(exec_env).strip()
            self.getuser = str(getpass.getuser()).strip()
            self.job_name = str(Job_name).strip()
            self.region = str(region).strip()
            self.loggroupname = str(loggroupname).strip()
            self.job_run_id = str(job_run_id).strip()

            # Define the class variables
            self.batch_date = str(dt.datetime.strptime(str(self.batch_id.split('_')[1].strip()), "%Y%m%d").date())
            self.region_name = str(self.batch_id.split('Batch')[0]).strip()
            self.process_name = 'DataValidation'
            self.dv_execution_status = 'Failed'
            self.error_message = 'NA'
            self.log_list_dictionary = []
            self.log_filepath = f"s3://sc360-{self.exec_env}-log-bucket/EMR-Lambda-Logs/dt={self.batch_date}/{self.pipeline_run_id}/"

            # Create the paths dynamically
            self.input_file_path =f"s3://sc360-{self.exec_env}-{self.region_name.lower()}-bucket/ProcessingZone/dt={self.batch_date}/{self.file_name}"
            self.output_file_path = f"s3://sc360-{self.exec_env}-{self.region_name.lower()}-bucket/TempRawZone/dt={self.batch_date}/"
            self.output_file_path_del = f"s3://sc360-{self.exec_env.lower()}-{self.region_name.lower()}-bucket/TempRawZone/dt={self.batch_date}/"
            self.config_file_path = f"s3://sc360-{self.exec_env.lower()}-{self.region_name.lower()}-bucket/Resources/Configs/DataConversionValidation/config_{self.file_key}.json"
            self.reject_file_path = f"s3://sc360-{self.exec_env.lower()}-{self.region_name.lower()}-bucket/RejectZone/FileRejectZone/dt={self.batch_date}/"
            self.reject_data_path = f"s3://sc360-{self.exec_env.lower()}-{self.region_name.lower()}-bucket/RejectZone/DataRejectZone/dt={self.batch_date}/"
            self.script_path = f"s3://sc360-{self.exec_env.lower()}-{self.region_name.lower()}-bucket/Resources/Scripts/DataConversionValidation/data_conversion_validation.py"
            self.final_output_file_name = self.file_key + "_" + time.strftime('%Y%m%d%H%M%S') + ".csv"

            # Initialize the count variables for audit tables
            self.landing_count = 0
            self.raw_count = 0
            self.reject_count = 0
            self.total_count = 0

            # Create the Spark session
            self.spark = SparkSession.builder.appName("DQ").config('spark.sql.codegen.wholeStage', 'false').getOrCreate()
            self.spark.conf.set("spark.sql.shuffle.partitions", 20)
            self.spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
            self.log_file_name = f"{self.batch_id}_{self.batch_date}_{self.data_pipeline_name}_{self.pipeline_run_id}_{self.file_name}_{time.strftime('%Y-%m-%d-%H:%M:%S')}_log.log"

            # Loggin initialization
            self.dq_logging = Logging(self.spark, "DQ", self.log_file_name)

            # Define the boto3 clients
            self.s3_client = boto3.client('s3')

            # AWS Secrets Manager secret
            self.resourceArn = resourcearn
            self.secretArn = secretarn
            self.database = database
            self.schema = schema

            # SNS Details
            self.sns_arn = sns_arn
            
            self.dq_logging.debug(f"Initialized job for file_key: {self.file_key}, file_name: {self.file_name}")
            self.dq_logging.debug(f"Batch ID: {self.batch_id}, Region: {self.region_name}, Env: {self.exec_env}")
            self.dq_logging.debug(f"Input file path: {self.input_file_path}")
            self.dq_logging.debug(f"Config file path: {self.config_file_path}")

            self.dq_logging.info("Initialization complete.")
        except Exception as e:
            self.exception_error(e,"Initialization Method", "")
    
    # Main method
    def main(self):
        """
        Main function for data extraction, validation, and transformation.
        Reads the config file, applies transformations, and logs audit details.
        """
        try:
            # Define Orderbook Backlog files specific to each region
            region_orderbook_backlog = [f"{self.region.upper()}_Orderbook_Backlog"]
            is_csv = self.file_name.split('.')[-1].lower() == 'csv'
            
            # Read the config file using boto3
            self.config_file_bucket = self.config_file_path.split('/', 3)[2]
            self.config_file_key = self.config_file_path.split('/', 3)[3]

            print(f"Loading config File from Bucket: {self.config_file_bucket}, File Key: {self.config_file_key}")

            try:
                content_object = self.s3_client.get_object(Bucket=self.config_file_bucket, Key=self.config_file_key)
                self.config_file_content = json.loads(content_object['Body'].read())

            except self.s3_client.exceptions.NoSuchKey as e:
                print(f"[ERROR] NoSuchKey: The key '{self.config_file_key}' does not exist in bucket '{self.config_file_bucket}'.")
                print("--File does not exist, skipping to next record--")
                return  # **Exit main() and move to next row in response['records']**

            except Exception as e:
                print(f"[ERROR] Unexpected error while fetching config: {e}")
                return  # **Skip execution and move to the next record**

            # Continue execution only if config file exists
            print(f"Config file {self.config_file_key.split('/')[-1]} loaded successfully, proceeding with execution.")
            
            # Initialize lists for rejected data and logs
            self.rejected_df_list = []
            self.log_list_dictionary = []
            self.execution_start_time = time.strftime('%Y-%m-%d %H:%M:%S')
            self.dq_logging.info(f"Operating on the file: {self.file_name}")
            
            if is_csv:
                # Process CSV files
                self.sheet_name = list(self.config_file_content[self.file_key].keys())[0]
                self.read_config_file_details()
                self.dq_logging.debug(f"CSV Sheet Name: {self.sheet_name}, Columns: {self.required_column_list}")
                self.input_file_df = self.read_csv_file()
                self.landing_count = self.input_file_df.count()
                
                # Perform Data Quality (DQ) checks for CSV files
                if self.file_key not in region_orderbook_backlog:
                    self.cast_to_integer()                                                      # DQ11 - Cast to Int check
                    self.replace_null_values()                                                  # DQ12 - Replace Null values check
                    self.replace_speacial_characters_columns()                                  # DQ3 - Replace Special Characters
                    self.removing_alphabets()                                                   # DQ4 - Removing Alphabets
                    self.trimming_non_breakable_spaces()                                        # DQ1 - Trimming Non Breakable Spaces
                    self.trimming_spaces()                                                      # DQ2 - Trimming Leading & Trailing Spaces
                    self.replace_zeros_with_null_date_column()                                  # DQ13 - Replace Zeros with Null in Date Columns
                else:
                    print(f'Unwanted DQ skipped for {self.region.upper()}_Orderbook_Backlog')
                
                self.row_level_duplicate_check_csv(self.region)                                 # DQ6 - Row level duplicate check
                self.integer_check_csv()                                                        # DQ7 - Integer Format check
                self.null_check()                                                               # DQ8 - Null check
                self.date_format_check()                                                        # DQ9 - Date Format check
                self.standarize_date_format()                                                   # DQ14 - Standardize Date Format
                self.timestamp_format_check()                                                   # DQ10 - Timestamp Format check
                self.standardize_timestamp_format()                                             # DQ15 - Standardize Timestamp Format
                self.rejected_records_handle_write_output(csv=True)                             # Handle rejected records and write output
                
                # Handling rejected files
                self.dv_execution_status = 'Succeeded' if self.reject_count == 0 else 'Failed'
                self.error_message = f'Full File Rejected as we have {self.reject_count} rejection records' if self.reject_count > 0 else ''
                self.execution_end_time = time.strftime('%Y-%m-%d %H:%M:%S')
                self.update_audit_log()  # Update audit log table
            
            else:
                # Process Excel files
                sheet_list = self.config_file_content[self.file_key].keys()
                for each_sheet in sheet_list:
                    self.sheet_name = each_sheet
                    self.read_config_file_details()
                    self.input_file_df = self.master_data_conversion().select("*").cache()
                    self.landing_count = self.input_file_df.count()
                    self.dq_logging.debug(f"Excel file {self.file_name}, sheet {self.sheet_name} read with {self.landing_count} records")
                    if self.landing_count > 0:
                        # Perform DQ checks for Excel files
                        if self.file_key not in region_orderbook_backlog:
                            self.remove_empty_columns()                                     # DQ11 - Remove empty columns
                            self.replace_speacial_characters_columns()                      # DQ3 - Replace Special Characters
                            self.decimal_columns_calculation()                              # Decimal column calculation
                            self.cast_to_integer()                                          # DQ11 - Cast to Int check
                            self.cast_to_double()                                           # DQ11 - Cast to double check
                            self.replace_null_values()                                      # DQ12 - Replace Null values check
                            self.trimming_non_breakable_spaces()                            # DQ1 - Trimming Non Breakable Spaces
                            self.trimming_spaces()                                          # DQ2 - Trimming Leading & Trailing Spaces
                            self.removing_alphabets()                                       # DQ4 - Removing Alphabets
                            self.replace_zeros_with_null_date_column()                      # DQ13 - Replace Zeros with Null in Date Columns
                            self.edi_columns_cleanup()                                      # DQ5 - EDI Columns Cleanup
                        else:
                            print(f'Unwanted DQ skipped for {self.region.upper()}_Orderbook_Backlog')
                        
                        self.row_level_duplicate_check(self.region)                         # DQ6 - Row level duplicate check
                        self.integer_check()                                                # DQ7 - Integer Format check
                        self.null_check()                                                   # DQ8 - Null check
                        self.date_format_check()                                            # DQ9 - Date Format check
                        self.standarize_date_format()                                       # DQ14 - Standardize Date Format
                        self.timestamp_format_check()                                       # DQ10 - Timestamp Format check
                        self.standardize_timestamp_format()                                 # DQ15 - Standardize Timestamp Format
                        self.rejected_records_handle_write_output()                         # Handle rejected records and write output
                        
                        # Handling rejected files
                        self.dv_execution_status = 'Succeeded' if self.reject_count == 0 else 'Failed'
                        self.error_message = f'Full File Rejected as we have {self.reject_count} rejection records' if self.reject_count > 0 else ''
                        self.execution_end_time = time.strftime('%Y-%m-%d %H:%M:%S')
                        self.update_audit_log()  # Update audit log table
                    else:
                        if self.file_key not in region_orderbook_backlog and self.file_key not in ('APJ_OSS', 'AMS_OSS', 'EMEA_OSS'):
                            self.empty_file_rejection()  # Handle empty file rejection
                        print("OSS files Successfully rejected")
                        self.execution_end_time = time.strftime('%Y-%m-%d %H:%M:%S')
                        self.error_message = 'Full File Rejected as we have zero records'
                        self.dv_execution_status = 'Succeeded'
                        self.update_audit_log()  # Update audit log table
        except (ClientError, BotoCoreError) as e:
            print(f"Error occurred")
            print(f"  Error code: {e.response.get('Error', {}).get('Code')}")
            print(f"  Error message: {e.response.get('Error', {}).get('Message')}")
            print(f"  Request ID: {e.response.get('ResponseMetadata', {}).get('RequestId')}")
            print(f"  Full error response: {e.response}")
            raise RuntimeError("Error occured") from e
            print("=== Full traceback ===")
            traceback.print_exc()
        except Exception as e:
            print("--Error at main--", str(e))
            self.dq_logging.debug(f"Exception traceback: {traceback.format_exc()}")
            self.dq_logging.error("Exception caught in the main method.")
            self.dv_execution_status = 'Failed'
            self.execution_end_time = time.strftime('%Y-%m-%d %H:%M:%S')

if __name__ == "__main__":
    """
    Entry point for the AWS Glue job.
    Reads job arguments, fetches file metadata, and invokes the main function.
    """
    try:
        print("Script Execution Started")
        start_time = time.time()
        
        args = getResolvedOptions(sys.argv, ['JOB_NAME', 'stepfunction_name', 'env', 'region', 'sns_arn','resourcearn', 'secretarn', 'database', 'schema'])
        
        loggroupname = '/aws-glue/jobs/output'
        job_run_id = args['JOB_RUN_ID']
        stepfunction_name = f"sc360-{args['env']}-dataFlow-{args['region']}"
        batchrundate = date.today()
        
        print('Connecting with RDS...')
        client = boto3.client('rds-data')
        
        sql = f"""
            SELECT FileName, BatchId, DestinationName 
            FROM audit.sc360_audit_log 
            WHERE BatchRunDate='{batchrundate}' 
            AND ExecutionStatus='ReadyToExecute' 
            AND ProcessName='FileValidation' 
            AND regionname='{args['region']}';
        """
        #################################Test Code Starts#################################################
        
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                print(f"[Attempt {attempt}] Executing RDS SQL")
                response_data = client.execute_statement(
                    resourceArn=args['resourcearn'],
                    secretArn=args['secretarn'],
                    database=args['database'],
                    sql=sql
                )
                break  # Success, exit retry loop
            except (ClientError, BotoCoreError) as e:
                # Print detailed AWS error info for root cause analysis
                error_code = e.response.get('Error', {}).get('Code') if hasattr(e, 'response') else None
                error_message = e.response.get('Error', {}).get('Message') if hasattr(e, 'response') else str(e)
                request_id = e.response.get('ResponseMetadata', {}).get('RequestId') if hasattr(e, 'response') else None
                full_error = e.response if hasattr(e, 'response') else str(e)
        
                print(f"[Attempt {attempt}] RDS query failed:")
                print(f"  Error code: {error_code}")
                print(f"  Error message: {error_message}")
                print(f"  Request ID: {request_id}")
                print(f"  Full error response: {full_error}")
        
                if attempt == max_retries:
                    raise RuntimeError(f"RDS query failed after {max_retries} attempts") from e
                
                # Wait before next retry (exponential backoff)
                time.sleep(3 * attempt)
                
        #################################Test Code Ends###################################################
        print("Connection established.")
        
        # Extract file names before processing each row
        file_names = [row[2]['stringValue'] for row in response_data['records']]
        
        # Print all file names
        print("Files to be processed:", file_names)
        
        for row in response_data['records']:
            try:
                filekey, Batch_Id, Filename = row[0]['stringValue'], row[1]['stringValue'], row[2]['stringValue']
                print(f"Region:{args['region']} ,Processing File: {filekey}, Batch ID: {Batch_Id}, Destination: {Filename}")
                
                dcv = DataConversionValidation(
                    filekey, Filename, Batch_Id, stepfunction_name, stepfunction_name,
                    args['env'], args['sns_arn'], args['resourcearn'], args['secretarn'],
                    args['database'], args['schema'], args['JOB_NAME'], args['region'], loggroupname, job_run_id
                )
                dcv.main()
            except Exception as e:
                print(f'Error occurred: {e}')
                dcv.dq_logging.error("Exception caught in the Class Main.")
                dcv.execution_end_time = time.strftime('%Y-%m-%d %H:%M:%S')
            finally:
                execution_duration = time.time() - start_time
                print(f'Execution Time: {execution_duration} seconds')
                dcv.dq_logging.info(f'Total execution time: {execution_duration} seconds')
                dcv.dq_logging.info(f'Log File Name: {dcv.log_file_name}')
                dcv.dq_logging.info("Writing the log file to log output path...")
                dcv.dq_logging.info("log file is written to log output path successfully.")
                dcv.dq_logging.info("END") 
    except Exception as e:
        print(f'Critical Error: {e}')
        sys.exit(f'Exception in main: {e}')
    finally:
        print('File processing completed.')
