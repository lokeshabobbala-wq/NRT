"""
AWS Glue Job: Report Refresh Automation

This Glue job automates the execution of stored procedures (SPs) in an AWS Redshift database. 
It retrieves pending SPs, executes them with a retry mechanism, and updates audit logs.

Key Features:
- Fetches pending stored procedures from RDS
- Executes stored procedures in Redshift
- Implements a retry mechanism for failed executions
- Logs execution details in an audit table
- Sends SNS notifications upon success or failure

Components:
1. **AWS Glue Runtime Parameters**: Retrieves parameters like job name, region, and database credentials.
2. **AWS Clients**: Initializes RDS, Redshift, and SNS clients.
3. **SP Execution with Retry**: Calls SPs with a retry mechanism to handle failures.
4. **Audit Logging**: Updates execution logs in an RDS audit table.
5. **SNS Notifications**: Notifies users about job status.

This job ensures robust and automated execution of Redshift stored procedures.
"""

from awsglue.utils import getResolvedOptions
import json
import os
import sys
import time
import boto3
from datetime import datetime, date
from botocore.exceptions import BotoCoreError, ClientError

class ReportRefresh:
    def __init__(self):
        """
        Initializes the ReportRefresh class and retrieves Glue job parameters.
        Sets up AWS clients for RDS, Redshift, and SNS.
        """
        self.BatchRunDate = date.today()                        # Stores today's date for batch processing

        # Get Glue job parameters
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME', 'env', 'sns_arn', 'reportregionname', 'resourcearn',
            'secretarn', 'database', 'schema', 'clusteridentifier', 
            'redshiftdatabase', 'redshiftuser', 'redshiftsecret', 
            'users_sns_arn', 'report_url', 'RETRY_LIMIT'
        ])

        # Parameter initialization
        self.RETRY_LIMIT = int(args['RETRY_LIMIT'])             # Max retries for SP execution
        self.Job_name = args['JOB_NAME']                        # Glue job name
        self.loggroupname = '/aws-glue/jobs/output'             # CloudWatch log group
        self.job_run_id = args['JOB_RUN_ID']                    # Unique Glue job run ID
        self.reportregionname = args['reportregionname']        # Report region name
        self.resourcearn = args['resourcearn']                  # AWS Resource ARN
        self.secretarn = args['secretarn']                      # Secret ARN for authentication
        self.database = args['database']                        # Database name
        self.schema = args['schema']                            # Schema name
        self.clusteridentifier = args['clusteridentifier']      # Redshift cluster ID
        self.redshiftdatabase = args['redshiftdatabase']        # Redshift database name
        self.redshiftuser = args['redshiftuser']                # Redshift user
        self.redshiftsecret = args['redshiftsecret']            # Redshift secret ARN
        self.env = args['env']                                  # Execution environment
        self.sns_arn = args['users_sns_arn']                    # SNS ARN for notifications
        self.report_url = args['report_url']                    # Report URL for successful execution

        self.rds_client = boto3.client('rds-data')              # RDS Data client for executing SQL queries
        self.redshiftclient = boto3.client('redshift-data')     # Redshift client for stored procedures
        self.sns_client = boto3.client('sns')                   # SNS client for notifications

        self.failedsps = []                                     # List to track failed stored procedures

    def send_sns_message(self, status, failed_sps=[]):
        """
        Sends an SNS notification regarding the job status.

        Args:
            status (str): Status of the job (e.g., "Success" or "Failure").
            failed_sps (list, optional): List of failed processing steps, if any. Defaults to an empty list.

        Returns:
            None
        """
        try:
            print("Preparing SNS message payload...")
            sns_message = {
                "Env": self.env,
                "Error Message": failed_sps,
                "status": status,
                "Region": self.reportregionname,
                "Glue_Job_name": self.Job_name,
                "Log_Group": self.loggroupname,
                "Log_Stream_ID": self.job_run_id
            }
            sns_subject = f"{self.reportregionname} SPDST/Others Report Refresh {'Failed' if failed_sps else 'Successful'}"
            
            print(f"Publishing SNS notification: {sns_subject}")
            self.sns_client.publish(
                TargetArn=self.sns_arn,
                Message=json.dumps(sns_message),
                Subject=sns_subject
            )
            print("SNS notification published successfully.")
            
            if len(failed_sps) == 0:
                print("Preparing success notification payload...")
                sns_message = {
                    "Env": self.env,
                    "Message": self.reportregionname + ' SPDST/others Report Refresh is Completed Successfully.',
                    "Report_Url": self.report_url
                }
                sns_subject = self.reportregionname + ' SPDST/others Report Refresh Successful'
                
                print(f"Publishing success notification: {sns_subject}")
                self.sns_client.publish(
                    TargetArn=self.sns_arn,
                    Message=json.dumps(sns_message),
                    Subject=sns_subject
                )
                print("Success SNS notification published successfully.")
        except Exception as e:
            print(f"Error sending SNS notification: {e}")

    def update_audit_log(self, status, execution_status=None, is_initial=False):
        """
        Updates the audit log tables in the database with job status.

        Args:
            status (str): The status to update in the audit log.
            execution_status (str, optional): The execution status for the report refresh process. Defaults to None.
            is_initial (bool, optional): Indicates if this is the initial log update. Defaults to False.

        Returns:
            None
        """
        max_retries = 3

        def execute_with_retry(sql, description):
            for attempt in range(1, max_retries + 1):
                try:
                    print(f"[Attempt {attempt}] Executing audit log update: {description}")
                    self.rds_client.execute_statement(
                        resourceArn=self.resourcearn,
                        secretArn=self.secretarn,
                        database=self.database,
                        sql=sql
                    )
                    print(f"{description} updated successfully.")
                    break
                except Exception as e:
                    print(f"[Attempt {attempt}] RDS query failed: {e}")
                    if attempt == max_retries:
                        raise RuntimeError(f"RDS query failed after {max_retries} attempts") from e
                    time.sleep(3 * attempt)

        try:
            if is_initial:
                sql = f"""
                    UPDATE audit.Master_Data_For_IRR
                    SET actual_start_time = '{datetime.utcnow()}', status = '{status}'
                    WHERE regionname = '{self.reportregionname}' AND identifier = 'SPDST'
                """
                execute_with_retry(sql, "Master_Data_For_IRR (initial)")
            else:
                sql1 = f"""
                    UPDATE audit.sc360_reportrefreshtrigger_log
                    SET execution_status = '{execution_status}', actual_end_time = '{datetime.utcnow()}',
                    error_message = '{self.error_message}'
                    WHERE regionname = '{self.reportregionname}' AND batchrundate = '{self.BatchRunDate}'
                      AND report_source = 'SPDST'
                """
                execute_with_retry(sql1, "sc360_reportrefreshtrigger_log")

                sql2 = f"""
                    UPDATE audit.Master_Data_For_IRR
                    SET status = '{status}'
                    WHERE regionname = '{self.reportregionname}' AND identifier = 'SPDST'
                """
                execute_with_retry(sql2, "Master_Data_For_IRR")
        except (ClientError, BotoCoreError) as e:
            print(f"Error updating audit logs: {e}")
            print(f"Error response: {getattr(e, 'response', 'No response available')}")
        except Exception as e:
            print(f"Error updating audit logs: {e}")
            import traceback
            traceback.print_exc()

    def fetch_pending_sps(self):
        """
        Retrieves a list of stored procedures pending execution from the database.

        Args:
            None

        Returns:
            list: A list of tuples containing stored procedure names and their respective file data sources.
        """
        max_retries = 3
        sql = f"""
            SELECT stored_procedure_name, file_datasource FROM audit.sc360_reportrefresh
            WHERE region = '{self.reportregionname}' AND file_datasource NOT LIKE '%BMT%'
            ORDER BY exec_order
        """

        for attempt in range(1, max_retries + 1):
            try:
                print(f"[Attempt {attempt}] Fetching pending SPs from audit.sc360_reportrefresh")
                response = self.rds_client.execute_statement(
                    resourceArn=self.resourcearn,
                    secretArn=self.secretarn,
                    database=self.database,
                    sql=sql
                )
                return [(row[0]['stringValue'], row[1]['stringValue']) for row in response['records']]
            except Exception as e:
                print(f"[Attempt {attempt}] RDS query failed: {e}")
                if attempt == max_retries:
                    raise RuntimeError(f"RDS query failed after {max_retries} attempts") from e
                time.sleep(3 * attempt)

        # If somehow all attempts fail and don't raise (safety net)
        return []

    def check_query_status(self, query_id, sleep_time=120):
        """
        Checks the status of a Redshift query.

        Args:
        query_id (str): The ID of the query to check.
        sleep_time (int): The time to wait (in seconds) between status checks. Default is 120 seconds.

        Returns:
        str: The final status of the query.
        """
        query_status = 'SUBMITTED'
        
        while query_status not in ['FINISHED', 'FAILED', 'ABORTED']:
            try:
                response_status = self.redshiftclient.describe_statement(Id=query_id)
                query_status = response_status['Status']
            except Exception as e:
                print(f"Error fetching query status: {e}")
                response_status = {'Status': 'FAILED', 'Error': str(e)}
                query_status = 'FAILED'  # Set status to FAILED to exit the loop in case of an error
                time.sleep(sleep_time)  # Wait before checking the status again
        return response_status, query_status

    def execute_sp_with_retries(self, sp_name, file_source):
        """
        Executes a stored procedure with retries in case of failure.

        Args:
            sp_name (str): Name of the stored procedure to execute.
            file_source (str): Source of the file associated with the stored procedure.

        Returns:
            bool: True if execution is successful, False if all retries fail.
        """
        retries = 0
        while retries < self.RETRY_LIMIT:
            try:
                print(f"Executing SP: {sp_name} (Attempt {retries + 1}/{self.RETRY_LIMIT})")
                response = self.redshiftclient.execute_statement(
                    ClusterIdentifier=self.clusteridentifier,
                    Database=self.redshiftdatabase,
                    SecretArn=self.redshiftsecret,
                    Sql=f"CALL {sp_name};",
                    WithEvent=True
                )
                query_id = response['Id']
                response_status, query_status = self.check_query_status(query_id)
                print(f"Query status for Execution of SP: {sp_name} is {query_status}")  # Print after fetching status
                
                if query_status == "FAILED":
                    error_msg = response_status.get("Error", "Unknown error")
                    print(f"SP {sp_name} failed on attempt {retries + 1} in {self.env} - {self.reportregionname}: {error_msg}")
                    retries += 1    # Increment retry count

                    if retries < self.RETRY_LIMIT:
                        print(f"Retrying SP {sp_name} ({retries}/{self.RETRY_LIMIT}) in {self.env} - {self.reportregionname} after waiting 2 minutes...")
                        time.sleep(120)  # **Wait before retrying**
                        continue    # Retry
                    else:
                        print(f"SP {sp_name} failed after {self.RETRY_LIMIT} attempts in {self.env} - {self.reportregionname}.")
                        self.failed_sp_entry = {
                                    "Stored Procedure Name": sp_name,
                                    "Error": error_msg,
                                    "Data Source": file_source if file_source == "SPDST" else "SPDST/Others",
                                    "Region": self.reportregionname
                        }               
                        self.error_message = error_msg.split(":", 1)[-1] if ":" in error_msg else error_msg
                        return False    # Return failure
                else:
                    print(f"SP {sp_name} executed successfully in {self.env} - {self.reportregionname}.")
                    self.error_message = "Null"
                    return True
            except Exception as e:
                print(f"Error executing SP {sp_name}: {e}")
                retries += 1
        print(f"SP {sp_name} failed after {self.RETRY_LIMIT} retries in {self.env} - {self.reportregionname}.")
        return False

    def main(self):
        """
        Glue Job main function for SPDST Report Refresh orchestration.

        Workflow:
        1. Initialize connections and environment variables
        2. Fetch list of stored procedures pending execution
        3. Execute each stored procedure with retries:
            - If any SP fails after all retries, stop execution
        4. Update audit log upon completion or failure
        5. Send SNS notifications based on the final execution status

        Args:
            None

        Returns:
            bool: True if all stored procedures execute successfully, False otherwise.
        """
        try:
            print(f"Starting Report Refresh Job in {self.env} - {self.reportregionname}...")
            self.update_audit_log('InProgress', is_initial=True)
            stored_procedures = self.fetch_pending_sps()

            if not stored_procedures:
                print(f"No pending stored procedures found in {self.env} - {self.reportregionname}. Exiting job.")
                self.send_sns_message("Succeeded")
                return True
            
            print(f"Found {len(stored_procedures)} stored procedures to execute in {self.env} - {self.reportregionname}.")

            for sp_name, file_source in stored_procedures:
                print(f"Processing SP: {sp_name} (Source: {file_source}) in {self.env} - {self.reportregionname}...")
                success = self.execute_sp_with_retries(sp_name, file_source)

                if not success:
                    print(f"SP {sp_name} failed after {self.RETRY_LIMIT} attempts in {self.env} - {self.reportregionname}. Stopping job.")

                    # **Handle failure here**
                    self.update_audit_log("Failed", execution_status="Failed", is_initial=False)
                    self.send_sns_message("Failed", [self.failed_sp_entry])

                    # **Raise Exception to Stop Execution**
                    raise Exception(f"SP {sp_name} failed after {self.RETRY_LIMIT} retries in {self.env} - {self.reportregionname}. Job terminating.")
            
            # If all SPs succeed
            self.update_audit_log("Completed", execution_status="Finished", is_initial=False)
            print(f"All stored procedures executed successfully in {self.env} - {self.reportregionname}.")
            self.send_sns_message("Succeeded")

            return True
        except Exception as e:
            print(f"Unexpected error in main: {e} (Env: {self.env}, Region: {self.reportregionname})")
            self.update_audit_log("Failed", execution_status="Failed", is_initial=False)
            self.send_sns_message("Failed", ["Unexpected error"])
            raise  # **Re-raise the exception to ensure Glue job fails properly**

if __name__ == "__main__":
    job = ReportRefresh()
    job.main()
