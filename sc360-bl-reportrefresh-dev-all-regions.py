"""
AWS Glue Job: BMT Report Refresh Automation

This Glue job automates the execution of BMT stored procedures (SPs) in an AWS Redshift database.
It retrieves pending BMT SPs, executes them with a retry mechanism, and updates audit logs.

Key Features:
- Fetches pending BMT stored procedures from RDS
- Executes stored procedures in Redshift
- Implements a retry mechanism for failed executions
- Logs execution details in audit tables
- Sends SNS notifications upon success or failure

This job ensures robust and automated execution of BMT Redshift stored procedures for any region.
"""

from awsglue.utils import getResolvedOptions
import json
import os
import sys
import time
import boto3
from datetime import datetime, date
from botocore.exceptions import BotoCoreError, ClientError

class BMTReportRefresh:
    def __init__(self):
        """
        Initializes the BMTReportRefresh class and retrieves Glue job parameters.
        Sets up AWS clients for RDS, Redshift, and SNS.
        """
        self.BatchRunDate = date.today()                        # Stores today's date for batch processing

        # Get Glue job parameters, including BMT-specific ones
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME', 'env', 'sns_arn', 'reportregionname', 'resourcearn',
            'secretarn', 'database', 'schema', 'clusteridentifier', 
            'redshiftdatabase', 'redshiftuser', 'redshiftsecret', 
            'users_sns_arn', 'report_url', 'RETRY_LIMIT', 'priority_File_recieved'
        ])

        # Parameter initialization
        self.RETRY_LIMIT = int(args['RETRY_LIMIT'])             # Max retries for SP execution
        self.Job_name = args['JOB_NAME']
        self.loggroupname = '/aws-glue/jobs/output'
        self.job_run_id = args['JOB_RUN_ID']
        self.reportregionname = args['reportregionname']
        self.resourcearn = args['resourcearn']
        self.secretarn = args['secretarn']
        self.database = args['database']
        self.schema = args['schema']
        self.clusteridentifier = args['clusteridentifier']
        self.redshiftdatabase = args['redshiftdatabase']
        self.redshiftuser = args['redshiftuser']
        self.redshiftsecret = args['redshiftsecret']
        self.env = args['env']
        self.sns_arn = args['users_sns_arn']
        self.report_url = args['report_url']
        self.priority_File_Flag = args['priority_File_recieved']

        self.rds_client = boto3.client('rds-data')
        self.redshiftclient = boto3.client('redshift-data')
        self.sns_client = boto3.client('sns')

        self.failedsps = []

    def send_sns_message(self, status, failed_sps=[]):
        """
        Sends an SNS notification regarding the job status.
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
            sns_subject = f"{self.reportregionname} BMT Report Refresh {'Failed' if failed_sps else 'Successful'}"
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
                    "Message": self.reportregionname + ' BMT Report Refresh is Completed Successfully.',
                    "Report_Url": self.report_url
                }
                sns_subject = self.reportregionname + ' BMT Report Refresh Successful'
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
        """
        max_retries = 3

        def execute_with_retry(sql, description):
            for attempt in range(1, max_retries + 1):
                print(f"[Attempt {attempt}] Executing audit log update: {description}")
                try:
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
                    WHERE regionname = '{self.reportregionname}' AND identifier = 'BMT'
                """
                execute_with_retry(sql, "Master_Data_For_IRR (initial)")
            else:
                sql1 = f"""
                    UPDATE audit.sc360_reportrefreshtrigger_log
                    SET execution_status = '{execution_status}', actual_end_time = '{datetime.utcnow()}',
                    error_message = '{self.error_message}'
                    WHERE regionname = '{self.reportregionname}' AND batchrundate = '{self.BatchRunDate}'
                      AND report_source = 'BMT'
                """
                execute_with_retry(sql1, "sc360_reportrefreshtrigger_log")

                sql2 = f"""
                    UPDATE audit.Master_Data_For_IRR
                    SET status = '{status}'
                    WHERE regionname = '{self.reportregionname}' AND identifier = 'BMT'
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
        Retrieves a list of BMT stored procedures pending execution from the database.
        """
        max_retries = 3
        sql = f"""
            SELECT stored_procedure_name FROM audit.sc360_reportrefresh
            WHERE region = '{self.reportregionname}' 
              AND file_datasource LIKE '%BMT%' 
              AND codebase = '{self.priority_File_Flag}'
            ORDER BY exec_order
        """

        for attempt in range(1, max_retries + 1):
            print(f"[Attempt {attempt}] Fetching pending BMT SPs from audit.sc360_reportrefresh")
            try:
                response = self.rds_client.execute_statement(
                    resourceArn=self.resourcearn,
                    secretArn=self.secretarn,
                    database=self.database,
                    sql=sql
                )
                return [row[0]['stringValue'] for row in response['records']]
            except Exception as e:
                print(f"[Attempt {attempt}] RDS query failed: {e}")
                if attempt == max_retries:
                    raise RuntimeError(f"RDS query failed after {max_retries} attempts") from e
                time.sleep(3 * attempt)
        return []

    def check_query_status(self, query_id, sleep_time=120):
        """
        Checks the status of a Redshift query.
        """
        query_status = 'SUBMITTED'
        response_status = {}
        while query_status not in ['FINISHED', 'FAILED', 'ABORTED']:
            try:
                response_status = self.redshiftclient.describe_statement(Id=query_id)
                query_status = response_status['Status']
            except Exception as e:
                print(f"Error fetching query status: {e}")
                response_status = {'Status': 'FAILED', 'Error': str(e)}
                query_status = 'FAILED'
                time.sleep(sleep_time)
        return response_status, query_status

    def execute_sp_with_retries(self, sp_name):
        """
        Executes a BMT stored procedure with retries in case of failure.
        """
        retries = 0
        while retries < self.RETRY_LIMIT:
            print(f"Executing SP: {sp_name} (Attempt {retries + 1}/{self.RETRY_LIMIT})")
            try:
                response = self.redshiftclient.execute_statement(
                    ClusterIdentifier=self.clusteridentifier,
                    Database=self.redshiftdatabase,
                    SecretArn=self.redshiftsecret,
                    Sql=f"CALL {sp_name};",
                    WithEvent=True
                )
                query_id = response['Id']
                response_status, query_status = self.check_query_status(query_id)
                print(f"Query status for SP: {sp_name} is {query_status}")

                if query_status == "FAILED":
                    error_msg = response_status.get("Error", "Unknown error")
                    print(f"SP {sp_name} failed on attempt {retries + 1}: {error_msg}")
                    retries += 1

                    if retries < self.RETRY_LIMIT:
                        print(f"Retrying SP {sp_name} ({retries}/{self.RETRY_LIMIT}) after waiting 2 minutes...")
                        time.sleep(120)
                        continue
                    else:
                        print(f"SP {sp_name} failed after {self.RETRY_LIMIT} attempts.")
                        self.failed_sp_entry = {
                            "Stored Procedure Name": sp_name,
                            "Error": error_msg,
                            "Data Source": "BMT",
                            "Region": self.reportregionname
                        }
                        self.error_message = error_msg.split(":", 1)[-1] if ":" in error_msg else error_msg
                        return False
                else:
                    print(f"SP {sp_name} executed successfully.")
                    self.error_message = "Null"
                    return True
            except Exception as e:
                print(f"Error executing SP {sp_name}: {e}")
                retries += 1
        print(f"SP {sp_name} failed after {self.RETRY_LIMIT} retries.")
        return False

    def main(self):
        """
        Glue Job main function for BMT Report Refresh orchestration.
        """
        try:
            print(f"Starting BMT Report Refresh Job in {self.env} - {self.reportregionname}...")
            self.update_audit_log('InProgress', is_initial=True)
            stored_procedures = self.fetch_pending_sps()

            if not stored_procedures:
                print(f"No pending BMT stored procedures found in {self.env} - {self.reportregionname}. Exiting job.")
                self.send_sns_message("Succeeded")
                return True

            print(f"Found {len(stored_procedures)} BMT stored procedures to execute.")

            for sp_name in stored_procedures:
                print(f"Processing SP: {sp_name} in {self.env} - {self.reportregionname}...")
                success = self.execute_sp_with_retries(sp_name)

                if not success:
                    print(f"SP {sp_name} failed after {self.RETRY_LIMIT} attempts. Stopping job.")
                    self.update_audit_log("Failed", execution_status="Failed", is_initial=False)
                    self.send_sns_message("Failed", [self.failed_sp_entry])
                    raise Exception(f"SP {sp_name} failed after {self.RETRY_LIMIT} retries. Job terminating.")

            # If all SPs succeed
            self.update_audit_log("Completed", execution_status="Finished", is_initial=False)
            print(f"All BMT stored procedures executed successfully.")
            self.send_sns_message("Succeeded")

            return True
        except Exception as e:
            print(f"Unexpected error in main: {e} (Env: {self.env}, Region: {self.reportregionname})")
            self.update_audit_log("Failed", execution_status="Failed", is_initial=False)
            self.send_sns_message("Failed", ["Unexpected error"])
            raise

if __name__ == "__main__":
    job = BMTReportRefresh()
    job.main()
