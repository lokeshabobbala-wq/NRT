#PROD BMT AMS GLUE : sc360-BMT-reportrefresh-prod-ams
# Job: sc360-BMT-reportrefresh-dev-ams


from awsglue.utils import getResolvedOptions
import json
import json
import os
from multiprocessing import Process, JoinableQueue
import sys
import traceback
from datetime import datetime
from datetime import timedelta
from datetime import date
import time
import ast
import sys
import boto3



def send_sns_message(env,Error_Dict,status,regionname):
    
    sns_message = {
                   "Env": env,
                   "Error Message": Error_Dict,
                   "status": status,
                   "Region":regionname,
                   "Glue_Job_name": Job_name,
                    "Log_Group": loggroupname,
                    "Log_Stream_ID": job_run_id
                   }
    if len(Error_Dict) > 0:
        sns_subject = regionname + ' BMT Report Refresh Failed'
    else:
        sns_subject = regionname + ' BMT Report Refresh Successful'
    sns = boto3.client('sns')
    snsarn = args['sns_arn']
    snsMessage = json.dumps(sns_message)
    sns.publish(
        TargetArn=snsarn,
        Message=snsMessage,
        Subject=sns_subject
    )
    
    if len(Error_Dict) ==0:
        sns_message = {
                   "Env": env,
                   "Message": regionname+ ' BMT Report Refresh is Completed Successfully.',
                   "Report_Url": args['report_url']

        }
        sns_subject = regionname+ ' BMT Report Refresh Successful'
        sns = boto3.client('sns')
        snsarn = args['users_sns_arn']
        snsMessage = json.dumps(sns_message)
        sns.publish(
            TargetArn=snsarn,
            Message=snsMessage,
            Subject=sns_subject
        )    


if __name__ == "__main__":
    # TODO implement
    todaysDate = date.today()
    BatchRunDate = todaysDate 
    args = getResolvedOptions(sys.argv, 
            [
            'JOB_NAME','env','sns_arn','reportregionname',  'resourcearn','secretarn','database','schema','clusteridentifier',
            'redshiftdatabase','redshiftuser','redshiftsecret','users_sns_arn','report_url','priority_File_recieved'
            ]
        )
    
    Job_name = args['JOB_NAME']
    loggroupname = '/aws-glue/jobs/output'
    job_run_id = args['JOB_RUN_ID']
    reportregionname = args['reportregionname']
    # report_datasource = args['report_datasource']
    resourcearn = args['resourcearn']
    secretarn = args['secretarn']
    database = args['database']
    schema = args['schema']
    clusteridentifier = args['clusteridentifier']
    redshiftdatabase = args['redshiftdatabase']
    redshiftuser = args['redshiftuser']
    redshiftsecret = args['redshiftsecret']
    priority_File_Flag = args['priority_File_recieved']
    # day_identifier = args['day_identifier']
    spname  = 'null'
    status= 'null'
    error= 'null'
    env = args['env']
    ct = datetime.now()
    log_time= str(ct)
    order = []
    d = datetime.utcnow()
    
    rds_client = boto3.client('rds-data')
    response_data_source = rds_client.execute_statement(
                    resourceArn=resourcearn,
                    secretArn=secretarn,
                    database=database,
                    sql="""update audit.Master_Data_For_IRR set actual_start_time = '{1}',status='InProgress' where regionname = '{0}' and identifier = 'BMT' 
                    ;""".format(reportregionname,d)
                    )
    print('status updated')
    response_data = rds_client.execute_statement(
            resourceArn=resourcearn,
            secretArn=secretarn,
            database=database,
            sql="""select exec_order from audit.sc360_reportrefresh where region = '{0}' and file_datasource like '%{1}%' and  codebase = '{2}';""".format(reportregionname,'BMT',priority_File_Flag)
            )
    
    for row in response_data['records']:
        order.append( float(row[0]['stringValue']) )
    orderlist = []

    for i in order:
        orderlist.append(i)
    orderlist.sort()
    failedsps = []
    execution_status = 'Finished'
    for orderID in orderlist:
        try:
            response_data = rds_client.execute_statement(
                resourceArn=resourcearn,
                secretArn=secretarn,
                database=database,
                sql="""select stored_procedure_name from audit.sc360_reportrefresh where exec_order = '{0}' and region = '{1}' 
                and file_datasource like '%{2}%' and codebase = '{3}'  ;""".format(orderID,reportregionname,'BMT',priority_File_Flag)
                )
    
            curspname = response_data['records'][0][0]['stringValue']
            query = 'call ' + curspname 
            print("Working on Executing:",query)
            redshiftclient = boto3.client('redshift-data')
            
            response =  redshiftclient.execute_statement(
                    ClusterIdentifier=clusteridentifier,
                    Database= redshiftdatabase,
                    SecretArn=redshiftsecret,
                    Sql=query,
                    WithEvent=True
                )
            
            query_id = response['Id']
            print("Query Response id",query_id)
            response_describe = redshiftclient.describe_statement(
                    Id=query_id
                    )
            query_status = response_describe['Status']
            
            print("query_status",query_status)
            query_status = 'SUBMITTED'
            while query_status not in ['FINISHED','FAILED','ABORTED']:
                try:
                    response_describe = redshiftclient.describe_statement(Id=query_id)
                except Exception as e:  #in case of connection failure
                    print('Exception ocurred while trying to get status of query, waiting for 2 min', str(e))
                    time.sleep(120)
                query_status = response_describe['Status']
                
                
            print("Status after executing", query_status)    
            if query_status == 'FAILED':
                execution_status = 'Failed'
                Error_Message = str(response_describe['Error'])
                print("Received Error after executing the statement",Error_Message)
                print('Error message',Error_Message)
                
                x=Error_Message.split(':')
                print('My error message :- ',x[1])
                em=str(x[1])
                failedsps.append({"Stored Procedure Name":query,"Error":Error_Message,"Data Source":'BMT',"Region":reportregionname})
                stat = 'Failed'
                break
            else:
                em ='NULL'
                stat = 'Completed'
                pass
                        
        except Exception as e:
            error = str(e)
            execution_status = 'Failed'
            print('Error occured-->',error)
            em='error'
            stat = 'Failed'
    
    print('Updating the report refresh status as',execution_status)
    d = datetime.utcnow()
    response_data_source = rds_client.execute_statement(
                    resourceArn=resourcearn,
                    secretArn=secretarn,
                    database=database,
                    sql="""update audit.sc360_reportrefreshtrigger_log set execution_status = '{0}',actual_end_time = '{3}', error_message = '{4}' where batchrundate = '{1}' and regionname = '{2}' and report_source = 'BMT' 
                    ;""".format(execution_status,BatchRunDate,reportregionname,d,em)
                )
    response_data_source = rds_client.execute_statement(
                    resourceArn=resourcearn,
                    secretArn=secretarn,
                    database=database,
                    sql="""update audit.Master_Data_For_IRR set status = '{1}' where regionname = '{0}' and identifier = 'BMT' 
                    ;""".format(reportregionname,stat)
                    )
    if len(failedsps) > 0:
        print("One or more Report Refresh SPs failed")
        send_sns_message(env,failedsps, "Failed",reportregionname)
    else:
        send_sns_message(env,failedsps,"Succeeded",reportregionname)
