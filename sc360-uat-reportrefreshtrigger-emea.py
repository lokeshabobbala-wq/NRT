#PROD BMT EMEA LAMBDA: sc360-EMEA-Reportrefresh-prod-t7cftJIf2RZX
import json
import os
from multiprocessing import Process, JoinableQueue
import sys
import traceback
from datetime import datetime
from datetime import date
from datetime import timedelta
import boto3
import datetime
import time
import ast
import psycopg2
import re
import calendar
from time import gmtime, strftime


def send_sns_message(env,missing_file,process_name,region):
    loggroupname = os.environ['AWS_LAMBDA_LOG_GROUP_NAME']
    Logstream = os.environ['AWS_LAMBDA_LOG_STREAM_NAME']
    
    sns_message = {
                   "Env": env,
                   "Missing Priority File": missing_file,
                   "Process Name failed, If any": process_name,
                   "Region": region,
                   "Lambda_Name": os.environ['AWS_LAMBDA_FUNCTION_NAME'],
                   "Log Group": loggroupname,
                   "CloudWatch_Logstream": Logstream
            }
            
    print("inside sns function")
    sns_subject = '*** '+region + 'BMT Priority File Missing ***'
    sns = boto3.client('sns')
    snsarn = os.environ['sns_arn']
    snsMessage = json.dumps(sns_message)
    sns.publish(
        TargetArn=snsarn,
        Message=snsMessage,
        Subject=sns_subject
    )
    print("msg sent")
    
def send_sns_message_failed(env,missing_file,process_name,region):
    loggroupname = os.environ['AWS_LAMBDA_LOG_GROUP_NAME']
    Logstream = os.environ['AWS_LAMBDA_LOG_STREAM_NAME']
    
    sns_message = {
                   "Env": env,
                   "Priority File Failed": missing_file,
                   "Process Name failed, If any": process_name,
                   "Region": region,
                   "Lambda_Name": os.environ['AWS_LAMBDA_FUNCTION_NAME'],
                   "Log Group": loggroupname,
                   "CloudWatch_Logstream": Logstream
            }
    print("inside sns function")
    sns_subject = '*** '+region + 'BMT Priority File Failure ***'
    sns = boto3.client('sns')
    snsarn = os.environ['sns_arn']
    snsMessage = json.dumps(sns_message)
    sns.publish(
        TargetArn=snsarn,
        Message=snsMessage,
        Subject=sns_subject
    )
    print("msg sent")

def lambda_handler(event, context):
    d = datetime.datetime.utcnow()
    d_now = datetime.datetime.now()
    env = os.environ['env']
    
    dependent_job1 = os.environ['dependent_job1']
    dependent_job2 = os.environ['dependent_job2']
    extraHour = os.environ['extraHour']
    extraMin = os.environ['extraMin']
    cutoff_strt_hour = os.environ['cutoff_strt_hour']
    cutoff_strt_min = os.environ['cutoff_strt_minute']
    cutoff_end_hour = os.environ['cutoff_end_hour']
    cutoff_end_min = os.environ['cutoff_end_minute']
    glue_job = os.environ['glue_job']
    
    glueclient = boto3.client('glue')
    reportregion = os.environ['reportregion']
    todaysDate = date.today()
    BatchRunDate = todaysDate
    em = ''
    #RDS variables
    #Redshift Variables
    
    redshift_secret_name = os.environ['redshift_secret_name']
    rds_secret_name = os.environ['rds_secret_name']
    region_name = "us-east-1"
    secrets_client = boto3.client('secretsmanager', region_name=region_name)

    redshift_conn_string = ""
    # Get the secret details
    response = secrets_client.get_secret_value(
        SecretId=redshift_secret_name
    )

    # Get the secret values
    
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print("redshift if")
        redshift_database = ast.literal_eval(response['SecretString'])['redshift_database']
        redshift_port = ast.literal_eval(response['SecretString'])['redshift_port']
        redshift_username = ast.literal_eval(response['SecretString'])['redshift_username']
        redshift_password = ast.literal_eval(response['SecretString'])['redshift_password']
        redshift_host = ast.literal_eval(response['SecretString'])['redshift_host']

        redshift_conn_string = "dbname='" + redshift_database + "' port='" + str(
            redshift_port) + "' user='" + redshift_username + "' password='" + redshift_password + "' host='" + redshift_host + "'"
        
    else:
        print("Not Able to extract Credentials for Redshift Connections")
        sys.exit("Not Able to extract Credentials for Redshift Connections")

    rds_conn_string = ""
    # Get the RDS secret details
    response = secrets_client.get_secret_value(
        SecretId=rds_secret_name
    )
    
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print("rds if")
        rds_database = ast.literal_eval(response['SecretString'])['engine']
        rds_port = ast.literal_eval(response['SecretString'])['port']
        rds_username = ast.literal_eval(response['SecretString'])['username']
        rds_password = ast.literal_eval(response['SecretString'])['password']
        rds_host = ast.literal_eval(response['SecretString'])['host']
        # rds_region = ast.literal_eval(response['SecretString'])['rds_region']

        rds_conn_string = "dbname='" + rds_database + "' port='" + str(
            rds_port) + "' user='" + rds_username + "' password='" + rds_password + "' host='" + rds_host + "'"
        
    else:
        print("Not Able to extract Credentials for RDS Connections")
        sys.exit("Not Able to extract Credentials for RDS Connections")

    #calculation of start & end timings 
    rds_connection = psycopg2.connect(rds_conn_string)
    rds_cursor = rds_connection.cursor()
    # To  update the table 
    print("update1")
    start_now = str(date.today()) 
    rds_cursor.execute("""select Expected_Start_time from audit.Master_Data_For_Report_Refresh where regionname = '{0}' 
    and report_source = 'BMT';""".format(reportregion))
    expectedstart = rds_cursor.fetchall() 
    expectedstart1=expectedstart[0][0]
    print("expectedstart - ", expectedstart1) 
    print(start_now) 
    # To update expected_end_time using average run time 
    rds_cursor.execute("""select Average_runtime from audit.Master_Data_For_Report_Refresh where regionname = '{0}' 
    and report_source = 'BMT';""".format(reportregion))
    averagerun = rds_cursor.fetchall()
    averagerun1=averagerun[0][0]
    print("averagerun - ", averagerun1) 
    x=expectedstart1
    date_format_str= '%H:%M:%S'
    x=str(x)
    y = datetime.datetime.strptime(x, date_format_str)
    final_time =y + timedelta(minutes=averagerun1)
    print('final time',final_time) 
    result = str(final_time)
    end_time=result[11::]
    print(end_time) # 12:10:00
    join_start=" ".join([start_now, str(expectedstart1)])
    f = "%Y-%m-%d %H:%M:%S"
    join_start = datetime.datetime.strptime(join_start, f)
    print('join_start',join_start)
    print(type(join_start))
    join_end=" ".join([start_now, end_time])
    f = "%Y-%m-%d %H:%M:%S"
    join_end = datetime.datetime.strptime(join_end, f)

    # To check table entry
    rds_connection = psycopg2.connect(rds_conn_string)
    rds_cursor = rds_connection.cursor()
    rds_cursor.execute("""select count(*) from audit.sc360_reportrefreshtrigger_log where regionname = '{0}' and batchrundate = '{1}'
    and report_source = 'BMT';""".format(reportregion,BatchRunDate))
    E = rds_cursor.fetchall()
    if E[0][0]>0:
        Refreshcount = E[0][0]
        rds_cursor.execute("""select execution_status from audit.sc360_reportrefreshtrigger_log where regionname = '{1}' and batchrundate = '{0}'
        and report_source = 'BMT';""".format(BatchRunDate,reportregion))
        execution_status = rds_cursor.fetchall()
        print('execution_status - ',execution_status[0][0])
        print("Refreshcount",Refreshcount)
        if Refreshcount != 0 and len(execution_status)>0 and execution_status[0][0] in('Finished','Submitted'):
            e = "Report Refresh Triggered for the day already : " + str(BatchRunDate)
            sys.exit(e)
        
        
        dependent_glue_response1 = glueclient.get_job_runs(
            JobName=dependent_job1        
            )
        status_dependent_job1 = dependent_glue_response1['JobRuns'][0]['JobRunState']
        print("status_dependent_job1",status_dependent_job1)
        
        dependent_glue_response2 = glueclient.get_job_runs(
            JobName=dependent_job2
            )
        status_dependent_job2 = dependent_glue_response2['JobRuns'][0]['JobRunState']
        print("status_dependent_job2",status_dependent_job2)
        #TO UPDATE THE EXPECTED START & STATUS OF 'INPROGRESS' & 'DELAY' FOR REPORTS
        rds_cursor.execute("""select report_name,report_refresh_frequency,expected_start_time,average_runtime,status,date(actual_start_time)  from audit.Master_Data_For_IRR
        where  regionname  = '{0}' ;""".format(reportregion))
        fetch_results = rds_cursor.fetchall()
        print('fetch_results ',fetch_results)
        for row in fetch_results:
            report_name = row[0]
            frequency = row[1]
            expect_time = row[2]
            averagerun = row[3]
            status = row[4]
            actual_start =row[5]
            print('report_name ',report_name)
            print('frequency ',frequency)
            print('expect_time ',expect_time)
            print(type(expect_time))
            print('averagerun ',averagerun) 
            print(type(averagerun))
            print('actual_start ',actual_start)
            start_now = str(date.today())
            date_format_str= '%H:%M:%S'
            x1=str(expect_time)
            y1 = datetime.datetime.strptime(x1, date_format_str)
            final_time1 =y1 + timedelta(minutes=averagerun)
            print('final time',final_time1) # final time 1900-01-01 12:10:00
            result1 = str(final_time1)
            end_time1=result1[11::]
            print(end_time1)
            current_time = datetime.datetime.now()
            if frequency in ('Monthly','Quarterly','Weekly','Yearly'):
                print('yes')
                rds_cursor.execute("""select File_arrival_cutoff_datetime from audit.Master_Data_For_IRR
                where  report_name  = '{0}' and regionname  = '{1}';""".format(report_name,reportregion))
                file_cuttoff= rds_cursor.fetchall()
                print('file_cuttoff',file_cuttoff)
                print(type(file_cuttoff))
                cut = file_cuttoff[0]
                print(cut)
                print(type(cut))
                cut1=cut[0]
                cutoffdate1=cut1.split()
                cutoffdate2=cutoffdate1[0]
                print('cutoffdate2',cutoffdate2)
                join_end1=" ".join([cutoffdate2, end_time1])
                f = "%Y-%m-%d %H:%M:%S"
                join_end1 = datetime.datetime.strptime(join_end1, f)
                join_end1=" ".join([cutoffdate2, end_time1])
                f = "%Y-%m-%d %H:%M:%S"
                join_end1 = datetime.datetime.strptime(join_end1, f) # use in table
                print('join_end',join_end1)
                print(type(join_end1))
                expect_sdt=" ".join([cutoffdate2, str(expect_time)])
                f = "%Y-%m-%d %H:%M:%S"
                expect_sdt = datetime.datetime.strptime(expect_sdt, f) # use in table
                print('join_start',expect_sdt)
                print(type(expect_sdt))
            else:
                join_end1=" ".join([start_now, end_time1])
                f = "%Y-%m-%d %H:%M:%S"
                join_end1 = datetime.datetime.strptime(join_end1, f)
                join_end1=" ".join([start_now, end_time1])
                f = "%Y-%m-%d %H:%M:%S"
                join_end1 = datetime.datetime.strptime(join_end1, f) # use in table
                print('join_end',join_end1)
                print(type(join_end1))
                expect_sdt=" ".join([start_now, str(expect_time)])
                f = "%Y-%m-%d %H:%M:%S"
                expect_sdt = datetime.datetime.strptime(expect_sdt, f) # use in table
                print('join_start',expect_sdt)
                print(type(expect_sdt))
                
            rds_cursor.execute("""update audit.Master_Data_For_IRR set expected_start = '{2}', expected_end ='{3}'
            where  report_name  = '{0}' and regionname  = '{1}';""".format(report_name,reportregion,expect_sdt,join_end1))
            rds_connection.commit()
            print('Report table update with expected start and end time')  
            rds_cursor.execute("""select date(expected_start) from audit.Master_Data_For_IRR  where 
            report_name  = '{0}' and regionname  = '{1}';""".format(report_name,reportregion))
            expectstart = rds_cursor.fetchall()
            expectstart=expectstart[0][0]
            print('actual_start',actual_start)
            print('expectstart',expectstart)
            print('current_time',current_time)
            print('expect_sdt',expect_sdt)
            print('status',status)
            if actual_start !=[]:
                if actual_start == expectstart:
                    if current_time <= expect_sdt and status == 'Null':
                        print('Yet to start')
                        status ='Yet to start'
                    elif current_time <= expect_sdt and status == 'Completed':
                        print('Completed')
                        status ='Completed'
                    elif current_time >= expect_sdt and status != 'Completed':
                        print('Delay')
                        status ='Delay'
                    elif current_time >= expect_sdt and status == 'Completed':
                        print('Completed')
                        status ='Completed'
                else:
                    status ='Null'
            else:
                status ='Null'
            rds_cursor.execute("""update audit.Master_Data_For_IRR set status='{2}'
            where  report_name  = '{0}' and regionname  = '{1}';""".format(report_name,reportregion,status))
            rds_connection.commit()
            print('Report table update with status')  
            #################################################3     
        if status_dependent_job2 in ['STARTING','RUNNING','STOPPING'] or status_dependent_job1 in  ['STARTING','RUNNING','STOPPING']:
             if d > join_start:
                rds_cursor.execute("""update audit.sc360_reportrefreshtrigger_log
                set  execution_status = 'Delay', error_message = 'Other Region Report Refresh is already running'
                where batchrundate = '{0}' and regionname = '{1}' and report_source = 'BMT';""" .format(BatchRunDate, reportregion))
                rds_connection.commit()
             sys.exit("Other Region Report Refresh is already running")
        
        print("No other Glue Jobs are running")
        
        if int(d.hour) >= (int(cutoff_strt_hour) + int(extraHour)) and int(d.minute) >= (int(cutoff_strt_min) + int(extraMin)):
            print("Not checking Priority Files as Lambda already ran for maximum time allowed after cutoff time, proceeding to process report refresh")
            region = reportregion
            sns_message = {
                           "Env": env,
                           "Message": 'The BMT ' + region + ' Report refresh is started. Open glue job log : '+ str(glue_job),
                           "Region": region,
                           "Lambda_Name": os.environ['AWS_LAMBDA_FUNCTION_NAME'],
                           "Log Group": os.environ['AWS_LAMBDA_LOG_GROUP_NAME'],
                           "CloudWatch_Logstream": os.environ['AWS_LAMBDA_LOG_STREAM_NAME']
                    }
                    
            print("inside sns function")
            sns_subject = '*** '+region + ' BMT report refresh is started without Priority files.***'
            sns = boto3.client('sns')
            snsarn = os.environ['sns_arn']
            snsMessage = json.dumps(sns_message)
            sns.publish(
                TargetArn=snsarn,
                Message=snsMessage,
                Subject=sns_subject
            )    
            
            response = glueclient.start_job_run(
              JobName=glue_job,
                Arguments={
                    '--priority_File_checked': 'N',
                    '--reportregionname': reportregion,
                    '--identifier': 'BMT'
                }
            )
             
            #once glue job starts it will update execution satus as submitted, glue job name and error msg
            rds_cursor.execute("""update audit.sc360_reportrefreshtrigger_log
            set Actual_Start_time='{2}', execution_status = 'Submitted', error_message = 'NULL'
            where batchrundate = '{0}' and regionname = '{1}' and report_source = 'BMT';""" .format(BatchRunDate,reportregion,d))
            rds_connection.commit()
            print("glue job started")
        
        else :
        
            rds_cursor.execute("""select filename from audit.fileproperty_check_new where 
                priorityflag = 'YES' and region = '{0}' and data_source = 'BMT';""".format(reportregion))
            priorityfiles = rds_cursor.fetchall()
        
            print("priorityfiles",priorityfiles)
            process_name = "NA"
            rds_cursor.execute("""select distinct filename  from audit.sc360_audit_log sal
                where batchrundate = '{0}' and regionname  = '{1}' and processname  = 'RedshiftCuratedLoad' and
                executionstatus = 'Succeeded' and filename in (select filename from audit.fileproperty_check_new where 
                priorityflag = 'YES' and region = '{1}' and data_source = 'BMT');""".format(BatchRunDate, reportregion))
            filesloaded_curated = rds_cursor.fetchall()
            print("filesloaded_curated",filesloaded_curated)

            loadedcuratedfiles = []
            for loadedfile in filesloaded_curated:
                loadedcuratedfiles.append(loadedfile[0])
            print("Loaded files in curated", loadedcuratedfiles )
        
            # get the execution status for the procedure of each files.
            procs_list = []
            for loadedfile in loadedcuratedfiles:
                rds_cursor.execute("""select distinct stored_procedure_name  from audit.sps_batch_master_table_updated  
                                where regionname  = '{0}' and source_filename like '%{1}%';""".format(reportregion, loadedfile))

                file_stored_procs = rds_cursor.fetchall()
                if len(file_stored_procs) != 0:
                    procs_list.append([loadedfile,file_stored_procs[0][0]])
        

            print('File stored procedure names = ', procs_list)
            final_files = []
            for procs in procs_list:
                x = re.findall(" \(\);$", procs[1])
                if x:
                    pass
                else:
                    each_Sps1 = procs[1].replace("('","(''")
                    each_Sps2 = each_Sps1.replace("')","'')")
                    procs[1] = each_Sps2   # to convert to PUBLISHED.SP_CURTOPUB_R_REVENUE_EGI (''EMEA'')
                  
                rds_cursor.execute("""select count(*)  from audit.sc360_audit_log
                    where batchrundate = '{0}' and regionname  = '{1}' and processname  = 'RedshiftPublishedLoad' and
                    executionstatus = 'Succeeded' and scriptpath = '{2}';""".format(BatchRunDate, reportregion, procs[1],procs[0]))
                final_load_files_list = rds_cursor.fetchall()
                if final_load_files_list[0][0]> 0:
                    final_files.append(procs[0])
            print('Final Files List Loaded to published',final_files)
            loadedPublishedFiles = final_files

            filesnotreceived = []
            filesfailed = []
            pfcount = 0
            failedprocess = []
            for pfile in priorityfiles:
                if pfile[0] in loadedPublishedFiles: 
                    print("Priority File loaded till Published", pfile[0])
                    pfcount += 1
                    print("pfcount",pfcount)
                else:
                    print("Priority File missing", pfile[0] )
                    rds_cursor.execute("""select count(*) from audit.sc360_audit_log where filename = '{0}' and processname = 'DataValidation' 
                        and errormessage like '%list index out of rangeException caught while converting the SCITS file to relational in scits_data_conversion_to_relational function.%'
                        and batchrundate = '{1}' and sourcename like '%.dat';""".format(pfile[0],BatchRunDate))
                
                    sourcefilecount = rds_cursor.fetchall()
                    if sourcefilecount[0][0] >0:
                        print('This scits is empty from source and has no data records')
                        pfcount += 1
                        continue
                    else:
                        pass    # file not received
                
                    s3client = boto3.client('s3')
                    landingBucketName = 'sc360-' + env + '-' + reportregion.lower() + '-bucket'
                    landingFolder = 'LandingZone/dt=' + str(BatchRunDate) + '/'
                    all_objects = s3client.list_objects_v2(Bucket=landingBucketName, Prefix=landingFolder, MaxKeys=350)
                    LandingdataFileNameList = []
                    try:
                        for obj in all_objects['Contents']:
                            filePath = obj['Key']
                            completeFileName = filePath.split('/')[-1]
                            if len(completeFileName) > 0 and not (completeFileName.startswith('SC360metadata_')):
                                LandingdataFileNameList.append(completeFileName)
                            else:
                                continue
                    except Exception as e:
                        pass

                    archiveFolder = 'ArchiveZone/dt=' + str(BatchRunDate) + '/'
                    all_objects = s3client.list_objects_v2(Bucket=landingBucketName, Prefix=archiveFolder, MaxKeys=350)
                    ArchivedataFileNameList = []
                    try:
                        for obj in all_objects['Contents']:
                            filePath = obj['Key']
                            completeFileName = filePath.split('/')[-1]
                            if len(completeFileName) > 0 and not (completeFileName.startswith('SC360metadata_')):
                                ArchivedataFileNameList.append(completeFileName)
                            else:
                                continue
                    except Exception as e:
                        pass
                
                    filereceivedflag = 0
                    for filename in LandingdataFileNameList:
                        if pfile[0] in filename:
                            print('This file is present in landingzone')
                            filereceivedflag = 1
                        else:
                            continue
                    
                    for filename in ArchivedataFileNameList:
                        if pfile[0] in filename:
                            print('This file is present in ArchiveZone')
                            filereceivedflag = 1
                        else:
                            continue

                    rds_cursor.execute("""select distinct processname  from audit.sc360_audit_log where batchrundate = '{0}' and executionstatus  = 'Failed'
                    and filename = '{1}'  ;""".format(BatchRunDate, pfile[0]))
                    failedprocesses = rds_cursor.fetchall()
                
                    for process in failedprocesses:
                        failedprocess.append(process[0])
                
                    if len(failedprocesses) == 0:
                        if filereceivedflag == 0:
                            print("priority File not received yet", pfile[0])
                            filesnotreceived.append(pfile[0])
                            s= str(filesnotreceived)
                            em= s
                            print('em',em)
                            em1= em.replace("['","")
                            print('em1',em1)
                            em2= em1.replace("']","")
                            print('em2',em2)
                            em3=em2.replace("', '",",")
                            print('em3',em3)
                            em= em3
                            print('em',em)
                        else:
                            print("priority File received but yet to complete loading process ", pfile[0])
                    else:
                        print("priority failed at", failedprocesses)
                        filesfailed.append({pfile[0]:failedprocesses})
                        s= str(pfile[0])
                        em= s
                        print('em',em)
                        em1= em.replace("['","")
                        print('em1',em1)
                        em2= em1.replace("']","")
                        print('em2',em2)
                        em3=em2.replace("', '",",")
                        print('em3',em3)
                        em= em3
                        print('em',em)

            print("pfcount",pfcount)
        
            print("glue_job:",glue_job)
            print("d.hour",d.hour) #7
            print("cutoff_strt_hour",cutoff_strt_hour) #7
            print("d.minute",d.minute) #20
            print("cutoff_strt_min", cutoff_strt_min) #15
            print("cutoff_end_hour", cutoff_end_hour) #7
        
            if (int(d.hour) >= int(cutoff_strt_hour) and int(d.minute) >= int(cutoff_strt_min)) and int(d.hour) <= int(cutoff_end_hour):
                snstimeperiod = 'YES'
            else:
                snstimeperiod = 'NO'
            
            print("SNS period",snstimeperiod)
            print('pfcount ', pfcount)
            print('len(priorityfiles)', len(priorityfiles))
            print('File Not received', filesnotreceived)
            print('File Failes', filesfailed)
        
        ########################################################################
                        
            if pfcount >= len(priorityfiles):
                print("Priority Files Received, proceeding to process report refresh")
            
                region = reportregion
                sns_message = {
                           "Env": env,
                           "Message": 'The BMT ' + region + ' Report refresh is started. Open glue job log : '+ str(glue_job),
                           "Region": region,
                           "Lambda_Name": os.environ['AWS_LAMBDA_FUNCTION_NAME'],
                           "Log Group": os.environ['AWS_LAMBDA_LOG_GROUP_NAME'],
                           "CloudWatch_Logstream": os.environ['AWS_LAMBDA_LOG_STREAM_NAME']
                    }
                
                sns_subject = '*** '+region + ' BMT report refresh is started.***'
                sns = boto3.client('sns')
                snsarn = os.environ['sns_arn']
                snsMessage = json.dumps(sns_message)
                sns.publish(
                    TargetArn=snsarn,
                    Message=snsMessage,
                    Subject=sns_subject
                )    
            
            
                response = glueclient.start_job_run(
                    JobName=glue_job,
                    Arguments={
                    '--priority_File_checked': 'Y',
                    '--reportregionname': reportregion,
                    '--identifier': 'BMT'
                    }
                )
             
                #once glue job starts it will update execution satus as submitted and glue job name , error msg
                rds_cursor.execute("""update audit.sc360_reportrefreshtrigger_log
                    set Actual_Start_time='{2}', execution_status = 'Submitted',error_message = 'NULL'
                    where batchrundate = '{0}' and regionname = '{1}' and report_source = 'BMT';""" .format(BatchRunDate,reportregion,d))
                rds_connection.commit()
                print("glue job ")
        
            elif (pfcount != len(priorityfiles)) and snstimeperiod =='YES' :
                print("Cut off time Reached and priority files not received")
                if d > join_start:
                    msg= "Cut off time Reached and priority files not received" 
                    em =  str(em)
                    error_msg = msg + em
                    print('error_message',error_msg) 
                    print(type(error_msg))
                    rds_cursor.execute("""update audit.sc360_reportrefreshtrigger_log
                    set error_message ='{2}' , execution_status = 'Delay'
                    where batchrundate = '{0}' and regionname = '{1}' and report_source = 'BMT';""" .format(BatchRunDate, reportregion,error_msg))
                    rds_connection.commit()
                    print('cutoff')
                if len(filesnotreceived) > 0:
                    region = reportregion
                    process_name = 'Files Not Received'
                    send_sns_message(env,filesnotreceived,process_name,region)
                
                if len(filesfailed) > 0:
                    region = reportregion
                    process_name = 'NA'
                    print("sending sns")
                    print(filesfailed)
                    send_sns_message_failed(env,filesfailed,'NA',region)    
        
            else:
                print("Priority Files Not rerceived yet, Monitoring the Load Status")
                if d > join_start:
                    msg= "PF Missing " 
                    em =  str(em)
                    error_msg = msg + em
                    print('error_message',error_msg) 
                    print(type(error_msg))
                    rds_cursor.execute("""update audit.sc360_reportrefreshtrigger_log
                        set  execution_status = 'Delay', error_message = '{2}'
                        where batchrundate = '{0}' and regionname = '{1}' and report_source = 'BMT';""" .format(BatchRunDate, reportregion,error_msg))
                    rds_connection.commit()
                    rds_cursor.execute("""select error_message,execution_status  from audit.sc360_reportrefreshtrigger_log 
                        where batchrundate = '{0}' and regionname = '{1}' and report_source = 'BMT';""" .format(BatchRunDate, reportregion))
                    z= rds_cursor.fetchall() 
                    print('z',z)
    else:
             
        # To insert values into  table

        execution_status = 'Yet to start'
        glue_job = os.environ['glue_job']
        error_message = 'BMT report refresh yet to start'
        rds_insert_query = """
        INSERT INTO audit.sc360_reportrefreshtrigger_log(
        batchrundate, regionname, execution_status,gluejob, report_source,Expected_Start_time,Expected_End_time,error_message)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
        """
        records_insert = (
        BatchRunDate, reportregion, execution_status,glue_job, 'BMT',join_start,join_end,error_message)

        rds_cursor.execute(rds_insert_query, records_insert)
        rds_connection.commit()
        print("Inserted")
        
        #######################################################################################################################
        #######################################################################################################################
