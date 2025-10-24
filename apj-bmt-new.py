"""
AWS Lambda Function: BMT Report Refresh Orchestrator (APJ) - optimized, SonarQube-friendly

This Lambda function manages the APJ/BMT report refresh process by:
1. Monitoring arrival of priority files and verifying published-layer completion
2. Validating dependent-region Glue jobs and enforcing start windows (priority-checked vs forced start)
3. Triggering BMT report generation Glue jobs when conditions are satisfied
4. Updating RDS audit tables (sc360_reportrefreshtrigger_log, Master_Data_For_IRR) and sending SNS alerts for success/failure/delay

Dependencies:
- Requires PostgreSQL connections to Redshift and RDS (psycopg2)
- Uses AWS Services: Secrets Manager, Glue, S3, SNS (boto3)
- Environment Variables (required/expected): env, reportregion, glue_job, redshift_secret_name, rds_secret_name, sns_arn,
  cutoff and delay window variables (cutoff_strt_hour/minute, cutoff_end_hour/minute, delay_cutoff_*),
  dependent_job1, dependent_job2, priority_File_recieved (optional), extraHour, extraMinSSSS
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
import psycopg2
from botocore.exceptions import BotoCoreError, ClientError

# Configure logger
logger = logging.getLogger("bmt_report_refresh_apj")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Constants
DEFAULT_AWS_REGION = "us-east-1"
S3_PAGE_SIZE = 1000


# -----------------------
# Utility helpers
# -----------------------
def _aws_region() -> str:
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or DEFAULT_AWS_REGION


def _parse_secret(secret_string: Optional[str]) -> Dict[str, Any]:
    """Parse SecretsManager secret string into dict (JSON preferred, fallback to literal eval)."""
    try:
        if not secret_string:
            return {}
        try:
            return json.loads(secret_string)
        except Exception:
            # ast.literal_eval fallback for legacy secret shapes
            import ast
            parsed = ast.literal_eval(secret_string)
            return dict(parsed) if isinstance(parsed, dict) else {}
    except Exception:
        logger.exception("Error parsing secret string")
        raise


def _ensure_date_str_from_db(value: Any, fallback: Optional[str] = None) -> str:
    """
    Robustly convert DB-returned date/datetime/string to YYYY-MM-DD string.
    If value is None and fallback is None, returns today's date string.
    If parsing fails, returns fallback (or today's date string when fallback is None).
    Accepts date, datetime, or string (ISO / 'YYYY-MM-DD HH:MM:SS' / 'YYYY-MM-DD').
    """
    try:
        if fallback is None:
            fallback = date.today().isoformat()
        if value is None:
            return fallback
        # If it's a date (but not datetime), return isoformat
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.isoformat()
        # If it's a datetime, get the date part
        if isinstance(value, datetime):
            return value.date().isoformat()
        # If it's a string, try parsing common formats
        if isinstance(value, str):
            # Try ISO first (handles 'YYYY-MM-DD' and 'YYYY-MM-DDTHH:MM:SS' etc)
            try:
                parsed = datetime.fromisoformat(value)
                return parsed.date().isoformat()
            except Exception:
                pass
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    parsed = datetime.strptime(value, fmt)
                    return parsed.date().isoformat()
                except Exception:
                    continue
        # If none matched, fallback
        logger.warning("Unable to coerce DB value to date string: %r. Using fallback %s", value, fallback)
        return fallback
    except Exception as exc:
        logger.exception("_ensure_date_str_from_db error: %s", exc)
        return fallback or date.today().isoformat()


def get_db_connection(secret_name: str, region_name: Optional[str] = None):
    """
    Fetch DB credentials from Secrets Manager and return psycopg2 connection.
    Raises RuntimeError on failure.
    """
    try:
        region = region_name or _aws_region()
        client = boto3.client("secretsmanager", region_name=region)
        resp = client.get_secret_value(SecretId=secret_name)
        secret_string = resp.get("SecretString", "")
        creds = _parse_secret(secret_string)

        # support common keys
        dbname = creds.get("redshift_database") or creds.get("engine") or creds.get("dbname")
        port = int(creds.get("redshift_port") or creds.get("port") or 5439)
        user = creds.get("redshift_username") or creds.get("username")
        password = creds.get("redshift_password") or creds.get("password")
        host = creds.get("redshift_host") or creds.get("host")

        if not all([dbname, port, user, password, host]):
            raise KeyError("Missing DB credential fields in secret")

        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        conn.autocommit = False
        logger.info("DB connection established for secret: %s", secret_name)
        return conn
    except (ClientError, BotoCoreError) as exc:
        logger.exception("Secrets Manager error retrieving %s: %s", secret_name, exc)
        raise RuntimeError("Failed to fetch DB secret") from exc
    except Exception:
        logger.exception("Failed to create DB connection for secret: %s", secret_name)
        raise


def publish_sns(sns_client, sns_arn: str, subject: str, message: Dict[str, Any]) -> None:
    """Publish JSON message to SNS; log failures but don't raise to avoid masking main error flows."""
    try:
        resp = sns_client.publish(TargetArn=sns_arn, Subject=subject, Message=json.dumps(message))
        logger.info("SNS published: subject=%s messageId=%s", subject, resp.get("MessageId"))
    except (ClientError, BotoCoreError) as exc:
        logger.exception("Failed to publish SNS subject=%s: %s", subject, exc)


def list_s3_files(s3_client, bucket: str, prefix: str) -> List[str]:
    """List non-metadata file names (file only) under the prefix using paginator."""
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        names: List[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": S3_PAGE_SIZE}):
            for obj in page.get("Contents", []) if page else []:
                key = obj.get("Key", "")
                fname = key.rsplit("/", 1)[-1]
                if fname and not fname.startswith("SC360metadata_"):
                    names.append(fname)
        return names
    except Exception:
        logger.exception("Error listing S3 files for %s/%s", bucket, prefix)
        raise


# -----------------------
# Glue helpers
# -----------------------
def dependent_glue_jobs_running(glue_client, jobs: Iterable[str]) -> bool:
    """Return True if any dependent Glue job is in STARTING/RUNNING/STOPPING state."""
    try:
        for job in jobs:
            if not job:
                continue
            resp = glue_client.get_job_runs(JobName=job, MaxResults=1)
            runs = resp.get("JobRuns") or []
            if not runs:
                logger.debug("No job runs for %s", job)
                continue
            state = runs[0].get("JobRunState")
            logger.info("Dependent job %s state=%s", job, state)
            if state in ("STARTING", "RUNNING", "STOPPING"):
                return True
        return False
    except Exception:
        logger.exception("Error checking dependent glue jobs")
        # conservative - avoid collisions
        return True


def handle_dependent_jobs_check(
    glue_client,
    rds_cursor,
    rds_conn,
    dependent_jobs: Sequence[str],
    current_time: datetime,
    join_start: datetime,
    batch_run_date: date,
    reportregion: str,
) -> bool:
    """
    If dependent jobs are running, optionally update trigger_log to 'Delay' (when past join_start).
    Returns True to indicate the lambda should exit early.
    """
    try:
        if dependent_glue_jobs_running(glue_client, dependent_jobs):
            if current_time > join_start:
                rds_cursor.execute(
                    "UPDATE audit.sc360_reportrefreshtrigger_log SET execution_status=%s, error_message=%s WHERE batchrundate=%s AND regionname=%s AND report_source='BMT';",
                    ("Delay", "Other Region Report Refresh is already running", batch_run_date, reportregion),
                )
                rds_conn.commit()
            logger.info("Dependent region jobs running; exiting.")
            return True
        return False
    except Exception:
        logger.exception("handle_dependent_jobs_check failed")
        try:
            rds_cursor.execute(
                "UPDATE audit.sc360_reportrefreshtrigger_log SET execution_status=%s, error_message=%s WHERE batchrundate=%s AND regionname=%s AND report_source='BMT';",
                ("Delay", "Error checking dependent jobs", batch_run_date, reportregion),
            )
            rds_conn.commit()
        except Exception:
            logger.exception("Failed to update audit when dependent job check failed")
        return True


# -----------------------
# Time window / IRR helpers
# -----------------------
def compute_join_start_end_for_bmt(rds_cursor, reportregion: str) -> Tuple[datetime, datetime, date]:
    """Compute expected start and end datetimes plus batch_run_date for BMT."""
    try:
        rds_cursor.execute(
            "SELECT Expected_Start_time FROM audit.Master_Data_For_Report_Refresh WHERE regionname = %s AND report_source LIKE %s;",
            (reportregion, "%BMT%"),
        )
        row = rds_cursor.fetchone()
        if not row:
            raise RuntimeError(f"Expected_Start_time missing for region {reportregion}")
        expected_time = row[0]

        rds_cursor.execute(
            "SELECT Average_runtime FROM audit.Master_Data_For_Report_Refresh WHERE regionname = %s AND report_source LIKE %s;",
            (reportregion, "%BMT%"),
        )
        avg_row = rds_cursor.fetchone()
        avg_minutes = int(avg_row[0]) if avg_row and avg_row[0] is not None else 0

        batch_run_date = date.today()
        if hasattr(expected_time, "hour"):
            start_dt = datetime.combine(batch_run_date, expected_time)
        else:
            start_dt = datetime.strptime(f"{batch_run_date} {expected_time}", "%Y-%m-%d %H:%M:%S")
        end_dt = start_dt + timedelta(minutes=avg_minutes)
        return start_dt, end_dt, batch_run_date
    except Exception:
        logger.exception("compute_join_start_end_for_bmt failed")
        raise


def update_master_data_for_irr(rds_cursor, rds_conn, reportregion: str) -> None:
    """Refresh expected_start/expected_end and status for Master_Data_For_IRR (legacy semantics preserved)."""
    try:
        rds_cursor.execute(
            "SELECT report_name, report_refresh_frequency, expected_start_time, average_runtime, status, date(actual_start_time) "
            "FROM audit.Master_Data_For_IRR WHERE regionname = %s;",
            (reportregion,),
        )
        rows = rds_cursor.fetchall()
        today_str = date.today().isoformat()
        for report_name, frequency, expect_time, averagerun, status, actual_start in rows:
            averagerun = int(averagerun or 0)
            expect_time_str = str(expect_time)
            expect_dt_base = datetime.strptime(f"{date.today()} {expect_time_str}", "%Y-%m-%d %H:%M:%S")
            if frequency in ("Monthly", "Quarterly", "Weekly", "Yearly"):
                rds_cursor.execute(
                    "SELECT File_arrival_cutoff_datetime FROM audit.Master_Data_For_IRR WHERE report_name = %s AND regionname = %s;",
                    (report_name, reportregion),
                )
                cutoff_row = rds_cursor.fetchone()
                # Use the robust coercion helper to convert DB value to 'YYYY-MM-DD'
                cutoff_date_str = _ensure_date_str_from_db(cutoff_row[0] if cutoff_row else None, fallback=today_str)
                expect_sdt = datetime.strptime(f"{cutoff_date_str} {expect_time_str}", "%Y-%m-%d %H:%M:%S")
                # final end uses the final_time time portion on the cutoff_date
                join_end_dt = expect_sdt + timedelta(minutes=averagerun)
            else:
                expect_sdt = expect_dt_base
                join_end_dt = expect_sdt + timedelta(minutes=averagerun)

            rds_cursor.execute(
                "UPDATE audit.Master_Data_For_IRR SET expected_start=%s, expected_end=%s WHERE report_name=%s AND regionname=%s;",
                (expect_sdt, join_end_dt, report_name, reportregion),
            )
            rds_conn.commit()

            current_time = datetime.now()
            new_status = "Null"
            expect_start_date = expect_sdt.date() if expect_sdt else None

            # Coerce actual_start (which may be date/datetime/string or None)
            actual_start_date = None
            if actual_start:
                # actual_start here comes from SELECT date(actual_start_time) - may be date or string
                actual_start_iso = _ensure_date_str_from_db(actual_start, fallback=None)
                # _ensure_date_str_from_db will return today's string when fallback=None and value is None,
                # but we only call this block when actual_start is truthy, so safe to parse
                try:
                    actual_start_date = datetime.fromisoformat(actual_start_iso).date()
                except Exception:
                    try:
                        actual_start_date = datetime.strptime(actual_start_iso, "%Y-%m-%d").date()
                    except Exception:
                        logger.warning("Could not parse actual_start for report %s: %r", report_name, actual_start)
                        actual_start_date = None

            if actual_start_date:
                if actual_start_date == expect_start_date:
                    if current_time <= expect_sdt and (status is None or status == "Null"):
                        new_status = "Yet to start"
                    elif current_time <= expect_sdt and status == "Completed":
                        new_status = "Completed"
                    elif current_time >= expect_sdt and status != "Completed":
                        new_status = "Delay"
                    elif current_time >= expect_sdt and status == "Completed":
                        new_status = "Completed"
                else:
                    new_status = "Null"
            else:
                new_status = "Null"

            rds_cursor.execute(
                "UPDATE audit.Master_Data_For_IRR SET status=%s WHERE report_name=%s AND regionname=%s;",
                (new_status, report_name, reportregion),
            )
            rds_conn.commit()
            logger.info("IRR updated for %s: status=%s", report_name, new_status)
    except Exception:
        logger.exception("update_master_data_for_irr failed")
        raise


# -----------------------
# Priority-file pipeline helpers
# -----------------------
def fetch_priority_files(rds_cursor, reportregion: str) -> List[str]:
    """Return list of BMT priority filenames from fileproperty_check_new."""
    try:
        rds_cursor.execute(
            "SELECT filename FROM audit.fileproperty_check_new WHERE priorityflag = 'YES' AND region = %s AND data_source LIKE %s;",
            (reportregion, "BMT%"),
        )
        return [row[0] for row in rds_cursor.fetchall()]
    except Exception:
        logger.exception("fetch_priority_files failed")
        raise


def fetch_loaded_curated_files(rds_cursor, reportregion: str, batch_run_date: date, priorityfiles: Sequence[str]) -> List[str]:
    """Return curated-layer filenames with successful RedshiftCuratedLoad for given priorityfiles."""
    try:
        if not priorityfiles:
            return []
        placeholders = ", ".join(["%s"] * len(priorityfiles))
        query = (
            f"SELECT DISTINCT filename FROM audit.sc360_audit_log "
            f"WHERE batchrundate = %s AND regionname = %s AND processname = %s AND executionstatus = %s AND filename IN ({placeholders});"
        )
        params = [batch_run_date, reportregion, "RedshiftCuratedLoad", "Succeeded"] + list(priorityfiles)
        rds_cursor.execute(query, params)
        return [r[0] for r in rds_cursor.fetchall()]
    except Exception:
        logger.exception("fetch_loaded_curated_files failed")
        raise


def map_loaded_files_to_procs(rds_cursor, reportregion: str, loadedcuratedfiles: Sequence[str]) -> List[Tuple[str, str]]:
    """Map curated filenames to stored_procedure_name using sps_batch_master_table_updated (first match)."""
    try:
        procs: List[Tuple[str, str]] = []
        for lf in loadedcuratedfiles:
            rds_cursor.execute(
                "SELECT DISTINCT stored_procedure_name FROM audit.sps_batch_master_table_updated WHERE regionname=%s AND source_filename LIKE %s LIMIT 1;",
                (reportregion, f"%{lf}%"),
            )
            row = rds_cursor.fetchone()
            if row:
                procs.append((lf, row[0]))
        return procs
    except Exception:
        logger.exception("map_loaded_files_to_procs failed")
        raise


def verify_published_layer_completion(rds_cursor, reportregion: str, batch_run_date: date, procs_list: Sequence[Tuple[str, str]]) -> List[str]:
    """Return list of files whose RedshiftPublishedLoad succeeded for the given procs."""
    try:
        final_files: List[str] = []
        for loadedfile, spname in procs_list:
            # keep scriptpath as stored; parameterized query handles safety
            rds_cursor.execute(
                "SELECT COUNT(*) FROM audit.sc360_audit_log WHERE batchrundate=%s AND regionname=%s AND processname=%s AND executionstatus=%s AND scriptpath=%s;",
                (batch_run_date, reportregion, "RedshiftPublishedLoad", "Succeeded", spname),
            )
            cnt = int(rds_cursor.fetchone()[0] or 0)
            if cnt > 0:
                final_files.append(loadedfile)
        return final_files
    except Exception:
        logger.exception("verify_published_layer_completion failed")
        raise


def handle_priority_files_and_counts(
    rds_cursor,
    s3_client,
    env: str,
    reportregion: str,
    batch_run_date: date,
    priorityfiles: Sequence[str],
    final_files: Sequence[str],
) -> Tuple[List[str], List[Dict[str, List[str]]], int, str]:
    """
    Evaluate priority files: counts, missing, failed, and the legacy SCITS empty-file special-case.
    Returns (files_not_received, files_failed, pfcount, em)
    """
    try:
        files_not_received: List[str] = []
        files_failed: List[Dict[str, List[str]]] = []
        pfcount = 0
        em = ""

        if not priorityfiles:
            return files_not_received, files_failed, pfcount, em

        landing_bucket = f"sc360-{env}-{reportregion.lower()}-bucket"
        landing_prefix = f"LandingZone/dt={batch_run_date}/"
        archive_prefix = f"ArchiveZone/dt={batch_run_date}/"

        landing_files = set(list_s3_files(s3_client, landing_bucket, landing_prefix))
        archived_files = set(list_s3_files(s3_client, landing_bucket, archive_prefix))

        for pfile in priorityfiles:
            if pfile in final_files:
                pfcount += 1
                continue

            # SCITS empty-file special-case
            rds_cursor.execute(
                "SELECT COUNT(*) FROM audit.sc360_audit_log WHERE filename=%s AND processname='DataValidation' AND errormessage LIKE %s AND batchrundate=%s AND sourcename LIKE %s;",
                (pfile, "%list index out of rangeException caught while converting the SCITS file to relational in scits_data_conversion_to_relational function.%", batch_run_date, "%.dat"),
            )
            src_cnt = int(rds_cursor.fetchone()[0] or 0)
            if src_cnt > 0:
                pfcount += 1
                continue

            file_received_flag = any(pfile in f for f in landing_files) or any(pfile in f for f in archived_files)

            rds_cursor.execute(
                "SELECT DISTINCT processname FROM audit.sc360_audit_log WHERE batchrundate=%s AND executionstatus='Failed' AND filename=%s;",
                (batch_run_date, pfile),
            )
            failed_procs = [r[0] for r in rds_cursor.fetchall()]
            if failed_procs:
                files_failed.append({pfile: failed_procs})
                em = str(pfile)
            else:
                if not file_received_flag:
                    files_not_received.append(pfile)
                    em = ", ".join(files_not_received)
                else:
                    logger.info("%s received but not finished loading yet", pfile)

        return files_not_received, files_failed, pfcount, em
    except Exception:
        logger.exception("handle_priority_files_and_counts failed")
        raise


# -----------------------
# Actions: trigger, pending, cutoff
# -----------------------
def trigger_glue_job(
    glue_client,
    sns_client,
    sns_arn: str,
    env: str,
    glue_job: str,
    reportregion: str,
    rds_cursor,
    rds_conn,
    batch_run_date: date,
    priority_checked: Optional[bool] = None,
    current_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Publish SNS start message, start Glue job, and update audit row to Submitted.
    Returns Glue start response.
    """
    try:
        if current_time is None:
            current_time = datetime.utcnow()

        publish_sns(
            sns_client,
            sns_arn,
            f"*** {reportregion} BMT report refresh is started ***",
            {"Env": env, "Message": f"The BMT {reportregion} Report refresh is started. Open glue job log : {glue_job}", "Region": reportregion},
        )

        args = {"--reportregionname": reportregion, "--identifier": "BMT"}
        if priority_checked is not None:
            args["--priority_File_checked"] = "Y" if priority_checked else "N"
        resp = glue_client.start_job_run(JobName=glue_job, Arguments=args)
        jobrunid = resp.get("JobRunId")
        logger.info("Started glue job %s for %s (JobRunId=%s)", glue_job, reportregion, jobrunid)

        rds_cursor.execute(
            "UPDATE audit.sc360_reportrefreshtrigger_log SET Actual_Start_time=%s, execution_status=%s, error_message=%s WHERE batchrundate=%s AND regionname=%s AND report_source='BMT';",
            (current_time, "Submitted", "NULL", batch_run_date, reportregion),
        )
        rds_conn.commit()
        logger.info("Audit updated to Submitted for %s batch %s", reportregion, batch_run_date)
        return resp
    except Exception:
        logger.exception("trigger_glue_job failed")
        raise


def update_pending_status_bmt(rds_cursor, rds_conn, reportregion: str, batch_run_date: date, em: str) -> None:
    """Update audit row to Delay with provided error message (em)."""
    try:
        msg = f"PF Missing {em or ''}"
        rds_cursor.execute(
            "UPDATE audit.sc360_reportrefreshtrigger_log SET execution_status=%s, error_message=%s WHERE batchrundate=%s AND regionname=%s AND report_source='BMT';",
            ("Delay", msg, batch_run_date, reportregion),
        )
        rds_conn.commit()
        logger.info("Audit updated to Delay for %s batch %s (msg=%s)", reportregion, batch_run_date, msg)
    except Exception:
        logger.exception("update_pending_status_bmt failed")
        raise


def handle_cutoff_updates_and_notifications(
    rds_cursor,
    rds_conn,
    reportregion: str,
    batch_run_date: date,
    files_not_received: Sequence[str],
    files_failed: Sequence[Dict[str, List[str]]],
    env: str,
    sns_client,
    sns_arn: str,
    current_time: datetime,
    join_start: datetime,
    em: str,
) -> None:
    """Update audit and publish SNS messages when cutoff reached and missing/failed files."""
    try:
        msg = f"Cut off time Reached and priority files not received {em or ''}"
        if current_time > join_start:
            rds_cursor.execute(
                "UPDATE audit.sc360_reportrefreshtrigger_log SET error_message=%s, execution_status=%s WHERE batchrundate=%s AND regionname=%s AND report_source='BMT';",
                (msg, "Delay", batch_run_date, reportregion),
            )
            rds_conn.commit()
        if files_not_received:
            publish_sns(
                sns_client,
                sns_arn,
                f"*** {reportregion} BMT Priority File Missing ***",
                {"Env": env, "Missing Priority File": list(files_not_received), "Process Name failed, If any": "Files Not Received", "Region": reportregion},
            )
        if files_failed:
            publish_sns(
                sns_client,
                sns_arn,
                f"*** {reportregion} BMT Priority File Failure ***",
                {"Env": env, "Priority File Failed": list(files_failed), "Process Name failed, If any": "NA", "Region": reportregion},
            )
    except Exception:
        logger.exception("handle_cutoff_updates_and_notifications failed")
        raise


def create_initial_log_entry(rds_cursor, rds_conn, batch_run_date: date, reportregion: str, glue_job: str, join_start: datetime, join_end: datetime) -> None:
    """Insert initial trigger_log row (legacy semantics)."""
    try:
        rds_cursor.execute(
            "INSERT INTO audit.sc360_reportrefreshtrigger_log (batchrundate, regionname, execution_status, gluejob, report_source, Expected_Start_time, Expected_End_time, error_message) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s);",
            (batch_run_date, reportregion, "Yet to start", glue_job, "BMT", join_start, join_end, "BMT report refresh yet to start"),
        )
        rds_conn.commit()
        logger.info("Inserted initial trigger_log for %s batch %s", reportregion, batch_run_date)
    except Exception:
        logger.exception("create_initial_log_entry failed")
        raise


def handle_delay_notifications(
    current_time: datetime,
    delay_start_hour: int,
    delay_start_min: int,
    delay_end_hour: int,
    delay_end_min: int,
    env: str,
    reportregion: str,
    sns_client,
    sns_arn: str,
) -> None:
    """Publish delay notification if within configured window."""
    try:
        today = current_time.date()
        start = datetime.combine(today, datetime.min.time()).replace(hour=delay_start_hour, minute=delay_start_min, second=0)
        end = datetime.combine(today, datetime.min.time()).replace(hour=delay_end_hour, minute=delay_end_min, second=59)
        if start <= current_time <= end:
            publish_sns(sns_client, sns_arn, f"*** Delay in {reportregion} BMT Report Refresh ***", {"Env": env, "Report_Source": "BMT", "Region": reportregion, "Message": "Potential delay in BMT report generation"})
    except Exception:
        logger.exception("handle_delay_notifications failed")
        raise


# -----------------------
# Lambda handler
# -----------------------
def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """
    Orchestrate BMT APJ report refresh flow. This function is intentionally compact:
    initialization + sequence of helper calls implementing the decision tree.
    """
    region = _aws_region()
    sns_client = boto3.client("sns", region_name=region)
    glue_client = boto3.client("glue", region_name=region)
    s3_client = boto3.client("s3", region_name=region)

    # Read environment vars
    env = os.environ.get("env")
    reportregion = os.environ.get("reportregion")
    sns_arn = os.environ.get("sns_arn")
    redshift_secret_name = os.environ.get("redshift_secret_name")
    rds_secret_name = os.environ.get("rds_secret_name")
    glue_job = os.environ.get("glue_job")
    extraHour = int(os.environ.get("extraHour", "0"))
    extraMin = int(os.environ.get("extraMin", "0"))
    cutoff_strt_hour = int(os.environ.get("cutoff_strt_hour", "0"))
    cutoff_strt_min = int(os.environ.get("cutoff_strt_minute", "0"))
    cutoff_end_hour = int(os.environ.get("cutoff_end_hour", "23"))
    cutoff_end_min = int(os.environ.get("cutoff_end_minute", "59"))
    delay_cutoff_strt_hour = int(os.environ.get("delay_cutoff_strt_hour", "0"))
    delay_cutoff_strt_min = int(os.environ.get("delay_cutoff_strt_minute", "0"))
    delay_cutoff_end_hour = int(os.environ.get("delay_cutoff_end_hour", "23"))
    delay_cutoff_end_min = int(os.environ.get("delay_cutoff_end_minute", "59"))

    dependent_job1 = os.environ.get("dependent_job1", "")

    current_time = datetime.utcnow()

    rds_conn = redshift_conn = None
    try:
        # DB connections
        redshift_conn = get_db_connection(redshift_secret_name, region_name=region)
        rds_conn = get_db_connection(rds_secret_name, region_name=region)

        with redshift_conn.cursor() as redshift_cursor, rds_conn.cursor() as rds_cursor:
            logger.info("Starting BMT APJ orchestration for region=%s env=%s", reportregion, env)

            # Timing / batch info
            join_start, join_end, batch_run_date = compute_join_start_end_for_bmt(rds_cursor, reportregion)

            # Check existing trigger row for today
            rds_cursor.execute(
                "SELECT execution_status FROM audit.sc360_reportrefreshtrigger_log WHERE regionname=%s AND batchrundate=%s AND report_source=%s LIMIT 1;",
                (reportregion, batch_run_date, "BMT"),
            )
            row = rds_cursor.fetchone()
            exists, execution_status = (True, row[0]) if row else (False, None)

            if exists:
                if execution_status in ("Finished", "Submitted"):
                    logger.info("Report refresh already triggered for %s", batch_run_date)
                    return

                # Update IRR table status/expected times
                update_master_data_for_irr(rds_cursor, rds_conn, reportregion)

                # Dependent job check
                if handle_dependent_jobs_check(glue_client, rds_cursor, rds_conn, [dependent_job1], current_time, join_start, batch_run_date, reportregion):
                    return

                # Late-run forced start (bypass priority)
                extra_cutoff_st_hr = cutoff_strt_hour + extraHour
                extra_cutoff_st_mn = cutoff_strt_min + extraMin
                extra_cutoff_end_hr = cutoff_end_hour + extraHour

                in_forced_window = (current_time.hour > extra_cutoff_st_hr) or (current_time.hour == extra_cutoff_st_hr and current_time.minute >= extra_cutoff_st_mn)
                in_forced_window = in_forced_window and (current_time.hour <= extra_cutoff_end_hr)

                if in_forced_window:
                    # Force start without priority checks
                    trigger_glue_job(glue_client, sns_client, sns_arn, env, glue_job, reportregion, rds_cursor, rds_conn, batch_run_date, priority_checked=False, current_time=current_time)
                else:
                    # Normal priority-file checking flow
                    priorityfiles = fetch_priority_files(rds_cursor, reportregion)
                    loadedcuratedfiles = fetch_loaded_curated_files(rds_cursor, reportregion, batch_run_date, priorityfiles)
                    procs_list = map_loaded_files_to_procs(rds_cursor, reportregion, loadedcuratedfiles)
                    final_files = verify_published_layer_completion(rds_cursor, reportregion, batch_run_date, procs_list)
                    files_not_received, files_failed, pfcount, em = handle_priority_files_and_counts(rds_cursor, s3_client, env, reportregion, batch_run_date, priorityfiles, final_files)

                    # determine cutoff window (inclusive)
                    in_cutoff_window = False
                    try:
                        today = current_time.date()
                        cutoff_start = datetime.combine(today, datetime.min.time()).replace(hour=cutoff_strt_hour, minute=cutoff_strt_min, second=0)
                        cutoff_end = datetime.combine(today, datetime.min.time()).replace(hour=cutoff_end_hour, minute=cutoff_end_min, second=59)
                        in_cutoff_window = cutoff_start <= current_time <= cutoff_end
                    except Exception:
                        logger.exception("Error evaluating cutoff window")

                    logger.info("pfcount=%d priority_count=%d in_cutoff=%s", pfcount, len(priorityfiles), in_cutoff_window)

                    if pfcount >= len(priorityfiles):
                        trigger_glue_job(glue_client, sns_client, sns_arn, env, glue_job, reportregion, rds_cursor, rds_conn, batch_run_date, priority_checked=True, current_time=current_time)
                    elif (pfcount != len(priorityfiles)) and in_cutoff_window:
                        handle_cutoff_updates_and_notifications(rds_cursor, rds_conn, reportregion, batch_run_date, files_not_received, files_failed, env, sns_client, sns_arn, current_time, join_start, em)
                    else:
                        # Monitoring: set Delay if past expected start
                        if current_time > join_start:
                            update_pending_status_bmt(rds_cursor, rds_conn, reportregion, batch_run_date, em)

                # delay notifications
                handle_delay_notifications(current_time, delay_cutoff_strt_hour, delay_cutoff_strt_min, delay_cutoff_end_hour, delay_cutoff_end_min, env, reportregion, sns_client, sns_arn)
            else:
                # create initial trigger row
                create_initial_log_entry(rds_cursor, rds_conn, batch_run_date, reportregion, glue_job, join_start, join_end)

        logger.info("BMT APJ orchestration completed for region=%s", reportregion)
    except Exception:
        logger.exception("Error in BMT APJ lambda handler")
        raise
    finally:
        try:
            if redshift_conn:
                redshift_conn.close()
            if rds_conn:
                rds_conn.close()
        except Exception:
            logger.exception("Error closing DB connections")
