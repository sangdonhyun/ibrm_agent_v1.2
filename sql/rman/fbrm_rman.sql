
set ECHO OFF
set pages 0
set HEADING OFF
set LINESIZE 2048
set PAGESIZE 2000
set TERM OFF
set TRIMS ON
set colsep ,
spool /tmp/fbrm/rman_data/{HOSTNAME}_catdb_list_backup_detail.tmp

SELECT DB_KEY,
DB_NAME,
SESSION_KEY,
SESSION_RECID,
SESSION_STAMP,
TO_CHAR(START_TIME,'yyyy-mm-dd hh24:mi:ss') START_TIME,
TO_CHAR(END_TIME,'yyyy-mm-dd hh24:mi:ss') END_TIME,
INPUT_BYTES,
OUTPUT_BYTES,
STATUS_WEIGHT,
OPTIMIZED_WEIGHT,
INPUT_TYPE_WEIGHT,
OUTPUT_DEVICE_TYPE,
AUTOBACKUP_COUNT,
BACKED_BY_OSB,
AUTOBACKUP_DONE,
STATUS,
INPUT_TYPE,
OPTIMIZED,
ELAPSED_SECONDS,
COMPRESSION_RATIO,
INPUT_BYTES_PER_SEC,
OUTPUT_BYTES_PER_SEC,
INPUT_BYTES_DISPLAY,
OUTPUT_BYTES_DISPLAY,
INPUT_BYTES_PER_SEC_DISPLAY,
OUTPUT_BYTES_PER_SEC_DISPLAY
FROM RC_RMAN_BACKUP_JOB_DETAILS;
--WHERE db_key =( select db_key from ( select row_number() over (order by db_key) as sr_no,reg_db_unique_name,db_key from db) where sr_no = $4)
--and INPUT_TYPE IN ('DB INCR','DB FULL','ARCHIVELOG')
--and start_time >= sysdate - 7
--ORDER BY start_time,command_id
--ORDER BY START_TIME desc


show all

select 'IBRM_TODAY_DB',DB_NAME,STATUS from rc_rman_backup_job_details where TO_CHAR(START_TIME,'yyyy-mm-dd') = '2020/09/02 gropu by DB_NAME'
spool off