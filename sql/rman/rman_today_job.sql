set ECHO OFF
set pages 0
set HEADING OFF
set LINESIZE 2048
set PAGESIZE 2000
set TERM OFF
set TRIMS ON
set colsep ,



select 'IBRM_TODAY_DB',DB_NAME,STATUS from rc_rman_backup_job_details where TO_CHAR(START_TIME,'yyyy-mm-dd') = '{}' gropu by DB_NAME
