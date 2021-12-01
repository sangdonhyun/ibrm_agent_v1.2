set ECHO OFF
set pages 0
set HEADING ON
set LINESIZE 2048
set PAGESIZE 2000
set TERM OFF
set TRIMS ON
set colsep ,

spool /tmp/{HOSTNAME}_{DBNAME}_session_longops.tmp
select 'DBNAME',NAME frOM V$DATABASE;
select
sid,
serial#,
opname,
target,
target_desc,
sofar,
totalwork,
units,
TO_CHAR(start_time,'yyyy-mm-dd hh24:mi:ss') start_time,
TO_CHAR(last_update_time,'yyyy-mm-dd hh24:mi:ss') last_update_time,
timestamp,
time_remaining,
elapsed_seconds,
context,
message,
username,
sql_address,
sql_hash_value,
sql_id,
sql_plan_hash_value,
sql_exec_id,
sql_plan_line_id,
sql_plan_operation,
sql_plan_options,
qcsid
 from V$SESSION_LONGOPS where opname like 'RMAN%';

spool off