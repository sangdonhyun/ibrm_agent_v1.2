set ECHO OFF
set pages 0
set HEADING OFF
set LINESIZE 2048
set PAGESIZE 2000
set TERM OFF
set TRIMS ON
spool /tmp/fbrm/data/{HOSTNAME}_{DBNAME}_check_database.tmp
select * from v$database;
spool off
