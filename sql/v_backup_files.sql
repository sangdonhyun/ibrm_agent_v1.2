set ECHO OFF
set pages 0
set HEADING ON
set LINESIZE 2048
set PAGESIZE 2000
set TERM OFF
set TRIMS ON
set colsep ,
spool /tmp/{HOSTNAME}_{DBNAME}_v_backup_files.tmp
select 'DBNAME',NAME frOM V$DATABASE;

select a.PKEY, a.BACKUP_TYPE, nvl(b.FILE_TYPE, a.BS_TYPE) as FILE_TYPE, a.STATUS, a.FNAME, a.TAG,
to_char(a.COMPLETION_TIME, 'yyyy/mm/dd hh24:mi') COMPLETION_TIME
from v$backup_files a, (select BS_KEY,FILE_TYPE from v$backup_files where file_type
in ('CONTROLFILE') group by BS_KEY, FILE_TYPE order by BS_KEY) b
where a.FILE_TYPE='PIECE' and a.bs_key=b.bs_key(+);

spool off