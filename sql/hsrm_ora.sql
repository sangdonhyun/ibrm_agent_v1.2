set ECHO OFF
set pages 0
set HEADING ON
set LINESIZE 2048
set PAGESIZE 2000
set TERM OFF
set TRIMS ON
set colsep ,

spool /tmp/{HOSTNAME}_{DBNAME}_hsrm_ora_fs.tmp

SELECT DBMS,instance_name,file_name,filesize,fileusage,tablespace_name,tbssize,tbsusage   AS RAW_FLETA
FROM (select 'Oracle'as DBMS, inst.instance_name, ddf.file_name, trunc((ddf.bytes)/1024/1024) as filesize,
        case when ddf.tablespace_name like '%UNDO%'
            THEN trunc(ddf.bytes/1024/1024)
            ELSE nvl(trunc((ddf.bytes - dfs.free)/1024/1024),trunc((ddf.bytes)/1024/1024))
        END as fileusage, ddf.tablespace_name, ddf1.tbssize,
        case when ddf.tablespace_name like '%UNDO%'
            THEN ddf1.tbssize
            ELSE trunc(ddf1.tbssize-dfs1.tbsfree)
        END as tbsusage
    from dba_data_files ddf,
        (select tablespace_name, trunc(sum(bytes)/1024/1024) as tbssize from dba_data_files group by  tablespace_name) ddf1,
        (select file_id,sum(bytes) as free from dba_free_space group by file_id) dfs,
        (select tablespace_name, trunc(sum(bytes)/1024/1024) as tbsfree from dba_free_space group by  tablespace_name) dfs1,
        (select instance_name from v$instance) inst
    where ddf.file_id = dfs.file_id(+) and ddf1.tablespace_name = dfs1.tablespace_name and ddf.tablespace_name=ddf1.tablespace_name
        and ddf.tablespace_name <> 'TEMP'
    group by ddf.file_name, ddf.bytes, dfs.free, ddf.tablespace_name , ddf1.tbssize, dfs1.tbsfree,inst.instance_name );




SELECT 'FPARM' ||','|| 'Oracle'||','||B.INSTANCE_NAME ||','||trim(name) ||','||trim(value)||','||trim(DESCRIPTION) as col1 from v$parameter A,
(SELECT INSTANCE_NAME FROM v$INSTANCE) B;


SELECT 'FREDU','ORACLE',INSTANCE_NAME,AA.MEMBER,ROUND(AA.BYTES),GROUP# AS REDOLOG
FROM
(SELECT A.MEMBER,B.BYTES/1024/1024 AS BYTES,A.GROUP# FROM v$LOGFILE A,v$LOG B WHERE A.GROUP#=B.GROUP#) AA,
(SELECT INSTANCE_NAME FROM v$INSTANCE) C;

SELECT 'FTEMP',B.INSTANCE_NAME,ROUND(BYTES/1024/1024), STATUS,NAME AS TEMPFI FROM v$TEMPFILE A,
(select INSTANCE_NAME from v$instance) B;

SELECT 'FARCH','ORACLE',B.INSTANCE_NAME,FIRST_TIME , ARCH_SIZE, FCNT  AS ARCH_FLETA FROM (
SELECT TO_CHAR(FIRST_TIME,'YYYYMMDD') AS FIRST_TIME,ROUND(SUM(BLOCKS*BLOCK_SIZE)/1024/1024) AS ARCH_SIZE,COUNT(FIRST_TIME) AS FCNT FROM v$ARCHIVED_LOG GROUP BY TO_CHAR(FIRST_TIME,'YYYYMMDD') ORDER BY 1) A,
(SELECT INSTANCE_NAME FROM v$INSTANCE) B;

SELECT
A.FNPSPARM ,B.INSTANCE_NAME,A.RESOURCE_NAME,A.CURRENT_UTILIZATION,A.MAX,A.INITIAL_ALLOCATION,A.LIMIT,A.USEAGE
FROM (
SELECT 'FNPSPARM'AS FNPSPARM,RESOURCE_NAME, CURRENT_UTILIZATION, MAX_UTILIZATION AS "MAX", INITIAL_ALLOCATION, LIMIT_VALUE AS "LIMIT", ROUND(MAX_UTILIZATION/INITIAL_ALLOCATION*100,1) AS "USEAGE"
FROM V$RESOURCE_LIMIT
WHERE LIMIT_VALUE NOT IN (' UNLIMITED', '         0'))A,
(SELECT INSTANCE_NAME FROM V$INSTANCE) B;

select 'DBNAME',NAME frOM V$DATABASE;

select 'VERSION', banner from v$version;


select * from v$ARCHIVED_LOG;

select * from v$LOG;

select * from V$DATABASE;

select * from V$ARCHIVE_PROCESSES;

select * from V$LOG_HISTORY;

select * from v$option where parameter = 'Real Application Clusters';

select * from v$asm_disk;

archive log list;

spool off