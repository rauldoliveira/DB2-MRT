/#
/*
CALL NULLID.MRT_Checkup ();
CALL NULLID.MRT_Checkup (1, 5);

--  call monreport.dbsummary(30);
--  call monreport.pkgcache(10);
--  call monreport.connection(10);
*/

/********************************************************************************************************************************
*********************************************************************************************************************************

    Autor: Raul Diego
    E-mail: raul.oliveira@sicoob.com.br
    Data cria��o: 24/07/2015
    ->Descri��o: MRT que mostra exatamente a SYSIBMADM.DB_SUMMARY, mas fazendo os deltas e mostrando dentro do intervalo de tempo 
        que precisar. 
    ->Modo de execu�ao: Crie a proc e a execute, informando o tempo de espera entre as execu�oes em segundos e quantas repeti�oes.
 
    Compatibilidade: DB2 LUW 10.1

    Hist�rico:
        - 00/00/0000: Historico de alteracao

        
*********************************************************************************************************************************
********************************************************************************************************************************/

CREATE OR REPLACE PROCEDURE NULLID.MRT_Checkup
     (IN v_Wait SMALLINT DEFAULT 5
     ,IN v_Repetition SMALLINT DEFAULT 3)
     
     LANGUAGE SQL
     RESULT SET 1

P1: BEGIN
 
    DECLARE v_RepetitionCount SMALLINT DEFAULT 1;
    
    DECLARE GLOBAL TEMPORARY TABLE SESSION.MRT_Checkup AS (
    SELECT T.*
        ,CURRENT_TIMESTAMP AS DATAREGISTRO
        ,1 AS RepetitionCount  
    FROM TABLE(MON_GET_SERVICE_SUBCLASS('', '', -2)) T
    ) DEFINITION ONLY
           ON COMMIT DELETE ROWS
           NOT LOGGED ON ROLLBACK DELETE ROWS
           WITH REPLACE;
         
    WHILE v_RepetitionCount <= v_Repetition DO
    
        INSERT INTO SESSION.MRT_Checkup
        SELECT T.*
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,v_RepetitionCount AS RepetitionCount 
        FROM TABLE(MON_GET_SERVICE_SUBCLASS('', '', -2)) T  
        ;        
--        ,CURRENT_TIMESTAMP AS DATAREGISTRO
--        ,v_RepetitionCount AS RepetitionCount    
        SET v_RepetitionCount = v_RepetitionCount + 1;
        
        --CALL NULLID.WAITFOR(v_Wait);
        CALL NULLID.WAITFOR_MRT(v_Wait, 'MRT_Checkup');

    END WHILE;    
    
P2: BEGIN

    DECLARE cRet CURSOR WITH RETURN FOR
--        SELECT * FROM SESSION.MRT_Checkup;
        WITH DIFF AS (
        SELECT 
                TO_CHAR(T1.DATAREGISTRO,'HH24:MI:SS') AS HorarioInicio
                , TO_CHAR(T2.DATAREGISTRO,'HH24:MI:SS') AS HorarioFim

                , T1.SERVICE_SUPERCLASS_NAME
                , T1.SERVICE_SUBCLASS_NAME
                , T2.ACT_ABORTED_TOTAL - T1.ACT_ABORTED_TOTAL AS ACT_ABORTED_TOTAL
                , T2.ACT_COMPLETED_TOTAL - T1.ACT_COMPLETED_TOTAL AS ACT_COMPLETED_TOTAL
		, T2.AGENT_WAIT_TIME - T1.AGENT_WAIT_TIME AS AGENT_WAIT_TIME
                , T2.POOL_DATA_L_READS - T1.POOL_DATA_L_READS AS POOL_DATA_L_READS
                , T2.POOL_INDEX_L_READS - T1.POOL_INDEX_L_READS AS POOL_INDEX_L_READS
                , T2.POOL_TEMP_DATA_L_READS - T1.POOL_TEMP_DATA_L_READS AS POOL_TEMP_DATA_L_READS
                , T2.POOL_TEMP_INDEX_L_READS - T1.POOL_TEMP_INDEX_L_READS AS POOL_TEMP_INDEX_L_READS
                , T2.POOL_TEMP_XDA_L_READS - T1.POOL_TEMP_XDA_L_READS AS POOL_TEMP_XDA_L_READS
                , T2.POOL_XDA_L_READS - T1.POOL_XDA_L_READS AS POOL_XDA_L_READS
                , T2.POOL_READ_TIME - T1.POOL_READ_TIME AS POOL_READ_TIME
                , T2.POOL_WRITE_TIME - T1.POOL_WRITE_TIME AS POOL_WRITE_TIME
                , T2.DEADLOCKS - T1.DEADLOCKS AS DEADLOCKS
                , T2.DIRECT_READ_TIME - T1.DIRECT_READ_TIME AS DIRECT_READ_TIME
                , T2.DIRECT_WRITE_TIME - T1.DIRECT_WRITE_TIME AS DIRECT_WRITE_TIME
                , T2.IPC_RECV_WAIT_TIME - T1.IPC_RECV_WAIT_TIME AS IPC_RECV_WAIT_TIME
                , T2.IPC_SEND_WAIT_TIME - T1.IPC_SEND_WAIT_TIME AS IPC_SEND_WAIT_TIME
                , T2.LOCK_ESCALS - T1.LOCK_ESCALS AS LOCK_ESCALS
                , T2.LOCK_TIMEOUTS - T1.LOCK_TIMEOUTS AS LOCK_TIMEOUTS
                , T2.LOCK_WAIT_TIME - T1.LOCK_WAIT_TIME AS LOCK_WAIT_TIME
                , T2.LOCK_WAITS - T1.LOCK_WAITS AS LOCK_WAITS
                , T2.RQSTS_COMPLETED_TOTAL - T1.RQSTS_COMPLETED_TOTAL AS RQSTS_COMPLETED_TOTAL
                , T2.ROWS_MODIFIED - T1.ROWS_MODIFIED AS ROWS_MODIFIED
                , T2.ROWS_READ - T1.ROWS_READ AS ROWS_READ
                , T2.ROWS_RETURNED - T1.ROWS_RETURNED AS ROWS_RETURNED
                , T2.TCPIP_SEND_WAIT_TIME - T1.TCPIP_SEND_WAIT_TIME AS TCPIP_SEND_WAIT_TIME
                , T2.TCPIP_RECV_WAIT_TIME - T1.TCPIP_RECV_WAIT_TIME AS TCPIP_RECV_WAIT_TIME
                , T2.TOTAL_RQST_TIME - T1.TOTAL_RQST_TIME AS TOTAL_RQST_TIME
                , T2.TOTAL_CPU_TIME - T1.TOTAL_CPU_TIME AS TOTAL_CPU_TIME
                , T2.TOTAL_WAIT_TIME - T1.TOTAL_WAIT_TIME AS TOTAL_WAIT_TIME
                , T2.APP_RQSTS_COMPLETED_TOTAL - T1.APP_RQSTS_COMPLETED_TOTAL AS APP_RQSTS_COMPLETED_TOTAL
                , T2.TOTAL_SECTION_SORT_PROC_TIME - T1.TOTAL_SECTION_SORT_PROC_TIME AS TOTAL_SECTION_SORT_PROC_TIME
                , T2.TOTAL_IMPLICIT_COMPILE_PROC_TIME - T1.TOTAL_IMPLICIT_COMPILE_PROC_TIME AS TOTAL_IMPLICIT_COMPILE_PROC_TIME
                , T2.TOTAL_COMPILE_PROC_TIME - T1.TOTAL_COMPILE_PROC_TIME AS TOTAL_COMPILE_PROC_TIME
                , T2.TOTAL_SECTION_PROC_TIME - T1.TOTAL_SECTION_PROC_TIME AS TOTAL_SECTION_PROC_TIME
                , T2.TOTAL_ACT_TIME - T1.TOTAL_ACT_TIME AS TOTAL_ACT_TIME
                , T2.TOTAL_ACT_WAIT_TIME - T1.TOTAL_ACT_WAIT_TIME AS TOTAL_ACT_WAIT_TIME
                , T2.TOTAL_ROUTINE_TIME - T1.TOTAL_ROUTINE_TIME AS TOTAL_ROUTINE_TIME
                , T2.TOTAL_COMMIT_PROC_TIME - T1.TOTAL_COMMIT_PROC_TIME AS TOTAL_COMMIT_PROC_TIME
                , T2.TOTAL_APP_COMMITS - T1.TOTAL_APP_COMMITS AS TOTAL_APP_COMMITS
                , T2.TOTAL_ROLLBACK_PROC_TIME - T1.TOTAL_ROLLBACK_PROC_TIME AS TOTAL_ROLLBACK_PROC_TIME
                , T2.TOTAL_APP_ROLLBACKS - T1.TOTAL_APP_ROLLBACKS AS TOTAL_APP_ROLLBACKS
                , T2.TOTAL_RUNSTATS_PROC_TIME - T1.TOTAL_RUNSTATS_PROC_TIME AS TOTAL_RUNSTATS_PROC_TIME
                , T2.TOTAL_REORG_PROC_TIME - T1.TOTAL_REORG_PROC_TIME AS TOTAL_REORG_PROC_TIME
                , T2.TOTAL_LOAD_PROC_TIME - T1.TOTAL_LOAD_PROC_TIME AS TOTAL_LOAD_PROC_TIME
                , T2.RECLAIM_WAIT_TIME - T1.RECLAIM_WAIT_TIME AS RECLAIM_WAIT_TIME
                , T2.SPACEMAPPAGE_RECLAIM_WAIT_TIME - T1.SPACEMAPPAGE_RECLAIM_WAIT_TIME AS SPACEMAPPAGE_RECLAIM_WAIT_TIME
                , T2.CF_WAIT_TIME - T1.CF_WAIT_TIME AS CF_WAIT_TIME
                , T2.POOL_DATA_LBP_PAGES_FOUND - T1.POOL_DATA_LBP_PAGES_FOUND AS POOL_DATA_LBP_PAGES_FOUND
                , T2.POOL_INDEX_LBP_PAGES_FOUND - T1.POOL_INDEX_LBP_PAGES_FOUND AS POOL_INDEX_LBP_PAGES_FOUND
                , T2.POOL_XDA_LBP_PAGES_FOUND - T1.POOL_XDA_LBP_PAGES_FOUND AS POOL_XDA_LBP_PAGES_FOUND
               	, T2.POOL_DATA_P_READS - T1.POOL_DATA_P_READS AS POOL_DATA_P_READS
                , T2.POOL_INDEX_P_READS - T1.POOL_INDEX_P_READS AS POOL_INDEX_P_READS
                , T2.POOL_DATA_WRITES - T1.POOL_DATA_WRITES AS POOL_DATA_WRITES
                , T2.POOL_INDEX_WRITES - T1.POOL_INDEX_WRITES AS POOL_INDEX_WRITES
                , T2.DIRECT_READS - T1.DIRECT_READS AS DIRECT_READS
                , T2.DIRECT_WRITES - T1.DIRECT_WRITES AS DIRECT_WRITES
                , T2.DIRECT_READ_REQS - T1.DIRECT_READ_REQS AS DIRECT_READ_REQS
                , T2.DIRECT_WRITE_REQS - T1.DIRECT_WRITE_REQS AS DIRECT_WRITE_REQS
		, T2.TOTAL_SORTS - T1.TOTAL_SORTS AS TOTAL_SORTS
		, T2.TOTAL_SECTION_SORT_TIME - T1.TOTAL_SECTION_SORT_TIME AS TOTAL_SECTION_SORT_TIME
		, T2.TOTAL_SECTION_SORTS - T1.TOTAL_SECTION_SORTS AS TOTAL_SECTION_SORTS
                , T2.TQ_SORT_HEAP_REQUESTS - T1.TQ_SORT_HEAP_REQUESTS AS TQ_SORT_HEAP_REQUESTS
                , T2.TQ_SORT_HEAP_REJECTIONS - T1.TQ_SORT_HEAP_REJECTIONS AS TQ_SORT_HEAP_REJECTIONS
		, T2.TQ_TOT_SEND_SPILLS - T1.TQ_TOT_SEND_SPILLS AS TQ_TOT_SEND_SPILLS
                , T2.CAT_CACHE_INSERTS - T1.CAT_CACHE_INSERTS AS CAT_CACHE_INSERTS
                , T2.CAT_CACHE_LOOKUPS - T1.CAT_CACHE_LOOKUPS AS CAT_CACHE_LOOKUPS
                , T2.PKG_CACHE_INSERTS - T1.PKG_CACHE_INSERTS AS PKG_CACHE_INSERTS
                , T2.PKG_CACHE_LOOKUPS - T1.PKG_CACHE_LOOKUPS AS PKG_CACHE_LOOKUPS
                , T2.TOTAL_COMPILE_TIME - T1.TOTAL_COMPILE_TIME AS TOTAL_COMPILE_TIME
                , T2.TOTAL_COMPILATIONS - T1.TOTAL_COMPILATIONS AS TOTAL_COMPILATIONS
		, T2.TOTAL_SECTION_TIME - T1.TOTAL_SECTION_TIME AS TOTAL_SECTION_TIME
                , T2.LOG_BUFFER_WAIT_TIME - T1.LOG_BUFFER_WAIT_TIME AS LOG_BUFFER_WAIT_TIME
                , T2.NUM_LOG_BUFFER_FULL - T1.NUM_LOG_BUFFER_FULL AS NUM_LOG_BUFFER_FULL
                , T2.LOG_DISK_WAIT_TIME - T1.LOG_DISK_WAIT_TIME AS LOG_DISK_WAIT_TIME
                , T2.LOG_DISK_WAITS_TOTAL - T1.LOG_DISK_WAITS_TOTAL AS LOG_DISK_WAITS_TOTAL
                , T2.SORT_OVERFLOWS - T1.SORT_OVERFLOWS AS SORT_OVERFLOWS
                , T2.POOL_QUEUED_ASYNC_DATA_REQS - T1.POOL_QUEUED_ASYNC_DATA_REQS AS POOL_QUEUED_ASYNC_DATA_REQS
                , T2.POOL_QUEUED_ASYNC_INDEX_REQS - T1.POOL_QUEUED_ASYNC_INDEX_REQS AS POOL_QUEUED_ASYNC_INDEX_REQS
                , T2.POOL_QUEUED_ASYNC_XDA_REQS - T1.POOL_QUEUED_ASYNC_XDA_REQS AS POOL_QUEUED_ASYNC_XDA_REQS
                , T2.POOL_QUEUED_ASYNC_TEMP_DATA_REQS - T1.POOL_QUEUED_ASYNC_TEMP_DATA_REQS AS POOL_QUEUED_ASYNC_TEMP_DATA_REQS
                , T2.POOL_QUEUED_ASYNC_TEMP_INDEX_REQS - T1.POOL_QUEUED_ASYNC_TEMP_INDEX_REQS AS POOL_QUEUED_ASYNC_TEMP_INDEX_REQS
                , T2.POOL_QUEUED_ASYNC_TEMP_XDA_REQS - T1.POOL_QUEUED_ASYNC_TEMP_XDA_REQS AS POOL_QUEUED_ASYNC_TEMP_XDA_REQS
                , T2.POOL_QUEUED_ASYNC_OTHER_REQS - T1.POOL_QUEUED_ASYNC_OTHER_REQS AS POOL_QUEUED_ASYNC_OTHER_REQS
                , T2.POOL_QUEUED_ASYNC_DATA_PAGES - T1.POOL_QUEUED_ASYNC_DATA_PAGES AS POOL_QUEUED_ASYNC_DATA_PAGES
                , T2.POOL_QUEUED_ASYNC_INDEX_PAGES - T1.POOL_QUEUED_ASYNC_INDEX_PAGES AS POOL_QUEUED_ASYNC_INDEX_PAGES
                , T2.POOL_QUEUED_ASYNC_XDA_PAGES - T1.POOL_QUEUED_ASYNC_XDA_PAGES AS POOL_QUEUED_ASYNC_XDA_PAGES
                , T2.POOL_QUEUED_ASYNC_TEMP_DATA_PAGES - T1.POOL_QUEUED_ASYNC_TEMP_DATA_PAGES AS POOL_QUEUED_ASYNC_TEMP_DATA_PAGES
                , T2.POOL_QUEUED_ASYNC_TEMP_INDEX_PAGES - T1.POOL_QUEUED_ASYNC_TEMP_INDEX_PAGES AS POOL_QUEUED_ASYNC_TEMP_INDEX_PAGES
                , T2.POOL_QUEUED_ASYNC_TEMP_XDA_PAGES - T1.POOL_QUEUED_ASYNC_TEMP_XDA_PAGES AS POOL_QUEUED_ASYNC_TEMP_XDA_PAGES
                , T2.POOL_FAILED_ASYNC_DATA_REQS - T1.POOL_FAILED_ASYNC_DATA_REQS AS POOL_FAILED_ASYNC_DATA_REQS
                , T2.POOL_FAILED_ASYNC_INDEX_REQS - T1.POOL_FAILED_ASYNC_INDEX_REQS AS POOL_FAILED_ASYNC_INDEX_REQS
                , T2.POOL_FAILED_ASYNC_XDA_REQS - T1.POOL_FAILED_ASYNC_XDA_REQS AS POOL_FAILED_ASYNC_XDA_REQS
                , T2.POOL_FAILED_ASYNC_TEMP_DATA_REQS - T1.POOL_FAILED_ASYNC_TEMP_DATA_REQS AS POOL_FAILED_ASYNC_TEMP_DATA_REQS
                , T2.POOL_FAILED_ASYNC_TEMP_INDEX_REQS - T1.POOL_FAILED_ASYNC_TEMP_INDEX_REQS AS POOL_FAILED_ASYNC_TEMP_INDEX_REQS
                , T2.POOL_FAILED_ASYNC_TEMP_XDA_REQS - T1.POOL_FAILED_ASYNC_TEMP_XDA_REQS AS POOL_FAILED_ASYNC_TEMP_XDA_REQS
                , T2.POOL_FAILED_ASYNC_OTHER_REQS - T1.POOL_FAILED_ASYNC_OTHER_REQS AS POOL_FAILED_ASYNC_OTHER_REQS
                , T2.RepetitionCount
        FROM SESSION.MRT_Checkup T1
        INNER JOIN SESSION.MRT_Checkup T2
               --ON T1.BANCO = T2.BANCO
                --AND T1.LATCH_NAME = T2.LATCH_NAME
                ON T1.RepetitionCount < T2.RepetitionCount
                AND T2.RepetitionCount - T1.RepetitionCount = 1
                AND T1.SERVICE_SUPERCLASS_NAME = T2.SERVICE_SUPERCLASS_NAME
                AND T1.SERVICE_SUBCLASS_NAME = T2.SERVICE_SUBCLASS_NAME
                AND T1.SERVICE_CLASS_ID = T2.SERVICE_CLASS_ID
        ),
        C AS (
        SELECT 	
                HorarioInicio, HorarioFim,
                --, SERVICE_SUPERCLASS_NAME, SERVICE_SUBCLASS_NAME,
                SUM(TOTAL_APP_COMMITS)                       AS TOTAL_APP_COMMITS ,
                SUM(TOTAL_APP_ROLLBACKS)                     AS TOTAL_APP_ROLLBACKS ,
                SUM(ACT_COMPLETED_TOTAL)                     AS ACT_COMPLETED_TOTAL ,
                SUM(ROWS_MODIFIED) AS ROWS_MODIFIED,
                SUM(ROWS_READ) AS ROWS_READ,
                SUM(ROWS_RETURNED) AS ROWS_RETURNED,
                SUM(APP_RQSTS_COMPLETED_TOTAL)                            AS APP_RQSTS_COMPLETED_TOTAL ,
                SUM(POOL_DATA_P_READS) AS POOL_DATA_P_READS
                ,SUM(POOL_INDEX_P_READS) AS POOL_INDEX_P_READS
                ,SUM(POOL_READ_TIME) AS POOL_READ_TIME
                ,SUM(POOL_DATA_WRITES) AS POOL_DATA_WRITES 
                ,SUM(POOL_INDEX_WRITES) AS POOL_INDEX_WRITES
                ,SUM(POOL_WRITE_TIME) AS POOL_WRITE_TIME
                ,SUM(ACT_COMPLETED_TOTAL + ACT_ABORTED_TOTAL)                              AS ACT_TOTAL ,
                SUM(TOTAL_CPU_TIME)                                                  AS TOTAL_CPU_TIME ,
                SUM(TOTAL_WAIT_TIME)                                                AS TOTAL_WAIT_TIME ,
                SUM(TOTAL_RQST_TIME)                                                AS TOTAL_RQST_TIME ,
                SUM(TOTAL_ACT_WAIT_TIME)                                        AS TOTAL_ACT_WAIT_TIME ,
                SUM(TOTAL_ACT_TIME)                                                  AS TOTAL_ACT_TIME ,
                SUM(POOL_READ_TIME + POOL_WRITE_TIME + DIRECT_READ_TIME + DIRECT_WRITE_TIME) AS
                                                                IO_WAIT_TIME ,
                SUM(LOCK_WAIT_TIME)  AS LOCK_WAIT_TIME ,
                SUM(AGENT_WAIT_TIME) AS AGENT_WAIT_TIME ,
                SUM(LOCK_WAITS)      AS LOCK_WAITS ,
                SUM(LOCK_TIMEOUTS)   AS LOCK_TIMEOUTS ,
                SUM(DEADLOCKS)       AS DEADLOCKS ,
                SUM(LOCK_ESCALS)     AS LOCK_ESCALS ,
                SUM(TCPIP_SEND_WAIT_TIME + TCPIP_RECV_WAIT_TIME + IPC_SEND_WAIT_TIME +
                IPC_RECV_WAIT_TIME)                            AS NETWORK_WAIT_TIME ,
                SUM(TCPIP_SEND_WAIT_TIME + TCPIP_RECV_WAIT_TIME) AS TCP_WAIT_TIME ,
                SUM(IPC_SEND_WAIT_TIME + IPC_RECV_WAIT_TIME) AS IPC_WAIT_TIME ,
                SUM(TOTAL_RQST_TIME - TOTAL_WAIT_TIME)                          AS PROC_TIME ,
                SUM(TOTAL_SECTION_PROC_TIME)                                    AS SECTION_PROC_TIME ,
                SUM(TOTAL_SECTION_SORT_PROC_TIME)                            AS SECTION_SORT_PROC_TIME ,
                SUM(TOTAL_COMPILE_PROC_TIME + TOTAL_IMPLICIT_COMPILE_PROC_TIME) AS COMPILE_PROC_TIME ,
                SUM(TOTAL_COMMIT_PROC_TIME + TOTAL_ROLLBACK_PROC_TIME)          AS
                TRANSACT_END_PROC_TIME ,
                SUM(TOTAL_RUNSTATS_PROC_TIME + TOTAL_REORG_PROC_TIME + TOTAL_LOAD_PROC_TIME) AS
                UTILS_PROC_TIME ,
                SUM(POOL_DATA_L_READS + POOL_TEMP_DATA_L_READS + POOL_INDEX_L_READS +
                POOL_TEMP_INDEX_L_READS + POOL_XDA_L_READS + POOL_TEMP_XDA_L_READS) AS L_READS ,
                --SUM(POOL_DATA_P_READS + POOL_TEMP_DATA_P_READS + POOL_INDEX_P_READS +
                --POOL_TEMP_INDEX_P_READS + POOL_XDA_P_READS + POOL_TEMP_XDA_P_READS) AS P_READS ,
                SUM(POOL_DATA_LBP_PAGES_FOUND + POOL_INDEX_LBP_PAGES_FOUND + POOL_XDA_LBP_PAGES_FOUND)
                                                                                                                AS BP_PAGES_FOUND ,
                SUM(POOL_DATA_LBP_PAGES_FOUND) AS POOL_DATA_LBP_PAGES_FOUND
                ,SUM(POOL_INDEX_LBP_PAGES_FOUND) AS POOL_INDEX_LBP_PAGES_FOUND 
                ,SUM(POOL_XDA_LBP_PAGES_FOUND) AS POOL_XDA_LBP_PAGES_FOUND
                ,SUM(TOTAL_ROUTINE_TIME)                                          AS TOTAL_ROUTINE_TIME ,
                SUM(CF_WAIT_TIME)                   AS CF_WAIT_TIME ,
                SUM(RECLAIM_WAIT_TIME)              AS RECLAIM_WAIT_TIME ,
                SUM(SPACEMAPPAGE_RECLAIM_WAIT_TIME) AS SPACEMAPPAGE_RECLAIM_WAIT_TIME
                ,SUM(POOL_QUEUED_ASYNC_DATA_REQS + POOL_QUEUED_ASYNC_INDEX_REQS + POOL_QUEUED_ASYNC_XDA_REQS 
                        + POOL_QUEUED_ASYNC_TEMP_DATA_REQS + POOL_QUEUED_ASYNC_TEMP_INDEX_REQS + POOL_QUEUED_ASYNC_TEMP_XDA_REQS
                        + POOL_QUEUED_ASYNC_OTHER_REQS) AS POOL_QUEUED_ASYNC_REQS
                ,SUM(POOL_QUEUED_ASYNC_DATA_PAGES + POOL_QUEUED_ASYNC_INDEX_PAGES + POOL_QUEUED_ASYNC_XDA_PAGES
                        + POOL_QUEUED_ASYNC_TEMP_DATA_PAGES + POOL_QUEUED_ASYNC_TEMP_INDEX_PAGES
                        + POOL_QUEUED_ASYNC_TEMP_XDA_PAGES) AS POOL_QUEUED_ASYNC_PAGES
                ,SUM(POOL_FAILED_ASYNC_DATA_REQS + POOL_FAILED_ASYNC_INDEX_REQS + POOL_FAILED_ASYNC_XDA_REQS
                        + POOL_FAILED_ASYNC_TEMP_DATA_REQS + POOL_FAILED_ASYNC_TEMP_INDEX_REQS + POOL_FAILED_ASYNC_TEMP_XDA_REQS  
                        + POOL_FAILED_ASYNC_OTHER_REQS) AS POOL_FAILED_ASYNC_REQS
                ,SUM(DIRECT_READS) AS DIRECT_READS
                ,SUM(DIRECT_WRITES) AS DIRECT_WRITES
                ,SUM(DIRECT_READ_REQS) AS DIRECT_READ_REQS
                ,SUM(DIRECT_WRITE_REQS) AS DIRECT_WRITE_REQS
                ,SUM(DIRECT_READ_TIME) AS DIRECT_READ_TIME
                ,SUM(DIRECT_WRITE_TIME) AS DIRECT_WRITE_TIME
                ,SUM(POOL_DATA_L_READS) AS POOL_DATA_L_READS
                ,SUM(POOL_INDEX_L_READS) AS POOL_INDEX_L_READS
                ,SUM(TOTAL_SORTS) AS TOTAL_SORTS
                ,SUM(TOTAL_SECTION_SORT_TIME) AS TOTAL_SECTION_SORT_TIME
                ,SUM(TOTAL_SECTION_SORTS) AS TOTAL_SECTION_SORTS
                ,SUM(TQ_SORT_HEAP_REQUESTS) AS TQ_SORT_HEAP_REQUESTS
                ,SUM(TQ_SORT_HEAP_REJECTIONS) AS TQ_SORT_HEAP_REJECTIONS
                ,SUM(TQ_TOT_SEND_SPILLS) AS TQ_TOT_SEND_SPILLS
                ,SUM(CAT_CACHE_INSERTS)  AS CAT_CACHE_INSERTS
                ,SUM(CAT_CACHE_LOOKUPS) AS CAT_CACHE_LOOKUPS
                ,SUM(PKG_CACHE_INSERTS) AS PKG_CACHE_INSERTS
                ,SUM(PKG_CACHE_LOOKUPS) AS PKG_CACHE_LOOKUPS
                ,SUM(TOTAL_COMPILE_TIME) AS TOTAL_COMPILE_TIME
                ,SUM(TOTAL_COMPILATIONS) AS TOTAL_COMPILATIONS
                ,SUM(TOTAL_SECTION_TIME) AS TOTAL_SECTION_TIME
                ,SUM(LOG_BUFFER_WAIT_TIME) AS LOG_BUFFER_WAIT_TIME
                ,SUM(NUM_LOG_BUFFER_FULL) AS NUM_LOG_BUFFER_FULL
                ,SUM(LOG_DISK_WAIT_TIME) AS LOG_DISK_WAIT_TIME
                ,SUM(LOG_DISK_WAITS_TOTAL) AS LOG_DISK_WAITS_TOTAL
                ,SUM(SORT_OVERFLOWS) AS SORT_OVERFLOWS
                ,DIFF.RepetitionCount
        FROM DIFF
        GROUP BY HorarioInicio, HorarioFim, DIFF.RepetitionCount
                --, SERVICE_SUPERCLASS_NAME, SERVICE_SUBCLASS_NAME
        )
        SELECT
	    HorarioInicio
	    ,HorarioFim
	    --, SERVICE_SUPERCLASS_NAME, SERVICE_SUBCLASS_NAME
            ,TOTAL_APP_COMMITS ,
            TOTAL_APP_ROLLBACKS ,
	    APP_RQSTS_COMPLETED_TOTAL ,
	    ACT_COMPLETED_TOTAL ,
	    ACT_TOTAL,
            ROWS_MODIFIED AS CON_ROWS_MODIFIED,
            C.ROWS_READ AS CON_ROWS_READ,
            ROWS_RETURNED AS CON_ROWS_RETURNED
            ,CASE
                WHEN ROWS_RETURNED > 0 AND C.ROWS_RETURNED > 0
                THEN C.ROWS_READ / ROWS_RETURNED
                ELSE NULL
            END AS ROWS_READ_PER_ROWS_RETURNED
	--TOTAL_CPU_TIME, -- RETIRAR ESSA AQUI, S? PRA TESTE
            ,'--' AS "--"

--            ,(SELECT TOP 1 (MEMORY_TOTAL /1024)
--		FROM DB2.ENV_GET_SYSTEM_RESOURCES EGSR
--		WHERE EGSR.Servidor = C.Servidor
--		    AND EGSR.BancoDados = C.BancoDados
--		    AND CAST(EGSR.DataRegistro AS DATE) = CAST(C.HoraInicio AS DATE)
--		ORDER BY EGSR.DataRegistro DESC) AS MemGBInicio
--            ,(SELECT TOP 1 (MEMORY_TOTAL /1024)
--		FROM DB2.ENV_GET_SYSTEM_RESOURCES EGSR
--		WHERE EGSR.Servidor = C.Servidor
--		    AND EGSR.BancoDados = C.BancoDados
--		    --AND CAST(EGSR.DataRegistro AS SMALLDATETIME) = C.HoraFim) AS MemGBFim
--                    AND CAST(EGSR.DataRegistro AS DATE) = CAST(C.HoraFim AS DATE)
--                ORDER BY EGSR.DataRegistro DESC) AS MemGBFim
--
--	    ,(SELECT TOP 1 CPU_ONLINE
--		FROM DB2.ENV_GET_SYSTEM_RESOURCES EGSR
--		WHERE EGSR.Servidor = C.Servidor
--		    AND EGSR.BancoDados = C.BancoDados
--		    --AND CAST(EGSR.DataRegistro AS SMALLDATETIME) = C.HoraInicio) AS CPUInicio
--		    AND CAST(EGSR.DataRegistro AS DATE) = CAST(C.HoraInicio AS DATE)
--		ORDER BY EGSR.DataRegistro DESC) AS CPUInicio
--	    ,(SELECT TOP 1 CPU_ONLINE
--		FROM DB2.ENV_GET_SYSTEM_RESOURCES EGSR
--		WHERE EGSR.Servidor = C.Servidor
--		    AND EGSR.BancoDados = C.BancoDados
--		    --AND CAST(EGSR.DataRegistro AS SMALLDATETIME) = C.HoraFim) AS CPUFim
--		    AND CAST(EGSR.DataRegistro AS DATE) = CAST(C.HoraFim AS DATE)
--		ORDER BY EGSR.DataRegistro DESC) AS CPUFim

			--,((pool_data_p_reads + pool_index_p_reads) - (pool_async_data_reads + pool_async_index_reads)) / (pool_data_l_reads + pool_index_l_reads)
	    ,'----' AS "----"
	    ,CASE
                WHEN TOTAL_RQST_TIME > 0
                THEN (TOTAL_ROUTINE_TIME / TOTAL_RQST_TIME * 100.0)
                ELSE NULL
            END AS ROUTINE_TIME_RQST_PERCENT
            ,CASE
                WHEN TOTAL_RQST_TIME > 0
                THEN CAST((TOTAL_WAIT_TIME / (TOTAL_RQST_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS RQST_WAIT_TIME_PERCENT 
            ,CASE
                WHEN TOTAL_ACT_TIME > 0
                THEN CAST((TOTAL_ACT_WAIT_TIME / (TOTAL_ACT_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS ACT_WAIT_TIME_PERCENT
            ,CASE
                WHEN TOTAL_WAIT_TIME > 0 AND IO_WAIT_TIME > 0
                THEN CAST((IO_WAIT_TIME / (TOTAL_WAIT_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS IO_WAIT_TIME_PERCENT
            ,CASE
                WHEN TOTAL_WAIT_TIME > 0 --AND LOCK_WAIT_TIME > 0
                --THEN CAST((LOCK_WAIT_TIME / (TOTAL_WAIT_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                THEN CAST((LOCK_WAIT_TIME / DECIMAL(TOTAL_WAIT_TIME,10,2) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS LOCK_WAIT_TIME_PERCENT ,
            CASE
                WHEN TOTAL_WAIT_TIME > 0 --AND AGENT_WAIT_TIME > 0
                THEN CAST((AGENT_WAIT_TIME / (TOTAL_WAIT_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS AGENT_WAIT_TIME_PERCENT ,
            CASE
                WHEN TOTAL_WAIT_TIME > 0 AND NETWORK_WAIT_TIME > 0
                THEN CAST((NETWORK_WAIT_TIME / (TOTAL_WAIT_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS NETWORK_WAIT_TIME_PERCENT
            
            ,CASE
                WHEN TOTAL_WAIT_TIME > 0 AND TCP_WAIT_TIME > 0
                THEN CAST((TCP_WAIT_TIME / (TOTAL_WAIT_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS TCP_WAIT_TIME_PERCENT
            ,CASE
                WHEN TOTAL_WAIT_TIME > 0 AND IPC_WAIT_TIME > 0
                THEN CAST((IPC_WAIT_TIME / (TOTAL_WAIT_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS IPC_WAIT_TIME_PERCENT                        
            
            ,CASE
                WHEN PROC_TIME > 0 AND SECTION_PROC_TIME > 0
                THEN CAST((SECTION_PROC_TIME / (PROC_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS SECTION_PROC_TIME_PERCENT ,
            CASE
                WHEN PROC_TIME > 0 AND SECTION_SORT_PROC_TIME > 0
                THEN CAST((SECTION_SORT_PROC_TIME / (PROC_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS SECTION_SORT_PROC_TIME_PERCENT ,

            CASE
                WHEN PROC_TIME > 0 AND COMPILE_PROC_TIME > 0
                THEN CAST((COMPILE_PROC_TIME / (PROC_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS COMPILE_PROC_TIME_PERCENT
--			
            ,CASE
                WHEN PROC_TIME > 0 AND TRANSACT_END_PROC_TIME > 0
                THEN CAST((TRANSACT_END_PROC_TIME / (PROC_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS TRANSACT_END_PROC_TIME_PERCENT ,
            CASE
                WHEN PROC_TIME > 0 AND UTILS_PROC_TIME > 0
                THEN CAST((UTILS_PROC_TIME / (PROC_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS UTILS_PROC_TIME_PERCENT

	    ,CASE
	       WHEN POOL_FAILED_ASYNC_REQS > 0
	       THEN ((POOL_FAILED_ASYNC_REQS * 1.0) 
	                       / ((POOL_FAILED_ASYNC_REQS + POOL_QUEUED_ASYNC_REQS) * 1.0) * 100.0) 
	       ELSE NULL 
	      END AS FAILED_PREFETCH_REQUESTS_PCT

            ,CASE
                WHEN L_READS > 0 AND BP_PAGES_FOUND > 0
                THEN CAST((BP_PAGES_FOUND / (L_READS * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS TOTAL_BP_HIT_RATIO_PERCENT

            ,CASE
                WHEN TOTAL_WAIT_TIME > 0 --AND CF_WAIT_TIME > 0
                THEN CAST((CF_WAIT_TIME / (TOTAL_WAIT_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS CF_WAIT_TIME_PERCENT ,
            CASE
                WHEN TOTAL_WAIT_TIME > 0 --AND RECLAIM_WAIT_TIME > 0
                THEN CAST((RECLAIM_WAIT_TIME / (TOTAL_WAIT_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS RECLAIM_WAIT_TIME_PERCENT ,
            CASE
                WHEN TOTAL_WAIT_TIME > 0 AND SPACEMAPPAGE_RECLAIM_WAIT_TIME > 0
                THEN CAST((SPACEMAPPAGE_RECLAIM_WAIT_TIME / (TOTAL_WAIT_TIME * 1.0) * 100.0) AS DECIMAL(10,2))
                ELSE NULL
            END AS SPACEMAPPAGE_RECLAIM_WAIT_TIME_PERCENT
	   , CASE
	        WHEN PKG_CACHE_INSERTS > 0 OR PKG_CACHE_LOOKUPS > 0
	        THEN DEC((DEC(PKG_CACHE_INSERTS,10,2) / DEC(PKG_CACHE_LOOKUPS,10,2) * 100.0),10,2)
	        ELSE NULL
	     END AS INSERTS_PKG_CACHE_PERCENT

	   , CASE
	        WHEN POOL_DATA_P_READS > 0 --AND POOL_DATA_L_READS > 0 --AND POOL_DATA_P_READS > 0
	        THEN DEC(((POOL_DATA_P_READS / DEC(POOL_DATA_L_READS,10,2)) * 100.0),10,2)
	        ELSE NULL
	     END AS PCT_P_READS

       ,'----' AS "----"

            ,CASE
		WHEN TOTAL_COMPILE_TIME > 0 AND TOTAL_COMPILATIONS > 0
		THEN (TOTAL_COMPILE_TIME * 1.0) / (TOTAL_COMPILATIONS * 1.0)
		ELSE NULL
		END AS AVG_COMPILATION_TIME 
	    ,CASE
                WHEN APP_RQSTS_COMPLETED_TOTAL > 0 AND TOTAL_CPU_TIME > 0
                THEN (TOTAL_CPU_TIME / APP_RQSTS_COMPLETED_TOTAL)
                ELSE NULL
             END AS AVG_RQST_CPU_TIME
	       
			--,POOL_FAILED_ASYNC_REQS, POOL_QUEUED_ASYNC_REQS --ESSE AQUI E SO PARA VALIDACAO

			--,((pool_data_lbp_pages_found - pool_async_data_lbp_pages_found) / (pool_data_l_reads + pool_temp_data_l_reads)) * 100

			--average number of sectors that are read by a direct read:
	    ,CASE 
	         WHEN DIRECT_READS > 0 --AND DIRECT_READ_REQS > 0
		 THEN DIRECT_READS / DIRECT_READ_REQS 
		 ELSE NULL
	     END AS AVG_SECTORS_READ_BY_DIRECT_READ
	    ,CASE
		 WHEN DIRECT_READ_TIME > 0 AND DIRECT_READS > 0 
		 THEN DIRECT_READ_TIME / DIRECT_READS 
             END AS AVG_TIME_READ_SECTOR
	    ,CASE 
		 WHEN DIRECT_READ_TIME > 0 AND DIRECT_READ_REQS > 0 
		 THEN DIRECT_READ_TIME / DIRECT_READ_REQS 
	     END AS AVG_TIME_READ_REQ
	    ,CASE 
		 WHEN DIRECT_WRITE_TIME > 0 AND DIRECT_WRITES > 0
		 THEN DIRECT_WRITE_TIME / DIRECT_WRITES
	     END AS AVG_TIME_WRITE_SECTOR
	    ,CASE 
		 WHEN DIRECT_WRITE_TIME > 0 AND DIRECT_WRITE_REQS > 0
		 THEN DIRECT_WRITE_TIME / DIRECT_WRITE_REQS
	     END AS AVG_TIME_WRITE_REQ
	    ,CASE
		 WHEN POOL_READ_TIME > 0 AND (POOL_DATA_P_READS + POOL_INDEX_P_READS + POOL_DATA_L_READS + POOL_INDEX_L_READS) > 0
		 THEN (POOL_READ_TIME * 1.0) / ((POOL_DATA_P_READS + POOL_INDEX_P_READS + POOL_DATA_L_READS + POOL_INDEX_L_READS) * 1.0)
		 ELSE NULL
	     END AS AVG_POOL_READ_TIME
	    ,CASE
		 WHEN POOL_READ_TIME > 0 AND (TOTAL_APP_COMMITS + TOTAL_APP_ROLLBACKS) > 0
		 --THEN CONVERT(DECIMAL(10,2),(POOL_READ_TIME * 1.0) / ((POOL_DATA_P_READS + POOL_INDEX_P_READS) * 1.0))
		 THEN (POOL_READ_TIME * 1.0) / ((TOTAL_APP_COMMITS + TOTAL_APP_ROLLBACKS) * 1.0)
		 ELSE NULL
	     END AS AVG_POOL_READ_TIME_PER_TRANS
            ,CASE
		WHEN POOL_WRITE_TIME > 0 AND (POOL_DATA_WRITES + POOL_INDEX_WRITES) > 0
		THEN (POOL_WRITE_TIME * 1.0) / ((POOL_DATA_WRITES + POOL_INDEX_WRITES) * 1.0)
		ELSE NULL
	     END AS AVG_POOL_WRITE_TIME
	     ,CASE
		WHEN POOL_WRITE_TIME > 0 AND (TOTAL_APP_COMMITS + TOTAL_APP_ROLLBACKS) > 0
		THEN (POOL_WRITE_TIME * 1.0) / ((TOTAL_APP_COMMITS + TOTAL_APP_ROLLBACKS) * 1.0)
		ELSE NULL
	     END AS AVG_POOL_WRITE_TIME_PER_TRANS	     
	     ,CASE
		WHEN TOTAL_SECTION_SORT_TIME > 0 AND TOTAL_SECTION_SORTS > 0
		THEN (TOTAL_SECTION_SORT_TIME * 1.0) / (TOTAL_SECTION_SORTS * 1.0)
		ELSE NULL
	     END AS AVG_SECTION_SORT_TIME,
	     CASE
                 WHEN ACT_TOTAL > 0 AND LOCK_WAITS > 0
                 THEN LOCK_WAITS / ACT_TOTAL
                 ELSE NULL
             END AS AVG_LOCK_WAITS_PER_ACT
             ,CASE
                 WHEN ACT_TOTAL > 0 AND LOCK_TIMEOUTS > 0
                 THEN LOCK_TIMEOUTS / ACT_TOTAL
                 ELSE NULL
             END AS AVG_LOCK_TIMEOUTS_PER_ACT
	     ,CASE
		 WHEN LOCK_WAIT_TIME > 0 AND LOCK_WAITS > 0
		 THEN (LOCK_WAIT_TIME * 1.0) / (LOCK_WAITS * 1.0)
		 ELSE NULL
	    END AS AVG_LOCK_WAIT_TIME,
            CASE
                WHEN ACT_TOTAL > 0 AND DEADLOCKS > 0
                THEN DEADLOCKS / ACT_TOTAL
                ELSE NULL
            END AS AVG_DEADLOCKS_PER_ACT
            ,CASE
                WHEN ACT_TOTAL > 0 AND LOCK_ESCALS > 0
                THEN LOCK_ESCALS / ACT_TOTAL
                ELSE NULL
            END AS AVG_LOCK_ESCALS_PER_ACT
           ,CASE
                WHEN LOG_BUFFER_WAIT_TIME > 0 AND NUM_LOG_BUFFER_FULL > 0
                THEN (LOG_BUFFER_WAIT_TIME / (NUM_LOG_BUFFER_FULL * 1.0))
                ELSE NULL
            END AS AVG_LOG_BUFFER_WAIT_TIME
           ,CASE
                WHEN LOG_DISK_WAIT_TIME > 0 AND LOG_DISK_WAITS_TOTAL > 0
                THEN (LOG_DISK_WAIT_TIME / (LOG_DISK_WAITS_TOTAL * 1.0))
                ELSE NULL
            END AS AVG_LOG_DISK_WAIT_TIME
	    
	   ,'----' AS "----"
	    
	   , POOL_DATA_WRITES + POOL_INDEX_WRITES AS POOL_WRITES
	   , POOL_DATA_P_READS + POOL_INDEX_P_READS AS POOL_P_READS
	   , POOL_DATA_L_READS + POOL_INDEX_L_READS AS POOL_L_READS
           , SORT_OVERFLOWS

	   ,LOG_BUFFER_WAIT_TIME
           ,NUM_LOG_BUFFER_FULL
           ,LOG_DISK_WAIT_TIME
           ,LOG_DISK_WAITS_TOTAL

	   , CAT_CACHE_INSERTS
	   , CAT_CACHE_LOOKUPS
	   , PKG_CACHE_INSERTS
	   , PKG_CACHE_LOOKUPS
	   ,TQ_SORT_HEAP_REQUESTS
	   ,TQ_SORT_HEAP_REJECTIONS
	   ,CASE 
		WHEN TQ_SORT_HEAP_REJECTIONS > 0
		THEN ((TQ_SORT_HEAP_REJECTIONS * 1.0) / (TQ_SORT_HEAP_REQUESTS * 1.0) * 100)
		ELSE NULL
	     END AS PCT_SORT_HEAP_REJECTIONS,
 	     TQ_TOT_SEND_SPILLS
 	    ,'REP' || RepetitionCount AS RepetitionCount
        FROM C
ORDER BY HorarioInicio;
   
    OPEN cReT;
    
END P2;
END P1

/*
CALL NULLID.MRT_Checkup ();
CALL NULLID.MRT_Checkup (1, 10);
*/
#/    


 