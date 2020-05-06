/#
/*
CALL NULLID.MRT_DBSummary ();
CALL NULLID.MRT_DBSummary (1, 10);

--  call monreport.dbsummary(30);
--  call monreport.pkgcache(10);
--  call monreport.connection(10);
*/

/********************************************************************************************************************************
*********************************************************************************************************************************

    Autor: Raul Diego
    E-mail: raul.oliveira@sicoob.com.br
    Data cria??o: 24/07/2015
    ->Descri??o: MRT que mostra exatamente a SYSIBMADM.DB_SUMMARY, mas fazendo os deltas e mostrando dentro do intervalo de tempo 
        que precisar. 
    ->Modo de execu?ao: Crie a proc e a execute, informando o tempo de espera entre as execu?oes em segundos e quantas repeti?oes.
 
    Compatibilidade: DB2 LUW 10.1

    Hist?rico:
        - 00/00/0000: Historico de alteracao

        
*********************************************************************************************************************************
********************************************************************************************************************************/

CREATE OR REPLACE PROCEDURE NULLID.MRT_DBSummary
     (IN v_Wait SMALLINT DEFAULT 5
     ,IN v_Repetition SMALLINT DEFAULT 3)
     
     LANGUAGE SQL
     RESULT SET 1

P1: BEGIN
 
    DECLARE v_RepetitionCount SMALLINT DEFAULT 1;
    
    DECLARE GLOBAL TEMPORARY TABLE SESSION.MRT_DBSummary AS (
    WITH METRICS AS
    (SELECT SUM(TOTAL_APP_COMMITS)                       AS TOTAL_APP_COMMITS ,
            SUM(TOTAL_APP_ROLLBACKS)                     AS TOTAL_APP_ROLLBACKS ,
            SUM(ACT_COMPLETED_TOTAL)                     AS ACT_COMPLETED_TOTAL ,
            SUM(ACT_COMPLETED_TOTAL + ACT_ABORTED_TOTAL)                              AS ACT_TOTAL ,
            SUM(APP_RQSTS_COMPLETED_TOTAL)                            AS APP_RQSTS_COMPLETED_TOTAL ,
            SUM(TOTAL_CPU_TIME)                                                  AS TOTAL_CPU_TIME ,
            SUM(ROWS_READ)                                                            AS ROWS_READ ,
            SUM(ROWS_RETURNED)                                                    AS ROWS_RETURNED ,
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
            SUM(POOL_DATA_LBP_PAGES_FOUND + POOL_INDEX_LBP_PAGES_FOUND + POOL_XDA_LBP_PAGES_FOUND)
                                                         AS BP_PAGES_FOUND ,
            SUM(TOTAL_ROUTINE_TIME)                                          AS TOTAL_ROUTINE_TIME ,
            SUM(POOL_DATA_GBP_L_READS + POOL_INDEX_GBP_L_READS + POOL_XDA_GBP_L_READS) AS
            GBP_L_READS ,
            SUM(POOL_DATA_GBP_P_READS + POOL_INDEX_GBP_P_READS + POOL_XDA_GBP_P_READS) AS
                                 GBP_P_READS ,
            SUM(CF_WAIT_TIME)                   AS CF_WAIT_TIME ,
            SUM(RECLAIM_WAIT_TIME)              AS RECLAIM_WAIT_TIME ,
            SUM(SPACEMAPPAGE_RECLAIM_WAIT_TIME) AS SPACEMAPPAGE_RECLAIM_WAIT_TIME
        FROM TABLE(MON_GET_SERVICE_SUBCLASS(NULL ,NULL ,-2))
    )
        SELECT 
                METRICS.*
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,1 AS RepetitionCount   
        FROM METRICS
    ) DEFINITION ONLY
           ON COMMIT DELETE ROWS
           NOT LOGGED ON ROLLBACK DELETE ROWS
           WITH REPLACE;
         
    WHILE v_RepetitionCount <= v_Repetition DO
    
        INSERT INTO SESSION.MRT_DBSummary
        WITH METRICS AS
        (SELECT SUM(TOTAL_APP_COMMITS)                       AS TOTAL_APP_COMMITS ,
            SUM(TOTAL_APP_ROLLBACKS)                     AS TOTAL_APP_ROLLBACKS ,
            SUM(ACT_COMPLETED_TOTAL)                     AS ACT_COMPLETED_TOTAL ,
            SUM(ACT_COMPLETED_TOTAL + ACT_ABORTED_TOTAL)                              AS ACT_TOTAL ,
            SUM(APP_RQSTS_COMPLETED_TOTAL)                            AS APP_RQSTS_COMPLETED_TOTAL ,
            SUM(TOTAL_CPU_TIME)                                                  AS TOTAL_CPU_TIME ,
            SUM(ROWS_READ)                                                            AS ROWS_READ ,
            SUM(ROWS_RETURNED)                                                    AS ROWS_RETURNED ,
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
            SUM(POOL_DATA_LBP_PAGES_FOUND + POOL_INDEX_LBP_PAGES_FOUND + POOL_XDA_LBP_PAGES_FOUND)
                                                         AS BP_PAGES_FOUND ,
            SUM(TOTAL_ROUTINE_TIME)                                          AS TOTAL_ROUTINE_TIME ,
            SUM(POOL_DATA_GBP_L_READS + POOL_INDEX_GBP_L_READS + POOL_XDA_GBP_L_READS) AS
            GBP_L_READS ,
            SUM(POOL_DATA_GBP_P_READS + POOL_INDEX_GBP_P_READS + POOL_XDA_GBP_P_READS) AS
                                 GBP_P_READS ,
            SUM(CF_WAIT_TIME)                   AS CF_WAIT_TIME ,
            SUM(RECLAIM_WAIT_TIME)              AS RECLAIM_WAIT_TIME ,
            SUM(SPACEMAPPAGE_RECLAIM_WAIT_TIME) AS SPACEMAPPAGE_RECLAIM_WAIT_TIME
        FROM TABLE(MON_GET_SERVICE_SUBCLASS(NULL ,NULL ,-2))
        )
        SELECT 
                METRICS.*  
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,v_RepetitionCount AS RepetitionCount   
        FROM METRICS;        
--        ,CURRENT_TIMESTAMP AS DATAREGISTRO
--        ,v_RepetitionCount AS RepetitionCount    
        SET v_RepetitionCount = v_RepetitionCount + 1;
        
        --CALL NULLID.WAITFOR(v_Wait);
        CALL NULLID.WAITFOR_MRT(v_Wait, 'MRT_DBSummary');

    END WHILE;    
    
P2: BEGIN

    DECLARE cRet CURSOR WITH RETURN FOR
--        SELECT * FROM SESSION.MRT_DBSummary;
        WITH C AS (
        SELECT T2.TOTAL_APP_COMMITS - T1.TOTAL_APP_COMMITS AS TOTAL_APP_COMMITS
                ,T2.TOTAL_APP_ROLLBACKS - T1.TOTAL_APP_ROLLBACKS AS TOTAL_APP_ROLLBACKS
                ,T2.ACT_COMPLETED_TOTAL - T1.ACT_COMPLETED_TOTAL AS ACT_COMPLETED_TOTAL
                ,T2.ACT_TOTAL - T1.ACT_TOTAL AS ACT_TOTAL
                ,T2.APP_RQSTS_COMPLETED_TOTAL - T1.APP_RQSTS_COMPLETED_TOTAL AS APP_RQSTS_COMPLETED_TOTAL
                ,T2.TOTAL_CPU_TIME - T1.TOTAL_CPU_TIME AS TOTAL_CPU_TIME
                ,T2.ROWS_READ - T1.ROWS_READ AS ROWS_READ
                ,T2.ROWS_RETURNED - T1.ROWS_RETURNED AS ROWS_RETURNED
                ,T2.TOTAL_WAIT_TIME - T1.TOTAL_WAIT_TIME AS TOTAL_WAIT_TIME
                ,T2.TOTAL_RQST_TIME - T1.TOTAL_RQST_TIME AS TOTAL_RQST_TIME
                ,T2.TOTAL_ACT_WAIT_TIME - T1.TOTAL_ACT_WAIT_TIME AS TOTAL_ACT_WAIT_TIME
                ,T2.TOTAL_ACT_TIME - T1.TOTAL_ACT_TIME AS TOTAL_ACT_TIME
                ,T2.IO_WAIT_TIME - T1.IO_WAIT_TIME AS IO_WAIT_TIME
                ,T2.LOCK_WAIT_TIME - T1.LOCK_WAIT_TIME AS LOCK_WAIT_TIME
                ,T2.AGENT_WAIT_TIME - T1.AGENT_WAIT_TIME AS AGENT_WAIT_TIME
                ,T2.LOCK_WAITS - T1.LOCK_WAITS AS LOCK_WAITS
                ,T2.LOCK_TIMEOUTS - T1.LOCK_TIMEOUTS AS LOCK_TIMEOUTS
                ,T2.DEADLOCKS - T1.DEADLOCKS AS DEADLOCKS
                ,T2.LOCK_ESCALS - T1.LOCK_ESCALS AS LOCK_ESCALS
                ,T2.NETWORK_WAIT_TIME - T1.NETWORK_WAIT_TIME AS NETWORK_WAIT_TIME
                ,T2.PROC_TIME - T1.PROC_TIME AS PROC_TIME
                ,T2.SECTION_PROC_TIME - T1.SECTION_PROC_TIME AS SECTION_PROC_TIME
                ,T2.SECTION_SORT_PROC_TIME - T1.SECTION_SORT_PROC_TIME AS SECTION_SORT_PROC_TIME
                ,T2.COMPILE_PROC_TIME - T1.COMPILE_PROC_TIME AS COMPILE_PROC_TIME
                ,T2.TRANSACT_END_PROC_TIME - T1.TRANSACT_END_PROC_TIME AS TRANSACT_END_PROC_TIME
                ,T2.UTILS_PROC_TIME - T1.UTILS_PROC_TIME AS UTILS_PROC_TIME
                ,T2.L_READS - T1.L_READS AS L_READS
                ,T2.BP_PAGES_FOUND - T1.BP_PAGES_FOUND AS BP_PAGES_FOUND 
                ,T2.TOTAL_ROUTINE_TIME - T1.TOTAL_ROUTINE_TIME AS TOTAL_ROUTINE_TIME
                ,T2.GBP_L_READS - T1.GBP_L_READS AS GBP_L_READS
                ,T2.GBP_P_READS - T1.GBP_P_READS AS GBP_P_READS
                ,T2.CF_WAIT_TIME - T1.CF_WAIT_TIME AS CF_WAIT_TIME
                ,T2.RECLAIM_WAIT_TIME - T1.RECLAIM_WAIT_TIME AS RECLAIM_WAIT_TIME
                ,T2.SPACEMAPPAGE_RECLAIM_WAIT_TIME - T1.SPACEMAPPAGE_RECLAIM_WAIT_TIME AS SPACEMAPPAGE_RECLAIM_WAIT_TIME
                ,T2.DATAREGISTRO
                ,T2.REPETITIONCOUNT
        FROM SESSION.MRT_DBSummary T1
        LEFT JOIN SESSION.MRT_DBSummary T2 
                --ON T1.BANCO = T2.BANCO
                --AND T1.LATCH_NAME = T2.LATCH_NAME
                ON T1.RepetitionCount < T2.RepetitionCount
                AND T2.RepetitionCount - T1.RepetitionCount = 1
        )
        SELECT TO_CHAR(C.DATAREGISTRO,'HH24:MI:SS') AS Horario ,
            TOTAL_APP_COMMITS ,
            TOTAL_APP_ROLLBACKS ,
            ACT_COMPLETED_TOTAL ,
            TOTAL_CPU_TIME, -- RETIRAR ESSA AQUI, S? PRA TESTE
            APP_RQSTS_COMPLETED_TOTAL ,
            CASE
                WHEN APP_RQSTS_COMPLETED_TOTAL > 0
                THEN (TOTAL_CPU_TIME / APP_RQSTS_COMPLETED_TOTAL)
                ELSE NULL
            END AS AVG_RQST_CPU_TIME ,
            CASE
                WHEN TOTAL_RQST_TIME > 0
                THEN DEC(FLOAT(TOTAL_ROUTINE_TIME) / FLOAT(TOTAL_RQST_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS ROUTINE_TIME_RQST_PERCENT ,
            CASE
                WHEN TOTAL_RQST_TIME > 0
                THEN DEC(FLOAT(TOTAL_WAIT_TIME) / FLOAT(TOTAL_RQST_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS RQST_WAIT_TIME_PERCENT ,
            CASE
                WHEN TOTAL_ACT_TIME > 0
                THEN DEC(FLOAT(TOTAL_ACT_WAIT_TIME) / FLOAT(TOTAL_ACT_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS ACT_WAIT_TIME_PERCENT ,
            CASE
                WHEN TOTAL_WAIT_TIME > 0
                THEN DEC(FLOAT(IO_WAIT_TIME) / FLOAT(TOTAL_WAIT_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS IO_WAIT_TIME_PERCENT ,
            CASE
                WHEN TOTAL_WAIT_TIME > 0
                THEN DEC(FLOAT(LOCK_WAIT_TIME) / FLOAT(TOTAL_WAIT_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS LOCK_WAIT_TIME_PERCENT ,
            CASE
                WHEN TOTAL_WAIT_TIME > 0
                THEN DEC(FLOAT(AGENT_WAIT_TIME) / FLOAT(TOTAL_WAIT_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS AGENT_WAIT_TIME_PERCENT ,
            CASE
                WHEN TOTAL_WAIT_TIME > 0
                THEN DEC(FLOAT(NETWORK_WAIT_TIME) / FLOAT(TOTAL_WAIT_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS NETWORK_WAIT_TIME_PERCENT ,
            CASE
                WHEN PROC_TIME > 0
                THEN DEC(FLOAT(SECTION_PROC_TIME) / FLOAT(PROC_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS SECTION_PROC_TIME_PERCENT ,
            CASE
                WHEN PROC_TIME > 0
                THEN DEC(FLOAT(SECTION_SORT_PROC_TIME) / FLOAT(PROC_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS SECTION_SORT_PROC_TIME_PERCENT ,
            CASE
                WHEN PROC_TIME > 0
                THEN DEC(FLOAT(COMPILE_PROC_TIME) / FLOAT(PROC_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS COMPILE_PROC_TIME_PERCENT ,
            CASE
                WHEN PROC_TIME > 0
                THEN DEC(FLOAT(TRANSACT_END_PROC_TIME) / FLOAT(PROC_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS TRANSACT_END_PROC_TIME_PERCENT ,
            CASE
                WHEN PROC_TIME > 0
                THEN DEC(FLOAT(UTILS_PROC_TIME) / FLOAT(PROC_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS UTILS_PROC_TIME_PERCENT ,
            CASE
                WHEN ACT_TOTAL > 0
                THEN LOCK_WAITS / ACT_TOTAL
                ELSE NULL
            END AS AVG_LOCK_WAITS_PER_ACT ,
            CASE
                WHEN ACT_TOTAL > 0
                THEN LOCK_TIMEOUTS / ACT_TOTAL
                ELSE NULL
            END AS AVG_LOCK_TIMEOUTS_PER_ACT ,
            CASE
                WHEN ACT_TOTAL > 0
                THEN DEADLOCKS / ACT_TOTAL
                ELSE NULL
            END AS AVG_DEADLOCKS_PER_ACT ,
            CASE
                WHEN ACT_TOTAL > 0
                THEN LOCK_ESCALS / ACT_TOTAL
                ELSE NULL
            END AS AVG_LOCK_ESCALS_PER_ACT ,
            CASE
                WHEN ROWS_RETURNED > 0
                THEN ROWS_READ / ROWS_RETURNED
                ELSE NULL
            END AS ROWS_READ_PER_ROWS_RETURNED ,
            CASE
                WHEN L_READS > 0
                THEN DEC((FLOAT(BP_PAGES_FOUND) / FLOAT(L_READS)) * 100 ,5 ,2)
                ELSE NULL
            END AS TOTAL_BP_HIT_RATIO_PERCENT ,
            CASE
                WHEN GBP_L_READS IS NULL
                THEN NULL
                WHEN GBP_L_READS = 0
                THEN NULL
                ELSE DEC((1 - (FLOAT(GBP_P_READS) / FLOAT(GBP_L_READS))) * 100 ,5 ,2)
            END AS TOTAL_GBP_HIT_RATIO_PERCENT ,
            CASE
                WHEN TOTAL_WAIT_TIME > 0
                THEN DEC(FLOAT(CF_WAIT_TIME) / FLOAT(TOTAL_WAIT_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS CF_WAIT_TIME_PERCENT ,
            CASE
                WHEN TOTAL_WAIT_TIME > 0
                THEN DEC(FLOAT(RECLAIM_WAIT_TIME) / FLOAT(TOTAL_WAIT_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS RECLAIM_WAIT_TIME_PERCENT ,
            CASE
                WHEN TOTAL_WAIT_TIME > 0
                THEN DEC(FLOAT(SPACEMAPPAGE_RECLAIM_WAIT_TIME) / FLOAT(TOTAL_WAIT_TIME) * 100 ,5 ,2)
                ELSE NULL
            END AS SPACEMAPPAGE_RECLAIM_WAIT_TIME_PERCENT
            --,RepetitionCount
            ,'REP' || RepetitionCount AS RepetitionCount
        FROM C
        WHERE RepetitionCount IS NOT NULL
        ORDER BY DATAREGISTRO;
   
    OPEN cReT;
    
END P2;
END P1

/*
CALL NULLID.MRT_DBSummary ();
CALL NULLID.MRT_DBSummary (1, 10);
*/
#/    


 