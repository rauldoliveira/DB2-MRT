/#
/*
CALL NULLID.MRT_Tables (1, 4); 
*/
/********************************************************************************************************************************
*********************************************************************************************************************************

    Autor: Raul Diego
    E-mail: raul.oliveira@sicoob.com.br
    Data criação: 24/07/2015
    ->Descrição: MRT que faz a análise do comportamento das tabelas no momento. Nao mostra por partição. 
    ->Modo de execuçao: Crie a proc e a execute, informando o tempo de espera entre as execuçoes em segundos e quantas repetiçoes.
 
    Compatibilidade: DB2 LUW 10.1

    Histórico:
        - 00/00/0000: Historico de alteracao

        
*********************************************************************************************************************************
********************************************************************************************************************************/

CREATE OR REPLACE PROCEDURE NULLID.MRT_Tables
     (IN v_Wait SMALLINT DEFAULT 5
     ,IN v_Repetition SMALLINT DEFAULT 3)
     
     LANGUAGE SQL
     RESULT SET 1

P1: BEGIN
 
    DECLARE v_RepetitionCount SMALLINT DEFAULT 1;
    
    DECLARE GLOBAL TEMPORARY TABLE SESSION.MRT_Tables AS (
        SELECT T.TABSCHEMA, T.TABNAME, DP.DATAPARTITIONNAME
                , ROWS_READ, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED
                , TABLE_SCANS, OVERFLOW_ACCESSES, OVERFLOW_CREATES
                , PAGE_REORGS, NO_CHANGE_UPDATES
                , LOCK_WAIT_TIME, LOCK_WAIT_TIME_GLOBAL, LOCK_WAITS, LOCK_WAITS_GLOBAL, LOCK_ESCALS, LOCK_ESCALS_GLOBAL
                , DIRECT_WRITES, DIRECT_WRITE_REQS, DIRECT_READS, DIRECT_READ_REQS
                , OBJECT_DATA_L_READS, OBJECT_DATA_P_READS, OBJECT_DATA_GBP_L_READS
                , OBJECT_DATA_GBP_P_READS, OBJECT_DATA_GBP_INVALID_PAGES
                , OBJECT_DATA_LBP_PAGES_FOUND, OBJECT_DATA_GBP_INDEP_PAGES_FOUND_IN_LBP
                , OBJECT_XDA_L_READS, OBJECT_XDA_P_READS
                , OBJECT_XDA_GBP_L_READS, OBJECT_XDA_GBP_P_READS, OBJECT_XDA_GBP_INVALID_PAGES
                , OBJECT_XDA_LBP_PAGES_FOUND
                , OBJECT_XDA_GBP_INDEP_PAGES_FOUND_IN_LBP, NUM_PAGE_DICT_BUILT
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,1 AS RepetitionCount 
        FROM TABLE(MON_GET_TABLE('','',-2)) AS T
        INNER JOIN SYSCAT.DATAPARTITIONS DP
                ON T.TABSCHEMA = DP.TABSCHEMA
                AND T.TABNAME = DP.TABNAME
                AND COALESCE(T.DATA_PARTITION_ID,0) = DP.DATAPARTITIONID 
        WHERE T.TABSCHEMA NOT LIKE '%SYS%'
    ) DEFINITION ONLY
      ON COMMIT DELETE ROWS
      NOT LOGGED ON ROLLBACK DELETE ROWS
      WITH REPLACE;
         
    WHILE v_RepetitionCount <= v_Repetition DO
    
        INSERT INTO SESSION.MRT_Tables
        SELECT T.TABSCHEMA, T.TABNAME,DP.DATAPARTITIONNAME
                , ROWS_READ, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED
                , TABLE_SCANS, OVERFLOW_ACCESSES, OVERFLOW_CREATES
                , PAGE_REORGS, NO_CHANGE_UPDATES
                , LOCK_WAIT_TIME, LOCK_WAIT_TIME_GLOBAL, LOCK_WAITS, LOCK_WAITS_GLOBAL, LOCK_ESCALS, LOCK_ESCALS_GLOBAL
                , DIRECT_WRITES, DIRECT_WRITE_REQS, DIRECT_READS, DIRECT_READ_REQS
                , OBJECT_DATA_L_READS, OBJECT_DATA_P_READS, OBJECT_DATA_GBP_L_READS
                , OBJECT_DATA_GBP_P_READS, OBJECT_DATA_GBP_INVALID_PAGES
                , OBJECT_DATA_LBP_PAGES_FOUND, OBJECT_DATA_GBP_INDEP_PAGES_FOUND_IN_LBP
                , OBJECT_XDA_L_READS, OBJECT_XDA_P_READS
                , OBJECT_XDA_GBP_L_READS, OBJECT_XDA_GBP_P_READS, OBJECT_XDA_GBP_INVALID_PAGES
                , OBJECT_XDA_LBP_PAGES_FOUND
                , OBJECT_XDA_GBP_INDEP_PAGES_FOUND_IN_LBP, NUM_PAGE_DICT_BUILT
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,v_RepetitionCount AS RepetitionCount 
        FROM TABLE(MON_GET_TABLE('','',-2)) AS T 
        INNER JOIN SYSCAT.DATAPARTITIONS DP
                ON T.TABSCHEMA = DP.TABSCHEMA
                AND T.TABNAME = DP.TABNAME
                AND COALESCE(T.DATA_PARTITION_ID,0) = DP.DATAPARTITIONID 
        WHERE T.TABSCHEMA NOT LIKE '%SYS%';
    
        SET v_RepetitionCount = v_RepetitionCount + 1;
        
        --CALL NULLID.WAITFOR(v_Wait);
        CALL NULLID.WAITFOR_MRT(v_Wait, 'MRT_Tables');

    END WHILE;    
    
P2: BEGIN

    DECLARE cRet CURSOR WITH RETURN FOR
        WITH DIFF AS (
        SELECT TO_CHAR(T1.DataRegistro,'HH24:MI:SS') AS HoraInicio
                ,TO_CHAR(T2.DataRegistro,'HH24:MI:SS') AS HoraFim
                ,T2.TABSCHEMA, T2.TABNAME, T2.DATAPARTITIONNAME
                , T2.ROWS_READ - T1.ROWS_READ AS ROWS_READ
                , T2.ROWS_INSERTED - T1.ROWS_INSERTED AS ROWS_INSERTED
                , T2.ROWS_UPDATED - T1.ROWS_UPDATED AS ROWS_UPDATED
                , T2.ROWS_DELETED - T1.ROWS_DELETED AS ROWS_DELETED
                , T2.TABLE_SCANS - T1.TABLE_SCANS AS TABLE_SCANS
                , T2.OVERFLOW_ACCESSES - T1.OVERFLOW_ACCESSES AS OVERFLOW_ACCESSES
                , T2.OVERFLOW_CREATES - T1.OVERFLOW_CREATES AS OVERFLOW_CREATES
                , T2.OBJECT_DATA_L_READS - T1.OBJECT_DATA_L_READS AS OBJECT_DATA_L_READS
                , T2.OBJECT_DATA_P_READS - T1.OBJECT_DATA_P_READS AS OBJECT_DATA_P_READS
                , T2.OBJECT_XDA_L_READS - T1.OBJECT_XDA_L_READS AS OBJECT_XDA_L_READS
                , T2.OBJECT_XDA_P_READS - T1.OBJECT_XDA_P_READS AS OBJECT_XDA_P_READS
                , T2.LOCK_WAIT_TIME - T1.LOCK_WAIT_TIME AS LOCK_WAIT_TIME
                , T2.LOCK_WAIT_TIME_GLOBAL - T1.LOCK_WAIT_TIME_GLOBAL AS LOCK_WAIT_TIME_GLOBAL
                , T2.LOCK_WAITS - T1.LOCK_WAITS AS LOCK_WAITS
                , T2.LOCK_WAITS_GLOBAL - T1.LOCK_WAITS_GLOBAL AS LOCK_WAITS_GLOBAL
                , T2.LOCK_ESCALS - T1.LOCK_ESCALS AS LOCK_ESCALS
                , T2.LOCK_ESCALS_GLOBAL - T1.LOCK_ESCALS_GLOBAL AS LOCK_ESCALS_GLOBAL
                , T2.PAGE_REORGS - T1.PAGE_REORGS AS PAGE_REORGS
                , T2.NO_CHANGE_UPDATES - T1.NO_CHANGE_UPDATES AS NO_CHANGE_UPDATES
                , T2.DIRECT_WRITES - T1.DIRECT_WRITES AS DIRECT_WRITES
                , T2.DIRECT_WRITE_REQS - T1.DIRECT_WRITE_REQS AS DIRECT_WRITE_REQS
                , T2.DIRECT_READS - T1.DIRECT_READS AS DIRECT_READS
                , T2.DIRECT_READ_REQS - T1.DIRECT_READ_REQS AS DIRECT_READ_REQS
                , T2.DATAREGISTRO
                , T2.REPETITIONCOUNT
        --, OBJECT_DATA_GBP_L_READS, OBJECT_DATA_GBP_P_READS
        --, OBJECT_DATA_GBP_INVALID_PAGES, OBJECT_DATA_LBP_PAGES_FOUND, OBJECT_DATA_GBP_INDEP_PAGES_FOUND_IN_LBP, OBJECT_XDA_L_READS, OBJECT_XDA_P_READS
        --, OBJECT_XDA_GBP_L_READS, OBJECT_XDA_GBP_P_READS, OBJECT_XDA_GBP_INVALID_PAGES, OBJECT_XDA_LBP_PAGES_FOUND, OBJECT_XDA_GBP_INDEP_PAGES_FOUND_IN_LBP, NUM_PAGE_DICT_BUILT
        FROM SESSION.MRT_Tables T1
        LEFT JOIN SESSION.MRT_Tables T2 
                ON T1.TABSCHEMA = T2.TABSCHEMA
                AND T1.TABNAME = T2.TABNAME
                AND T1.DATAPARTITIONNAME = T2.DATAPARTITIONNAME
                AND T1.RepetitionCount < T2.RepetitionCount
                AND T2.RepetitionCount - T1.RepetitionCount = 1
        )
        SELECT DIFF.*
        ,'REP' || DIFF.REPETITIONCOUNT AS RepetitionCount
        FROM DIFF
        WHERE TABSCHEMA IS NOT NULL  
        AND (ROWS_READ <> 0 OR ROWS_INSERTED <> 0 OR ROWS_UPDATED <> 0 OR ROWS_DELETED <> 0 OR TABLE_SCANS <> 0 
                OR OVERFLOW_ACCESSES <> 0 OR  OVERFLOW_CREATES <> 0 OR OBJECT_DATA_L_READS <> 0 OR OBJECT_DATA_P_READS <> 0 
                OR OBJECT_XDA_L_READS <> 0 OR OBJECT_XDA_P_READS <> 0 OR  LOCK_WAIT_TIME <> 0 OR LOCK_WAIT_TIME_GLOBAL <> 0 
                OR LOCK_WAITS <> 0 OR LOCK_WAITS_GLOBAL <> 0 OR LOCK_ESCALS <> 0 OR PAGE_REORGS <> 0 OR  DIRECT_WRITES <> 0 
                OR DIRECT_WRITE_REQS <> 0 OR DIRECT_READS <> 0 OR DIRECT_READ_REQS <> 0
             )
        ORDER BY HoraInicio, TABSCHEMA, TABNAME
        ;
    
    OPEN cReT;
    
END P2;
END P1

/*
CALL NULLID.MRT_Tables ();
CALL NULLID.MRT_Tables (1, 4); 
*/
#/    


