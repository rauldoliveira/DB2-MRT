/#
/*
CALL NULLID.MRT_Index (1, 5); 
*/

/********************************************************************************************************************************
*********************************************************************************************************************************

    Autor: Raul Diego
    E-mail: raul.oliveira@sicoob.com.br
    Data criação: 24/07/2015
    ->Descrição: MRT que faz a análise dos indices que estao sendo usados dentro do espaco de tempo solicitado 
    ->Modo de execuçao: Crie a proc e a execute, informando o tempo de espera entre as execuçoes em segundos e quantas repetiçoes.
 
    Compatibilidade: DB2 LUW 10.1

    Histórico:
        - 00/00/0000: Historico de alteracao

        
*********************************************************************************************************************************
********************************************************************************************************************************/

CREATE OR REPLACE PROCEDURE NULLID.MRT_Index
     (IN v_Wait SMALLINT DEFAULT 5
     ,IN v_Repetition SMALLINT DEFAULT 3)
     
     LANGUAGE SQL
     RESULT SET 1

P1: BEGIN
 
    DECLARE v_RepetitionCount SMALLINT DEFAULT 1;
    
    DECLARE GLOBAL TEMPORARY TABLE SESSION.MRT_Index AS (
        SELECT 
                T.TABSCHEMA, T.TABNAME, I.INDNAME, DP.DATAPARTITIONNAME
                , T.NLEAF, T.INDEX_SCANS, T.INDEX_ONLY_SCANS
                , T.KEY_UPDATES, T.INCLUDE_COL_UPDATES, T.PSEUDO_DELETES, T.DEL_KEYS_CLEANED
                , T.ROOT_NODE_SPLITS, T.INT_NODE_SPLITS, T.BOUNDARY_LEAF_NODE_SPLITS
                , T.NONBOUNDARY_LEAF_NODE_SPLITS, T.PAGE_ALLOCATIONS, T.PSEUDO_EMPTY_PAGES
                , T.EMPTY_PAGES_REUSED, T.EMPTY_PAGES_DELETED, T.PAGES_MERGED
                , T.OBJECT_INDEX_L_READS, T.OBJECT_INDEX_P_READS, T.OBJECT_INDEX_GBP_L_READS
                , T.OBJECT_INDEX_GBP_P_READS, T.OBJECT_INDEX_GBP_INVALID_PAGES
                , T.OBJECT_INDEX_LBP_PAGES_FOUND, T.OBJECT_INDEX_GBP_INDEP_PAGES_FOUND_IN_LBP
                , T.INDEX_JUMP_SCANS
                , CURRENT_TIMESTAMP AS DATAREGISTRO
                , 1 AS RepetitionCount
        FROM TABLE(MON_GET_INDEX('','', -2)) AS T
        INNER JOIN SYSCAT.INDEXES I 
                ON T.TABSCHEMA = I.TABSCHEMA
                AND T.TABNAME = I.TABNAME
                AND T.IID = I.IID
        LEFT JOIN SYSCAT.DATAPARTITIONS DP
                ON T.TABSCHEMA = DP.TABSCHEMA
                AND T.TABNAME = DP.TABNAME
                AND COALESCE(T.DATA_PARTITION_ID, 0) = DP.DATAPARTITIONID
        WHERE 1=1
                AND I.TABSCHEMA NOT IN ('SYSTOOLS', 'SYSIBM', 'SYSIBMADM')
    ) DEFINITION ONLY
           ON COMMIT DELETE ROWS
           NOT LOGGED ON ROLLBACK DELETE ROWS
           WITH REPLACE;
         
    WHILE v_RepetitionCount <= v_Repetition DO
    
        INSERT INTO SESSION.MRT_Index
        SELECT 
                T.TABSCHEMA, T.TABNAME, I.INDNAME, DP.DATAPARTITIONNAME
                , T.NLEAF, T.INDEX_SCANS, T.INDEX_ONLY_SCANS
                , T.KEY_UPDATES, T.INCLUDE_COL_UPDATES, T.PSEUDO_DELETES, T.DEL_KEYS_CLEANED
                , T.ROOT_NODE_SPLITS, T.INT_NODE_SPLITS, T.BOUNDARY_LEAF_NODE_SPLITS
                , T.NONBOUNDARY_LEAF_NODE_SPLITS, T.PAGE_ALLOCATIONS, T.PSEUDO_EMPTY_PAGES
                , T.EMPTY_PAGES_REUSED, T.EMPTY_PAGES_DELETED, T.PAGES_MERGED
                , T.OBJECT_INDEX_L_READS, T.OBJECT_INDEX_P_READS, T.OBJECT_INDEX_GBP_L_READS
                , T.OBJECT_INDEX_GBP_P_READS, T.OBJECT_INDEX_GBP_INVALID_PAGES
                , T.OBJECT_INDEX_LBP_PAGES_FOUND, T.OBJECT_INDEX_GBP_INDEP_PAGES_FOUND_IN_LBP
                , T.INDEX_JUMP_SCANS
                , CURRENT_TIMESTAMP AS DATAREGISTRO
                , v_RepetitionCount AS RepetitionCount
        FROM TABLE(MON_GET_INDEX('','', -2)) AS T
        INNER JOIN SYSCAT.INDEXES I 
                ON T.TABSCHEMA = I.TABSCHEMA
                AND T.TABNAME = I.TABNAME
                AND T.IID = I.IID
        LEFT JOIN SYSCAT.DATAPARTITIONS DP
                ON T.TABSCHEMA = DP.TABSCHEMA
                AND T.TABNAME = DP.TABNAME
                AND COALESCE(T.DATA_PARTITION_ID,0) = DP.DATAPARTITIONID
        WHERE 1=1
                AND I.TABSCHEMA NOT IN ('SYSTOOLS', 'SYSIBM', 'SYSIBMADM');
    
        SET v_RepetitionCount = v_RepetitionCount + 1;
        
        --CALL NULLID.WAITFOR(v_Wait);
        CALL NULLID.WAITFOR_MRT(v_Wait, 'MRT_Index');

    END WHILE;    
    
P2: BEGIN

    DECLARE cRet CURSOR WITH RETURN FOR
        WITH DIFF AS (
        SELECT 
                TO_CHAR(T1.DataRegistro,'HH24:MI:SS') AS HoraInicio
                ,TO_CHAR(T2.DataRegistro,'HH24:MI:SS') AS HoraFim
                , T2.TABSCHEMA
                , T2.TABNAME
                , T2.INDNAME
                , T2.DATAPARTITIONNAME
                , T2.NLEAF
                , T2.NLEAF - T1.NLEAF AS NLEAFDiff
                , T2.INDEX_SCANS - T1.INDEX_SCANS AS INDEX_SCANS
                , T2.INDEX_ONLY_SCANS - T1.INDEX_ONLY_SCANS AS INDEX_ONLY_SCANS
                , T2.KEY_UPDATES - T1.KEY_UPDATES AS KEY_UPDATES
                , T2.INCLUDE_COL_UPDATES - T1.INCLUDE_COL_UPDATES AS INCLUDE_COL_UPDATES
                , T2.PSEUDO_DELETES - T1.PSEUDO_DELETES AS PSEUDO_DELETES
                , T2.DEL_KEYS_CLEANED - T1.DEL_KEYS_CLEANED AS DEL_KEYS_CLEANED
                , T2.ROOT_NODE_SPLITS - T1.ROOT_NODE_SPLITS AS ROOT_NODE_SPLITS
                , T2.INT_NODE_SPLITS - T1.INT_NODE_SPLITS AS INT_NODE_SPLITS
                , T2.BOUNDARY_LEAF_NODE_SPLITS - T1.BOUNDARY_LEAF_NODE_SPLITS AS BOUNDARY_LEAF_NODE_SPLITS
                , T2.NONBOUNDARY_LEAF_NODE_SPLITS - T1.NONBOUNDARY_LEAF_NODE_SPLITS AS NONBOUNDARY_LEAF_NODE_SPLITS
                , T2.PAGE_ALLOCATIONS - T1.PAGE_ALLOCATIONS AS PAGE_ALLOCATIONS
                , T2.PSEUDO_EMPTY_PAGES - T1.PSEUDO_EMPTY_PAGES AS PSEUDO_EMPTY_PAGES
                , T2.EMPTY_PAGES_REUSED - T1.EMPTY_PAGES_REUSED AS EMPTY_PAGES_REUSED
                , T2.EMPTY_PAGES_DELETED - T1.EMPTY_PAGES_DELETED AS EMPTY_PAGES_DELETED
                , T2.PAGES_MERGED - T1.PAGES_MERGED AS PAGES_MERGED
                , T2.OBJECT_INDEX_L_READS - T1.OBJECT_INDEX_L_READS AS OBJECT_INDEX_L_READS
                , T2.OBJECT_INDEX_P_READS - T1.OBJECT_INDEX_P_READS AS OBJECT_INDEX_P_READS
                , T2.OBJECT_INDEX_GBP_L_READS - T1.OBJECT_INDEX_GBP_L_READS AS OBJECT_INDEX_GBP_L_READS
                , T2.OBJECT_INDEX_GBP_P_READS - T1.OBJECT_INDEX_GBP_P_READS AS OBJECT_INDEX_GBP_P_READS
                , T2.OBJECT_INDEX_GBP_INVALID_PAGES - T1.OBJECT_INDEX_GBP_INVALID_PAGES AS OBJECT_INDEX_GBP_INVALID_PAGES
                , T2.OBJECT_INDEX_LBP_PAGES_FOUND - T1.OBJECT_INDEX_LBP_PAGES_FOUND AS OBJECT_INDEX_LBP_PAGES_FOUND
                , T2.OBJECT_INDEX_GBP_INDEP_PAGES_FOUND_IN_LBP - T1.OBJECT_INDEX_GBP_INDEP_PAGES_FOUND_IN_LBP AS OBJECT_INDEX_GBP_INDEP_PAGES_FOUND_IN_LBP
                , T2.INDEX_JUMP_SCANS - T1.INDEX_JUMP_SCANS AS INDEX_JUMP_SCANS
                , 'REP' || T2.RepetitionCount AS RepetitionCount
        FROM SESSION.MRT_Index AS T1
        INNER JOIN SESSION.MRT_Index AS T2
                ON T1.INDNAME = T2.INDNAME
                AND T1.TABSCHEMA = T2.TABSCHEMA
                AND T1.TABNAME = T2.TABNAME
                AND T1.DATAPARTITIONNAME = T2.DATAPARTITIONNAME
                AND T1.RepetitionCount < T2.RepetitionCount
                AND T2.RepetitionCount - T1.RepetitionCount = 1
        )
        SELECT *
        FROM DIFF 
        WHERE 1=1
             AND (INDEX_SCANS <> 0 OR INDEX_ONLY_SCANS <> 0 OR KEY_UPDATES <> 0 OR INCLUDE_COL_UPDATES <> 0 
             OR PSEUDO_DELETES <> 0 OR DEL_KEYS_CLEANED <> 0 OR ROOT_NODE_SPLITS <> 0 OR INT_NODE_SPLITS <> 0 
             OR BOUNDARY_LEAF_NODE_SPLITS <> 0 OR NONBOUNDARY_LEAF_NODE_SPLITS <> 0 OR PAGE_ALLOCATIONS <> 0 
             OR PSEUDO_EMPTY_PAGES <> 0 OR EMPTY_PAGES_REUSED <> 0 OR EMPTY_PAGES_DELETED <> 0 OR PAGES_MERGED <> 0 
             OR OBJECT_INDEX_L_READS <> 0 OR OBJECT_INDEX_P_READS <> 0 OR OBJECT_INDEX_GBP_L_READS <> 0 
             OR OBJECT_INDEX_GBP_P_READS <> 0 OR OBJECT_INDEX_GBP_INVALID_PAGES <> 0 OR OBJECT_INDEX_LBP_PAGES_FOUND <> 0 
             OR OBJECT_INDEX_GBP_INDEP_PAGES_FOUND_IN_LBP <> 0 OR INDEX_JUMP_SCANS <> 0)
        ;
    
    OPEN cReT;
    
END P2;
END P1

/*
CALL NULLID.MRT_Index ();
CALL NULLID.MRT_Index (1, 10);
*/
#/    


 