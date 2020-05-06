/#
CREATE OR REPLACE PROCEDURE NULLID.MRT_MemoryPools
     (IN v_Wait SMALLINT DEFAULT 5
     ,IN v_Repetition SMALLINT DEFAULT 3)
     
     LANGUAGE SQL
     RESULT SET 1

P1: BEGIN
 
    DECLARE v_RepetitionCount SMALLINT DEFAULT 1;
    
    DECLARE GLOBAL TEMPORARY TABLE SESSION.MRT_MemoryPools AS (
        SELECT T.MEMORY_SET_TYPE
                ,T.MEMORY_POOL_TYPE
                ,SUM(T.MEMORY_POOL_USED)/1024 as MEMORY_POOL_USED_MB
                --,SUM(T.MEMORY_POOL_USED_HWM) /1024 AS MEMORY_POOL_USED_HWM_MB
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,1 AS RepetitionCount
        FROM TABLE(MON_GET_MEMORY_POOL(NULL, CURRENT_SERVER, -2)) T
        GROUP BY T.MEMORY_POOL_TYPE, T.MEMORY_SET_TYPE
        ORDER BY T.MEMORY_SET_TYPE, MEMORY_POOL_USED_MB DESC
    ) DEFINITION ONLY
           ON COMMIT DELETE ROWS
           NOT LOGGED ON ROLLBACK DELETE ROWS
           WITH REPLACE;
         
    WHILE v_RepetitionCount <= v_Repetition DO
    
        INSERT INTO SESSION.MRT_MemoryPools
        SELECT T.MEMORY_SET_TYPE
                ,T.MEMORY_POOL_TYPE
                ,SUM(T.MEMORY_POOL_USED)/1024 as MEMORY_POOL_USED_MB
                --,SUM(T.MEMORY_POOL_USED_HWM) /1024 AS MEMORY_POOL_USED_HWM_MB
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,v_RepetitionCount AS RepetitionCount 
        FROM TABLE(MON_GET_MEMORY_POOL(NULL, CURRENT_SERVER, -2)) T
        GROUP BY T.MEMORY_POOL_TYPE, T.MEMORY_SET_TYPE
        ORDER BY T.MEMORY_SET_TYPE, MEMORY_POOL_USED_MB DESC
        WITH UR;

    
        SET v_RepetitionCount = v_RepetitionCount + 1;
        
        --CALL NULLID.WAITFOR(v_Wait);
        CALL NULLID.WAITFOR_MRT(v_Wait, 'MRT_MemoryPools');

    END WHILE;    
    
P2: BEGIN

    DECLARE cRet CURSOR WITH RETURN FOR
        SELECT * FROM SESSION.MRT_MemoryPools
--        SELECT T.MEMORY_SET_TYPE
--                ,T.MEMORY_POOL_TYPE
--                ,T.MEMORY_POOL_USED_MB
--                ,T.DATAREGISTRO
--                ,T.RepetitionCount
--        FROM SESSION.MRT_MemoryPools T
--        GROUP BY T.MEMORY_POOL_TYPE, T.MEMORY_SET_TYPE,DATAREGISTRO,RepetitionCount
--        ORDER BY T.MEMORY_SET_TYPE, MEMORY_POOL_USED_MB DESC
        ;
    
    OPEN cReT;
    
END P2;
END P1
#/    


--  CALL NULLID.MRT_MemoryPools (1,5);
--  CALL NULLID.MRT_MemoryPools (2, 3); 