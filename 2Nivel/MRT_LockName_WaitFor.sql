/#
/*
CALL NULLID.LockName_WaitFor(1,10)
*/
/********************************************************************************************************************************
*********************************************************************************************************************************

    Autor: Raul Diego
    E-mail: raul.oliveira@sicoob.com.br
    Data criação: 24/07/2015
    ->Descrição: MRT que mostra os objectos que sofreram bloqueio. 
    ->Modo de execuçao: Crie a proc e a execute, informando o tempo de espera entre as execuçoes em segundos e quantas repetiçoes.
 
    Compatibilidade: DB2 LUW 10.1

    Histórico:
        - 00/00/0000: Historico de alteracao

        
*********************************************************************************************************************************
********************************************************************************************************************************/

CREATE OR REPLACE PROCEDURE NULLID.LockName_WaitFor
     (IN v_Wait SMALLINT DEFAULT 5
     ,IN v_Repetition SMALLINT DEFAULT 3)

    
     LANGUAGE SQL
     RESULT SET 1

P1: BEGIN

DECLARE vLockName VARCHAR(30);
DECLARE v_RepetitionCount SMALLINT DEFAULT 1;
--DECLARE v_Repetition SMALLINT DEFAULT 1;
DECLARE vLockRepetitionCount SMALLINT DEFAULT 1;
DECLARE vDataLock TIMESTAMP;

DECLARE GLOBAL TEMPORARY TABLE SESSION.MRTLockNameTEMP AS (
        SELECT LOCK_NAME
        ,CURRENT_TIMESTAMP AS DATAREGISTRO
        , 1 AS RepetitionCount
        FROM TABLE (MON_GET_APPL_LOCKWAIT(NULL, -2)) T
    ) DEFINITION ONLY
           ON COMMIT DELETE ROWS
           NOT LOGGED ON ROLLBACK DELETE ROWS
           WITH REPLACE;
           
DECLARE GLOBAL TEMPORARY TABLE SESSION.MRTResultLockName AS (
        SELECT  'FFFA8000005D07A53314004852' AS LOCK_NAME
            ,MAX(DECODE(L.NAME, 'LOCK_OBJECT_TYPE', L.VALUE)) AS LOCK_OBJECT_TYPE
            ,MAX(DECODE(L.NAME, 'ROWID', L.VALUE)) AS ROW_ID
            ,MAX(DECODE(L.NAME, 'DATA_PARTITION_ID', L.VALUE)) AS DATA_PARTITION_ID
            ,MAX(DECODE(L.NAME, 'PAGEID', L.VALUE)) AS PAGEID
            ,MAX(DECODE(L.NAME, 'TABSCHEMA', L.VALUE)) AS TAB_SCHEMA
            ,MAX(DECODE(L.NAME, 'TABNAME', L.VALUE)) AS TAB_NAME
            ,CURRENT_TIMESTAMP AS DATAREGISTRO
            ,1 AS REPETITIONCOUNT
        FROM TABLE( MON_FORMAT_LOCK_NAME('FFFA8000005D07A53314004852')) as L
    ) DEFINITION ONLY
           ON COMMIT DELETE ROWS
           NOT LOGGED ON ROLLBACK DELETE ROWS
           WITH REPLACE;
           
    WHILE v_RepetitionCount <= v_Repetition DO

        INSERT INTO SESSION.MRTLockNameTEMP
        SELECT LOCK_NAME, CURRENT_TIMESTAMP AS DataRegistro, v_RepetitionCount AS RepetitionCount
        FROM TABLE (MON_GET_APPL_LOCKWAIT(NULL, -2)) T;
        
        SET v_RepetitionCount = v_RepetitionCount + 1;
                      
        --CALL NULLID.WAITFOR(v_Wait);
        CALL NULLID.WAITFOR_MRT(v_Wait, 'MRT_LockName');

    END WHILE;    


--P2: BEGIN 

WHILE (SELECT COUNT(*) FROM SESSION.MRTLockNameTEMP) > 0
DO
        
        SET vLockName = (SELECT LOCK_NAME FROM SESSION.MRTLockNameTEMP FETCH FIRST 1 ROWS ONLY);
        SET vLockRepetitionCount = (SELECT REPETITIONCOUNT FROM SESSION.MRTLockNameTEMP WHERE LOCK_NAME = vLockName FETCH FIRST 1 ROWS ONLY);
        SET vDataLock = (SELECT TIMESTAMP(DataRegistro) FROM SESSION.MRTLockNameTEMP WHERE LOCK_NAME = vLockName AND RepetitionCount = vLockRepetitionCount FETCH FIRST 1 ROWS ONLY);
        
        INSERT INTO SESSION.MRTResultLockName
        SELECT vLockName AS Lock_Name 
            ,MAX(DECODE(L.NAME, 'LOCK_OBJECT_TYPE', L.VALUE)) AS LOCK_OBJECT_TYPE
            ,MAX(DECODE(L.NAME, 'ROWID', L.VALUE)) AS ROW_ID
            ,MAX(DECODE(L.NAME, 'DATA_PARTITION_ID', L.VALUE)) AS DATA_PARTITION_ID
            ,MAX(DECODE(L.NAME, 'PAGEID', L.VALUE)) AS PAGEID
            ,MAX(DECODE(L.NAME, 'TABSCHEMA', L.VALUE)) AS TAB_SCHEMA
            ,MAX(DECODE(L.NAME, 'TABNAME', L.VALUE)) AS TAB_NAME
            --,CURRENT_TIMESTAMP AS DATAREGISTRO
            ,vDataLock as DATAREGISTRO
             ,vLockRepetitionCount AS REPETITIONCOUNT
        FROM TABLE(MON_FORMAT_LOCK_NAME(''||vLockName||'')) as L;
        
        DELETE FROM SESSION.MRTLockNameTEMP WHERE LOCK_NAME = vLockName AND RepetitionCount = vLockRepetitionCount ;

END WHILE;

P2: BEGIN
--P3: BEGIN

    DECLARE cRet CURSOR WITH RETURN FOR
        SELECT 
        --T.*   --, TO_CHAR(T.DATAREGISTRO,'HH24:MI:SS') AS Horario
        T.LOCK_NAME, T.LOCK_OBJECT_TYPE, T.ROW_ID, T.DATA_PARTITION_ID, T.PAGEID, T.TAB_SCHEMA, T.TAB_NAME, T.DATAREGISTRO
        --,DATA_PARTITION_ID * POWER(2,48) + PAGEID * POWER(2,16) + ROWID AS RID
        ,'REP' || T.REPETITIONCOUNT AS RepetitionCount
        FROM SESSION.MRTResultLockName T
        ORDER BY REPETITIONCOUNT;    
    OPEN cReT;

--END P3;    
END P2;
END P1

--  CALL NULLID.LockName_WaitFor(1,5)

#/    

