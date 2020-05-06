/#
/*
CALL NULLID.MRT_Latch (1, 10); 
*/

/********************************************************************************************************************************
*********************************************************************************************************************************

    Autor: Raul Diego
    E-mail: raul.oliveira@sicoob.com.br
    Data criação: 24/07/2015
    ->Descrição: MRT que faz a análise dos latches que estao sendo utilizados naquele momento. 
    ->Modo de execuçao: Crie a proc e a execute, informando o tempo de espera entre as execuçoes em segundos e quantas repetiçoes.
 
    Compatibilidade: DB2 LUW 10.1

    Histórico:
        - 00/00/0000: Historico de alteracao

        
*********************************************************************************************************************************
********************************************************************************************************************************/

CREATE OR REPLACE PROCEDURE NULLID.MRT_Latch
     (IN v_Wait SMALLINT DEFAULT 5
     ,IN v_Repetition SMALLINT DEFAULT 3)
     
     LANGUAGE SQL
     RESULT SET 1

P1: BEGIN
 
    DECLARE v_RepetitionCount SMALLINT DEFAULT 1;
    
    DECLARE GLOBAL TEMPORARY TABLE SESSION.MRT_Latch AS (
        SELECT CURRENT_SERVER AS BANCO
                ,LATCH_NAME ,TOTAL_EXTENDED_LATCH_WAITS ,TOTAL_EXTENDED_LATCH_WAIT_TIME
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,1 AS RepetitionCount 
        FROM TABLE(SYSPROC.MON_GET_EXTENDED_LATCH_WAIT(-1))
    ) DEFINITION ONLY
           ON COMMIT DELETE ROWS
           NOT LOGGED ON ROLLBACK DELETE ROWS
           WITH REPLACE;
         
    WHILE v_RepetitionCount <= v_Repetition DO
    
        INSERT INTO SESSION.MRT_Latch
        SELECT CURRENT_SERVER AS BANCO
                ,LATCH_NAME ,TOTAL_EXTENDED_LATCH_WAITS ,TOTAL_EXTENDED_LATCH_WAIT_TIME
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,v_RepetitionCount AS RepetitionCount 
        FROM TABLE(SYSPROC.MON_GET_EXTENDED_LATCH_WAIT(-1));
    
        SET v_RepetitionCount = v_RepetitionCount + 1;
        
        --CALL NULLID.WAITFOR(v_Wait);
        CALL NULLID.WAITFOR_MRT(v_Wait, 'MRT_Latches');
        
        

    END WHILE;    
    
P2: BEGIN

    DECLARE cRet CURSOR WITH RETURN FOR
        WITH C AS (
        SELECT T2.BANCO
                ,T2.LATCH_NAME
                ,T2.TOTAL_EXTENDED_LATCH_WAITS - T1.TOTAL_EXTENDED_LATCH_WAITS AS TOTAL_EXTENDED_LATCH_WAITS
                ,T2.TOTAL_EXTENDED_LATCH_WAIT_TIME - T1.TOTAL_EXTENDED_LATCH_WAIT_TIME AS TOTAL_EXTENDED_LATCH_WAIT_TIME
                ,T2.DATAREGISTRO
                ,T2.REPETITIONCOUNT 
        FROM SESSION.MRT_Latch T1
        LEFT JOIN SESSION.MRT_Latch T2 
                ON T1.BANCO = T2.BANCO
                AND T1.LATCH_NAME = T2.LATCH_NAME
                AND T1.RepetitionCount < T2.RepetitionCount
                AND T2.RepetitionCount - T1.RepetitionCount = 1
        ), C2 AS (
        SELECT
            SUM(T.TOTAL_EXTENDED_LATCH_WAITS) AS SUM_TOTAL_EXTENDED_LATCH_WAITS
            ,SUM(T.TOTAL_EXTENDED_LATCH_WAIT_TIME) AS SUM_TOTAL_EXTENDED_LATCH_WAIT_TIME
            ,T.RepetitionCount
        FROM C AS T 
        GROUP BY T.RepetitionCount
        )
        SELECT TO_CHAR(C.DATAREGISTRO,'HH24:MI:SS') AS Horario
            ,C.LATCH_NAME
            ,TOTAL_EXTENDED_LATCH_WAITS AS WAITS_LATCH_EXTENDED
            ,DECIMAL((C.TOTAL_EXTENDED_LATCH_WAITS * 100) / (SELECT
                                                                CASE
                                                                  WHEN C2.SUM_TOTAL_EXTENDED_LATCH_WAITS > 0
                                                                  THEN C2.SUM_TOTAL_EXTENDED_LATCH_WAITS
                                                                  ELSE 1
                                                                  END SUM_TOTAL_EXTENDED_LATCH_WAITS     
                                                                FROM C2
                                                                WHERE C.RepetitionCount = C2.RepetitionCount
                                                                FETCH FIRST 1 ROWS ONLY
                                                                ) ,15,2) AS PCT_WAITS_LATCH
            ,C.TOTAL_EXTENDED_LATCH_WAIT_TIME AS TIME_WAIT_LATCH_EXTENDED
            ,DECIMAL((C.TOTAL_EXTENDED_LATCH_WAIT_TIME * 100) / (SELECT 
                                                                    CASE 
                                                                      WHEN C2.SUM_TOTAL_EXTENDED_LATCH_WAIT_TIME > 0
                                                                      THEN C2.SUM_TOTAL_EXTENDED_LATCH_WAIT_TIME
                                                                      ELSE 1
                                                                      END SUM_TOTAL_EXTENDED_LATCH_WAIT_TIME 
                                                                    FROM C2
                                                                    WHERE C.RepetitionCount = C2.RepetitionCount
                                                                    FETCH FIRST 1 ROWS ONLY
                                                                    ),15,2) AS PCT_TIME_WAIT_LATCH
            ,C.TOTAL_EXTENDED_LATCH_WAIT_TIME / TOTAL_EXTENDED_LATCH_WAITS AS AVG_WAIT_TIME_LATCH 
            ,C.DATAREGISTRO
            ,'REP' || C.REPETITIONCOUNT AS RepetitionCount
            ,C.BANCO 
        FROM C
        WHERE LATCH_NAME IS NOT NULL
        AND (TOTAL_EXTENDED_LATCH_WAITS <> 0 OR TOTAL_EXTENDED_LATCH_WAIT_TIME <> 0)
        ORDER BY BANCO ASC, DATAREGISTRO ASC, TOTAL_EXTENDED_LATCH_WAIT_TIME DESC, TOTAL_EXTENDED_LATCH_WAITS DESC
        ;
    
    OPEN cReT;
    
END P2;
END P1

/*
CALL NULLID.MRT_Latch ();
CALL NULLID.MRT_Latch (1, 10); 
*/
#/    


