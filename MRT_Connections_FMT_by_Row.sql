
/#
/*
CALL NULLID.MRT_Connections_FMT_by_Row (1, 10);
*/

/********************************************************************************************************************************
*********************************************************************************************************************************

    Autor: Raul Diego
    E-mail: raul.oliveira@sicoob.com.br
    Data criação: 24/07/2015
    ->Descrição: MRT que faz a análise de quais conexões estão trabalhando no momento e mostra quais sao as esperas que 
        cada conexão está sofrendo. 
    ->Modo de execuçao: Execute o scprit de uma única vez a partir o DBVisualizer e consulte as abas geradas por statement
 
    Compatibilidade: DB2 LUW 10.1

    Histórico:
        - 00/00/0000: Historico de alteracao

        
*********************************************************************************************************************************
********************************************************************************************************************************/

CREATE OR REPLACE PROCEDURE NULLID.MRT_Connections_FMT_by_Row
     (IN v_Wait SMALLINT DEFAULT 5
     ,IN v_Repetition SMALLINT DEFAULT 3)
     
     LANGUAGE SQL
     RESULT SET 1

P1: BEGIN
 
    DECLARE v_RepetitionCount SMALLINT DEFAULT 1;
    
    DECLARE GLOBAL TEMPORARY TABLE SESSION.MRT_Connections_FMT_by_Row AS (
        SELECT CURRENT_SERVER AS BANCO
                , C.APPLICATION_HANDLE, X.METRIC_NAME, X.TOTAL_TIME_VALUE, X.COUNT, X.PARENT_METRIC_NAME
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,1 AS RepetitionCount
        FROM TABLE(mon_get_connection_details(NULL ,-2))  AS C ,
            TABLE(mon_format_xml_times_by_row(C.details)) AS X
    ) DEFINITION ONLY
           ON COMMIT DELETE ROWS
           NOT LOGGED ON ROLLBACK DELETE ROWS
           WITH REPLACE;
         
    WHILE v_RepetitionCount <= v_Repetition DO
    
        INSERT INTO SESSION.MRT_Connections_FMT_by_Row
        SELECT CURRENT_SERVER AS BANCO
                , C.APPLICATION_HANDLE, X.METRIC_NAME, X.TOTAL_TIME_VALUE, X.COUNT, X.PARENT_METRIC_NAME
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,v_RepetitionCount AS RepetitionCount
        FROM TABLE(mon_get_connection_details(NULL ,-2))  AS C ,
            TABLE(mon_format_xml_times_by_row(C.details)) AS X
        WHERE PARENT_METRIC_NAME IS NOT NULL;
    
        SET v_RepetitionCount = v_RepetitionCount + 1;
        
        --CALL NULLID.WAITFOR(v_Wait);
        CALL NULLID.WAITFOR_MRT(v_Wait, 'MRT_ConnectionsFMTbyROW');

    END WHILE;    
    
P2: BEGIN

    DECLARE cRet CURSOR WITH RETURN FOR
--      SELECT * 
--      FROM SESSION.MRT_Connections_FMT_by_Row 
--      WHERE 1=1
--          AND APPLICATION_HANDLE = 24
--          AND METRIC_NAME = 'TOTAL_WAIT_TIME'
--      ORDER BY REPETITIONCOUNT, APPLICATION_HANDLE, TOTAL_TIME_VALUE DESC, COUNT DESC;

        WITH C AS (
        SELECT T2.BANCO
                ,T2.APPLICATION_HANDLE
                ,T2.METRIC_NAME
                ,T2.PARENT_METRIC_NAME
                ,T2.TOTAL_TIME_VALUE - T1.TOTAL_TIME_VALUE AS TOTAL_TIME_VALUE
                ,T2.COUNT - T1.COUNT AS COUNT
                ,T2.DATAREGISTRO
                ,T2.REPETITIONCOUNT 
        FROM SESSION.MRT_Connections_FMT_by_Row T1
        LEFT JOIN SESSION.MRT_Connections_FMT_by_Row T2 
                --ON T1.APPLICATION_HANDLE = T2.APPLICATION_HANDLE
                ON T1.BANCO = T2.BANCO
                AND T1.APPLICATION_HANDLE = T2.APPLICATION_HANDLE
                AND T1.METRIC_NAME = T2.METRIC_NAME
                AND T1.PARENT_METRIC_NAME = T2.PARENT_METRIC_NAME
                AND T1.RepetitionCount < T2.RepetitionCount
                AND T2.RepetitionCount - T1.RepetitionCount = 1
        )
        SELECT C.BANCO, C.APPLICATION_HANDLE, C.METRIC_NAME, C.TOTAL_TIME_VALUE
            , C.COUNT, C.PARENT_METRIC_NAME, C.DATAREGISTRO, 'REP' || C.REPETITIONCOUNT AS REPETITION_COUNT
        FROM C
        WHERE 1=1
            AND C.METRIC_NAME IS NOT NULL
            AND (C.TOTAL_TIME_VALUE <> 0 OR C.COUNT <> 0)
        ORDER BY BANCO, APPLICATION_HANDLE, TOTAL_TIME_VALUE DESC, COUNT DESC
        ;
    
    OPEN cReT;
    
END P2;
END P1

--  CALL NULLID.MRT_Latch ();
--  CALL NULLID.MRT_Connections_FMT_by_Row (1, 3);

#/    


