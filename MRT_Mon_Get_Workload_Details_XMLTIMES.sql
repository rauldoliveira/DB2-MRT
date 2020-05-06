/#
/*
CALL NULLID.MRT_WorkloadDetXMLTIMES (1, 10); 
*/
/********************************************************************************************************************************
*********************************************************************************************************************************

    Autor: Raul Diego
    E-mail: raul.oliveira@sicoob.com.br
    Data cria??o: 24/07/2015
    ->Descri??o: MRT que faz a an?lise das esperas por workload 
    ->Modo de execu?ao: Crie a proc e a execute, informando o tempo de espera entre as execu?oes em segundos e quantas repeti?oes.
 
    Compatibilidade: DB2 LUW 10.1

    Hist?rico:
        - 00/00/0000: Historico de alteracao

        
*********************************************************************************************************************************
********************************************************************************************************************************/

CREATE OR REPLACE PROCEDURE NULLID.MRT_WorkloadDetXMLTIMES
     (IN v_Wait SMALLINT DEFAULT 5
     ,IN v_Repetition SMALLINT DEFAULT 3)
     
     LANGUAGE SQL
     RESULT SET 1

P1: BEGIN
 
    DECLARE v_RepetitionCount SMALLINT DEFAULT 1;
    
    DECLARE GLOBAL TEMPORARY TABLE SESSION.MRT_WorkloadDetXMLTIMES AS (
        SELECT 
                UPPER((SELECT HOST_NAME FROM TABLE(SYSPROC.ENV_GET_SYS_INFO())  T)) AS Servidor
                , CURRENT SERVER AS DATABASE 
                , W.WORKLOAD_NAME, X.METRIC_NAME, X.TOTAL_TIME_VALUE, X.COUNT, X.PARENT_METRIC_NAME
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,1 AS RepetitionCount  
        FROM TABLE( MON_GET_WORKLOAD_DETAILS( NULL ,-2 ) ) W,
        TABLE( MON_FORMAT_XML_TIMES_BY_ROW( W.DETAILS )) X
    ) DEFINITION ONLY
           ON COMMIT DELETE ROWS
           NOT LOGGED ON ROLLBACK DELETE ROWS
           WITH REPLACE;
         
    WHILE v_RepetitionCount <= v_Repetition DO
    
        INSERT INTO SESSION.MRT_WorkloadDetXMLTIMES
        SELECT 
                UPPER((SELECT HOST_NAME FROM TABLE(SYSPROC.ENV_GET_SYS_INFO())  T)) AS Servidor
                , CURRENT SERVER AS DATABASE 
                , W.WORKLOAD_NAME, X.METRIC_NAME, X.TOTAL_TIME_VALUE, X.COUNT, X.PARENT_METRIC_NAME
                ,CURRENT_TIMESTAMP AS DATAREGISTRO
                ,v_RepetitionCount AS RepetitionCount  
        FROM TABLE( MON_GET_WORKLOAD_DETAILS( NULL ,-2 ) ) W,
            TABLE( MON_FORMAT_XML_TIMES_BY_ROW( W.DETAILS )) X;
    
        SET v_RepetitionCount = v_RepetitionCount + 1;
        
        --CALL NULLID.WAITFOR(v_Wait);
        CALL NULLID.WAITFOR_MRT(v_Wait, 'MRT_WorkloadDetXMLTIMES');
        
        

    END WHILE;    
    
P2: BEGIN

    DECLARE cRet CURSOR WITH RETURN FOR
       --SELECT * FROM SESSION.MRT_MonGetWorkloadDetailsXMLTIMES;
       WITH DIFF AS (
        SELECT 
                T2.Servidor, T2.DataBase
                ,TO_CHAR(T1.DataRegistro,'HH24:MI:SS') AS HoraInicio
                ,TO_CHAR(T2.DataRegistro,'HH24:MI:SS') AS HoraFim
                ,T2.WORKLOAD_NAME  
                ,T2.METRIC_NAME
                ,T2.TOTAL_TIME_VALUE - T1.TOTAL_TIME_VALUE AS TOTAL_TIME_VALUE 
                ,T2.COUNT - T1.COUNT AS COUNT
                ,T2.PARENT_METRIC_NAME
                ,T2.RepetitionCount
        FROM SESSION.MRT_WorkloadDetXMLTIMES AS T1
        INNER JOIN SESSION.MRT_WorkloadDetXMLTIMES AS T2
                ON T1.Servidor = T2.Servidor
                AND T1.DataBase = T2.DataBase
                AND T1.WORKLOAD_NAME = T2.WORKLOAD_NAME
                AND T1.METRIC_NAME = T2.METRIC_NAME
                AND T1.PARENT_METRIC_NAME = T2.PARENT_METRIC_NAME
                AND (T1.PARENT_METRIC_NAME IS NOT NULL AND T2.PARENT_METRIC_NAME IS NOT NULL)
                AND T1.RepetitionCount < T2.RepetitionCount
                AND T2.RepetitionCount - T1.RepetitionCount = 1
        WHERE 1=1
       ) -- SELECT * FROM DIFF
       ,CSUM AS (
        SELECT 
                HoraInicio
                , WORKLOAD_NAME
                , SUM(TOTAL_TIME_VALUE) AS SUM_TOTAL_TIME_VALUE
                , SUM(COUNT) AS SUM_COUNT
                , PARENT_METRIC_NAME
        FROM DIFF
        --GROUP BY Servidor, DataBase, HoraInicio, PARENT_METRIC_NAME
        GROUP BY Servidor, DataBase, HoraInicio,WORKLOAD_NAME, PARENT_METRIC_NAME
        ) --SELECT * FROM CSUM
        SELECT 
        D.Servidor, D.DataBase, D.HoraInicio, D.HoraFim
        , D.WORKLOAD_NAME, D.METRIC_NAME
        , D.TOTAL_TIME_VALUE, D.COUNT
        , CASE 
                WHEN TOTAL_TIME_VALUE > 0
                THEN CAST((D.TOTAL_TIME_VALUE / ((SELECT SUM_TOTAL_TIME_VALUE
                                        FROM CSUM
                                        WHERE D.HoraInicio = CSUM.HoraInicio
                                            AND D.WORKLOAD_NAME = CSUM.WORKLOAD_NAME 
                                            AND D.PARENT_METRIC_NAME = CSUM.PARENT_METRIC_NAME) * 1.0) * 100.0)AS DECIMAL(10,2)) 
                ELSE NULL
          END AS PCT_TIME_TOTAL
        , CASE 
                WHEN TOTAL_TIME_VALUE > 0 AND D.COUNT > 0
                THEN D.TOTAL_TIME_VALUE / D.COUNT 
                ELSE NULL
          END AS AVG_TIME
        , D.PARENT_METRIC_NAME
        ,'REP' || D.REPETITIONCOUNT AS RepetitionCount 
        FROM DIFF D
        WHERE 1=1
                AND (D.TOTAL_TIME_VALUE <> 0 OR D.COUNT <> 0)
                AND D.PARENT_METRIC_NAME IN ('TOTAL_WAIT_TIME')
        ORDER BY Servidor, DataBase, HoraInicio, WORKLOAD_NAME, PCT_TIME_TOTAL DESC, COUNT DESC -- TOTAL_TIME_VALUE DESC, COUNT DESC
        ;
    
    OPEN cReT;
    
END P2;
END P1

/*
CALL NULLID.MRT_MonGetWorkloadDetailsXMLTIMES (1, 5); 
CALL NULLID.MRT_MonGetWorkloadDetailsXMLTIMES (1, 15); 
*/
#/    


