/#

/*
CALL NULLID.MRT_BP(1,15);
CALL NULLID.MRT_BP(2,2);
CALL NULLID.MRT_BP(3,30);
*/
/********************************************************************************************************************************
*********************************************************************************************************************************

    Autor: Raul Diego
    E-mail: raul.oliveira@sicoob.com.br
    Data criação: 24/07/2015
    ->Descrição: MRT que faz a análise do Buffer Pool.
        Foi um dos primeiros a ser criado a faltam algumas melhorias, a mais necessária é que mostra o percentual de Page Cleaners
        por BufferPool, corrigindo as distorçoes entre os BPs.
    ->Modo de execuçao: Crie a proc e a execute, informando o tempo de espera entre as execuçoes em segundos e quantas repetiçoes.
 
    Compatibilidade: DB2 LUW 10.1

    Histórico:
        - 00/00/0000: Historico de alteracao

        
*********************************************************************************************************************************
********************************************************************************************************************************/

CREATE OR REPLACE PROCEDURE NULLID.MRT_BP
     (IN v_Wait SMALLINT DEFAULT 5
     ,IN v_Repetition SMALLINT DEFAULT 3)
     
     LANGUAGE SQL
     RESULT SET 1

P1: BEGIN
 
    DECLARE v_RepetitionCount SMALLINT DEFAULT 1;

    DECLARE GLOBAL TEMPORARY TABLE SESSION.MRT_BP AS (
        SELECT     
           CURRENT_SERVER AS BANCO
           ,MBP.BP_NAME
           ,MBP.AUTOMATIC,MBP.BP_CUR_BUFFSZ
           ,MBP.POOL_DATA_L_READS + MBP.POOL_TEMP_DATA_L_READS + MBP.POOL_INDEX_L_READS + MBP.POOL_TEMP_INDEX_L_READS 
                + MBP.POOL_XDA_L_READS + MBP.POOL_TEMP_XDA_L_READS AS LOGICAL_READS
           ,MBP.POOL_DATA_P_READS + MBP.POOL_TEMP_DATA_P_READS + MBP.POOL_INDEX_P_READS + MBP.POOL_TEMP_INDEX_P_READS 
                + MBP.POOL_XDA_P_READS + MBP.POOL_TEMP_XDA_P_READS AS PHYSICAL_READS
           ,MBP.POOL_READ_TIME
           ,MBP.POOL_DATA_WRITES + MBP.POOL_XDA_WRITES + MBP.POOL_INDEX_WRITES AS POOL_WRITES
           ,MBP.POOL_WRITE_TIME
           ,MBP.UNREAD_PREFETCH_PAGES
           ,MBP.POOL_DRTY_PG_STEAL_CLNS , MBP.POOL_DRTY_PG_THRSH_CLNS, MBP.POOL_LSN_GAP_CLNS
           ,MBP.MEMBER
           ,CURRENT_TIMESTAMP AS DataRegistro
           ,1 AS RepetitionCount
        FROM TABLE (MON_GET_BUFFERPOOL(NULL, -2)) AS MBP ) DEFINITION ONLY
           ON COMMIT DELETE ROWS
           NOT LOGGED ON ROLLBACK DELETE ROWS
           WITH REPLACE;

    WHILE v_RepetitionCount <= v_Repetition DO
        INSERT INTO SESSION.MRT_BP   
        SELECT     
           CURRENT_SERVER AS BANCO
           ,MBP.BP_NAME
           ,MBP.AUTOMATIC,MBP.BP_CUR_BUFFSZ
           ,MBP.POOL_DATA_L_READS + MBP.POOL_TEMP_DATA_L_READS + MBP.POOL_INDEX_L_READS + MBP.POOL_TEMP_INDEX_L_READS 
                + MBP.POOL_XDA_L_READS + MBP.POOL_TEMP_XDA_L_READS AS LOGICAL_READS
           ,MBP.POOL_DATA_P_READS + MBP.POOL_TEMP_DATA_P_READS + MBP.POOL_INDEX_P_READS + MBP.POOL_TEMP_INDEX_P_READS 
                + MBP.POOL_XDA_P_READS + MBP.POOL_TEMP_XDA_P_READS AS PHYSICAL_READS
           ,MBP.POOL_READ_TIME
           ,MBP.POOL_DATA_WRITES + MBP.POOL_XDA_WRITES + MBP.POOL_INDEX_WRITES AS POOL_WRITES
           ,MBP.POOL_WRITE_TIME
           ,MBP.UNREAD_PREFETCH_PAGES
           ,MBP.POOL_DRTY_PG_STEAL_CLNS , MBP.POOL_DRTY_PG_THRSH_CLNS, MBP.POOL_LSN_GAP_CLNS
           ,MBP.MEMBER
           ,CURRENT_TIMESTAMP AS DataRegistro
           ,v_RepetitionCount AS RepetitionCount
        FROM TABLE (MON_GET_BUFFERPOOL(NULL, -2)) AS MBP;
        
        SET v_RepetitionCount = v_RepetitionCount + 1;
        
        --CALL NULLID.WAITFOR(v_Wait);
        CALL NULLID.WAITFOR_MRT(v_Wait, 'MRT_BufferPools');

    END WHILE;

P2: BEGIN

    DECLARE cRet CURSOR WITH RETURN FOR
        SELECT T2.BANCO 
        ,T2.BP_NAME
        ,T2.AUTOMATIC  
        ,T2.BP_CUR_BUFFSZ  
        ,T2.LOGICAL_READS - T1.LOGICAL_READS AS LOGICAL_READS  
        ,T2.PHYSICAL_READS - T1.PHYSICAL_READS AS PHYSICAL_READS 
        ,T2.POOL_READ_TIME - T1.POOL_READ_TIME AS POOL_READ_TIME  
        ,T2.POOL_WRITES - T1.POOL_WRITES AS POOL_WRITES 
        ,T2.POOL_WRITE_TIME - T1.POOL_WRITE_TIME AS POOL_WRITE_TIME  
        ,T2.UNREAD_PREFETCH_PAGES - T1.UNREAD_PREFETCH_PAGES AS UNREAD_PREFETCH_PAGES
        ,T2.POOL_DRTY_PG_STEAL_CLNS - T1.POOL_DRTY_PG_STEAL_CLNS AS POOL_DRTY_PG_STEAL_CLNS  
        ,T2.POOL_DRTY_PG_THRSH_CLNS - T1.POOL_DRTY_PG_THRSH_CLNS AS POOL_DRTY_PG_THRSH_CLNS
        ,T2.POOL_LSN_GAP_CLNS - T1.POOL_LSN_GAP_CLNS AS POOL_LSN_GAP_CLNS 
        ,T2.DATAREGISTRO
        ,'REP' || T2.RepetitionCount AS RepetitionCount       
        FROM SESSION.MRT_BP AS T1
        INNER JOIN SESSION.MRT_BP AS T2
                ON T1.BP_NAME = T2.BP_NAME
                AND T1.DATAREGISTRO < T2.DATAREGISTRO
                AND T2.RepetitionCount - T1.RepetitionCount = 1
                --AND TIMESTAMPDIFF(4,CHAR(TIMESTAMP(T2.DATAREGISTRO) - TIMESTAMP(T1.DATAREGISTRO))) = 1 --BETWEEN 50 AND 70
        ;

    OPEN cReT;
END P2;
END P1

/*
CALL NULLID.MRT_BP(1,5);
CALL NULLID.MRT_BP(2,2);
CALL NULLID.MRT_BP(3,30);
*/
#/





