/#
CREATE OR REPLACE PROCEDURE NULLID.WAITFOR_MRT
     (IN v_timeout SMALLINT DEFAULT 10
     ,IN v_name VARCHAR(30) DEFAULT 'waitfor_MRT')
     LANGUAGE SQL    
BEGIN
    --DECLARE v_name    VARCHAR(30) DEFAULT 'waitfor_dummy';
    DECLARE v_msg     VARCHAR(80);
    DECLARE v_status  INTEGER;
    DECLARE v_valuerandom VARCHAR(5);
    
    SET v_valuerandom = (SELECT CAST ( SMALLINT ( RAND () *10000 + 1 ) AS CHAR(5)) FROM SYSIBM.DUAL);

    SET v_name = (SELECT v_name || '_' || v_valuerandom FROM SYSIBM.DUAL);
    -- Garante que não existe o alerta para não haver conflito de chave duplicada.
    -- Poderia acontecer se o procedimento for cancelado no meio da execução.
    
    --CALL DBMS_ALERT.REMOVE(v_name);
    --CALL DBMS_ALERT.REGISTER(v_name);    
    
    ----Essa linha abaixo � a correta:
    CALL DBMS_ALERT.WAITONE(v_name , v_msg , v_status , v_timeout);    
    
    --VALUES v_name
    --DBMS_OUTPUT.PUT_LINE and DBMS_OUTPUT.GET_LINES
    --CALL DBMS_OUTPUT.PUT_LINE( v_name );
    
    
    --Esse aqui remove o alerta
    --CALL DBMS_ALERT.REMOVE(v_name);
    
    --DROP PROCEDURE NULLID.WAITFOR(SMALLINT);
    --DROP PROCEDURE NULLID.WAITFOR(SMALLINT,VARCHAR(30));
    
END
#/

/*
/#
SET SERVEROUTPUT ON;
  CALL NULLID.WAITFOR_MRT(5, 'MRT_Index');
SET SERVEROUTPUT ON;
#/
*/

/*
SELECT *
FROM SYSCAT.PROCEDURES P
WHERE P.PROCSCHEMA = 'NULLID'
    AND P.PROCNAME = 'WAITFOR';

*/

--DROP PROCEDURE NULLID.WAITFOR(SMALLINT);
--DROP PROCEDURE NULLID.WAITFOR(SMALLINT,VARCHAR(30));

--
--/#
--BEGIN
--        --CALL DBMS_ALERT.WAITONE('trig1', ?, ?, 5)
--          DECLARE v_name    VARCHAR(30) DEFAULT 'alert_test';
--          DECLARE v_msg     VARCHAR(80);
--          DECLARE v_status  INTEGER;
--          DECLARE v_timeout INTEGER DEFAULT 20;
--        
--             
--          CALL DBMS_ALERT.WAITONE(v_name , v_msg , v_status , v_timeout);
--          
--          
--          CALL DBMS_OUTPUT.PUT_LINE('Alert name : ' || v_name);
--          CALL DBMS_OUTPUT.PUT_LINE('Alert msg : ' || v_msg);
--          CALL DBMS_OUTPUT.PUT_LINE('Alert status : ' || v_status);
--          CALL DBMS_OUTPUT.PUT_LINE('Alert timeout: ' || v_timeout || ' seconds');
--END
--#/




/*
--A linha abaixo serve para ser executada direto na linha de comando


CREATE OR REPLACE PROCEDURE NULLID.WAITFOR
     (IN v_timeout SMALLINT DEFAULT 10)
     LANGUAGE SQL    
BEGIN
    DECLARE v_name    VARCHAR(30) DEFAULT 'waitfor_dummy';
    DECLARE v_msg     VARCHAR(80);
    DECLARE v_status  INTEGER;

    -- Garante que não existe o alerta para não haver conflito de chave duplicada.
    -- Poderia acontecer se o procedimento for cancelado no meio da execução.
    CALL DBMS_ALERT.REMOVE(v_name);
    CALL DBMS_ALERT.REGISTER(v_name);    
    CALL DBMS_ALERT.WAITONE(v_name , v_msg , v_status , v_timeout);    
    CALL DBMS_ALERT.REMOVE(v_name);
END@

*/




--SELECT DAYNAME(CURRENT_TIMESTAMP) FROM SYSIBM.SYSDUMMY1;