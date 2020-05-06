/#
CREATE OR REPLACE FUNCTION SQLSigDB2(p1 NCLOB, parselength INT DEFAULT 4000)
     RETURNS NVARCHAR(4000)
     DETERMINISTIC NO EXTERNAL ACTION CONTAINS SQL
BEGIN ATOMIC
  DECLARE pos INT;
  DECLARE mode CHAR(10);
  DECLARE maxlength INT;
  --DECLARE p2 NVARCHAR(4000);
  DECLARE p2 VARCHAR(4000);
  DECLARE currchar, nextchar CHAR(1);
  DECLARE p2len INT;

  --SET p1 = CASE WHEN 'C' THEN D END;

  SET maxlength = LENGTH(RTRIM(SUBSTR(p1,1,4000)));
  SET maxlength = CASE WHEN maxlength > parselength 
                     THEN parselength ELSE maxlength END;
  SET pos = 1;
  SET p2 = '';
  SET p2len = 0;
  SET currchar = '';
  set nextchar = '';
  SET mode = 'command';

  WHILE (pos <= maxlength) DO
    SET currchar = SUBSTR(p1,pos,1);
    --SET currchar = CAST(currchar AS VARCHAR(4000) FOR MIXED DATA);
--    SET currchar = CASE
--                        WHEN currchar = CAST('Á' AS VARCHAR(10) FOR MIXED DATA) = 'Á' THEN 'A'
--                        WHEN CURRCHAR = 'É' THEN 'E'
--                        WHEN CURRCHAR = 'Í' THEN 'I'
--                        WHEN CURRCHAR = 'Ó' THEN 'O'
--                        WHEN CURRCHAR = 'Ú' THEN 'U'
--                        WHEN CURRCHAR = 'Á' THEN 'A'
--                        WHEN CURRCHAR = 'É' THEN 'E'
--                        WHEN CURRCHAR = 'Í' THEN 'I'
--                        WHEN CURRCHAR = 'Ó' THEN 'O'
--                        WHEN CURRCHAR = 'Ú' THEN 'U'
--                        WHEN CURRCHAR = 'Ç' THEN 'C'
--                        WHEN CURRCHAR = 'Ç' THEN 'C'
--                        ELSE currchar      
--                     END;
--    SET currchar = translate(currchar, 'Á', 'A');      
--    SET currchar = UCASE(currchar);
--    SET currchar = CASE
--                        WHEN currchar = 'á' THEN 'a'
--                        WHEN currchar = 'é' THEN 'e'
--                        WHEN currchar = 'í' THEN 'i'
--                        WHEN currchar = 'ó' THEN 'o'
--                        WHEN currchar = 'ú' THEN 'u'
--                        WHEN currchar = 'Á' THEN 'A'
--                        WHEN currchar = 'É' THEN 'E'
--                        WHEN currchar = 'Í' THEN 'I'
--                        WHEN currchar = 'Ó' THEN 'O'
--                        WHEN currchar = 'Ú' THEN 'U'
--                        WHEN currchar = 'ç' THEN 'c'
--                        WHEN currchar = 'Ç' THEN 'C'
--                        ELSE currchar      
--                     END;
    SET nextchar = SUBSTR(p1,pos+1,1);
    IF mode = 'command' THEN
      SET p2 = LEFT(p2,p2len) || currchar;
      SET p2len = p2len + 1 ;
      
              IF currchar IN (',','(',' ','=','<','>','!')
                AND nextchar BETWEEN '0' AND '9' THEN
                 SET mode = 'number';
                 SET p2 = LEFT(p2,p2len) || '?';
                 SET p2len = p2len + 1;
              END IF;
              IF currchar = '''' THEN
                SET mode = 'literal';
                SET p2 = LEFT(p2,p2len) || '?''';
                SET p2len = p2len + 2;
              END IF;
    --ELSEIF (mode = 'number') AND (nextchar IN (',',')',' ','=','<','>','!')) THEN
    ELSEIF (mode = 'number') AND nextchar IN (',', ')', ' ', '=', '<', '>', '!') THEN
        SET mode= 'command';
    --ELSEIF ((mode = 'literal') and (currchar = '''')) THEN
    ELSEIF mode = 'literal' AND currchar = '''' THEN
        SET mode= 'command';
    END IF;

    SET pos = pos + 1;
  END WHILE;
  RETURN p2;
END
#/