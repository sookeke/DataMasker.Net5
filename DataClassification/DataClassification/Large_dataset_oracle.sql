SET SERVEROUTPUT ON;

DECLARE
    tbl_exist   PLS_INTEGER;
BEGIN
    SELECT
        COUNT(*)
    INTO
        tbl_exist
    FROM
        user_tables
    WHERE
        table_name = 'RESULT_CLASSIFICATION';

    IF
        tbl_exist = 1
    THEN
        EXECUTE IMMEDIATE 'drop table RESULT_CLASSIFICATION';
    END IF;
END;
/

CREATE GLOBAL TEMPORARY TABLE result_classification (
    databasename             VARCHAR(4000),
    table_name               VARCHAR(4000),
    "SCHEMA"                 VARCHAR2(2000),
    fullqualifiedtablename   VARCHAR(4000),
    column_name              VARCHAR(4000),
    data_type                VARCHAR2(2000),
    column_id                INT,
    comments                 VARCHAR(4000),
    pkconstraintname         VARCHAR(4000),
    "MIN"                    VARCHAR(4000),
    "MAX"                    VARCHAR(4000),
    minnvarchar              VARCHAR(4000),
    maxnvarchar              VARCHAR(4000)
)
/

DECLARE
    databasename             VARCHAR2(2000);
    tablename                VARCHAR2(2000);
    "SCHEMA"                 VARCHAR2(2000);
    fullqualifiedtablename   VARCHAR(4000);
    column_name              VARCHAR2(2000);
    data_type                VARCHAR2(2000);
    column_id                INT;
    comments                 VARCHAR(4000);
    pkconstraintname         VARCHAR(4000);
    sql_stmt                 VARCHAR(4000);
    sql_stmt2                VARCHAR(4000);
    sql_stmt3                VARCHAR(4000);
    sql_stmt4                VARCHAR(4000);
    sql_stmt5                VARCHAR(4000);
    sql_stmt6                VARCHAR(4000);
    "min"                    VARCHAR(4000);
    "max"                    VARCHAR(4000);
    "MinNvarChar"            VARCHAR(4000);
    "MaxNvarChar"            VARCHAR(4000);
    CURSOR cur IS
        SELECT
            (
                SELECT
                    sys_context(
                        'userenv',
                        'instance_name'
                    )
                FROM
                    dual
            ) AS databasename,
            c.table_name,
            c.owner AS "SCHEMA",
            c.owner
             || '.'
             || c.table_name AS fullqualifiedtablename,
            c.column_name,
            c.column_id,
            com.comments,
            LISTAGG(
                cols.column_name,
                ','
            ) WITHIN GROUP(ORDER BY
                c.table_name,
                c.column_name,
                cols.constraint_name
            ) pkconstraintname
  --,(select min(c.column_name) from (select cc.table_name from sys.all_tab_Columns cc
--where cc.TABLE_NAME = c.table_name)) as "min"  
        FROM
            sys.all_col_comments com,
            sys.all_tab_columns c
            LEFT JOIN sys.all_constraints cons ON (
                    c.table_name = cons.table_name
                AND
                    cons.constraint_type = 'P'
            )
            LEFT JOIN all_cons_columns cols ON (
                    cons.constraint_name = cols.constraint_name
                AND
                    cons.owner = cols.owner
            )
            INNER JOIN sys.all_tables v ON
                c.owner = v.owner
            AND
                c.table_name = v.table_name
        WHERE
                c.owner = 'APP_TAP_MASKED'--:OBJECT_OWNER  or Schema name or USER
            AND
                c.table_name = com.table_name
            AND
                c.owner = com.owner
            AND
                c.column_name = com.column_name
            AND
                c.table_name <> 'RESULT_CLASSIFICATION'
            AND
                c.data_type NOT IN (
                    'BLOB','SDO_GEOMETRY'
                )
        GROUP BY
            c.table_name,
            c.owner,
            c.owner || '.' || c.table_name,
            c.column_name,
            c.column_id,
            com.comments
        ORDER BY
            c.table_name,
            c.column_name;

BEGIN
    OPEN cur;
    FETCH cur INTO databasename,tablename,"SCHEMA",fullqualifiedtablename,column_name,column_id,comments,pkconstraintname;

    sql_stmt := tablename;
    sql_stmt2 := column_name;
    sql_stmt3 := 'Select min('
     || column_name
     || ')  from '
     || fullqualifiedtablename
     || '';
    EXECUTE IMMEDIATE sql_stmt3 INTO
        "min";
    sql_stmt4 := 'Select max('
     || column_name
     || ')  from '
     || fullqualifiedtablename
     || '';
    EXECUTE IMMEDIATE sql_stmt4 INTO
        "max";
    sql_stmt5 := 'Select min(Length('
     || column_name
     || '))  from '
     || fullqualifiedtablename
     || '';
    EXECUTE IMMEDIATE sql_stmt5 INTO
        "MinNvarChar";
    sql_stmt6 := 'Select max(Length('
     || column_name
     || '))  from '
     || fullqualifiedtablename
     || '';
    EXECUTE IMMEDIATE sql_stmt6 INTO
        "MaxNvarChar";
    --DBMS_OUTPUT.PUT_LINE ('Total employees are: '||c_name);
   --execute IMMEDIATE sql_stmt3;
   --DBMS_OUTPUT.put_line(sql_stmt3);
   --dbms_output.put_line(c_name);
    WHILE cur%found LOOP 
      --DBMS_OUTPUT.put_line(UPPER(CONCAT(CONCAT('''',table_name),'''')));
        INSERT INTO result_classification VALUES (
            databasename,
            tablename,
            "SCHEMA",
            fullqualifiedtablename,
            column_name,
            (
                SELECT
                    (
                        CASE
                            WHEN data_type = 'CHAR'      THEN data_type
                             || '('
                             || c.char_length
                             || DECODE(
                                char_used,
                                'B',
                                ' BYTE',
                                'C',
                                ' CHAR',
                                NULL
                            )
                             || ')'
                            WHEN data_type = 'VARCHAR'   THEN data_type
                             || '('
                             || c.char_length
                             || DECODE(
                                char_used,
                                'B',
                                ' BYTE',
                                'C',
                                ' CHAR',
                                NULL
                            )
                             || ')'
                            WHEN data_type = 'VARCHAR2'  THEN data_type
                             || '('
                             || c.char_length
                             || DECODE(
                                char_used,
                                'B',
                                ' BYTE',
                                'C',
                                ' CHAR',
                                NULL
                            )
                             || ')'
                            WHEN data_type = 'NCHAR'     THEN data_type
                             || '('
                             || c.char_length
                             || DECODE(
                                char_used,
                                'B',
                                ' BYTE',
                                'C',
                                ' CHAR',
                                NULL
                            )
                             || ')'
                            WHEN data_type = 'NUMBER'    THEN (
                                CASE
                                    WHEN
                                        c.data_precision IS NULL
                                    AND
                                        c.data_scale IS NULL
                                    THEN 'NUMBER'
                                    WHEN
                                        c.data_precision IS NULL
                                    AND
                                        c.data_scale IS NOT NULL
                                    THEN 'NUMBER(38,'
                                     || c.data_scale
                                     || ')'
                                    ELSE data_type
                                     || '('
                                     || c.data_precision
                                     || ','
                                     || c.data_scale
                                     || ')'
                                END
                            )
                            WHEN data_type = 'NVARCHAR'  THEN data_type
                             || '('
                             || c.char_length
                             || DECODE(
                                char_used,
                                'B',
                                ' BYTE',
                                'C',
                                ' CHAR',
                                NULL
                            )
                             || ')'
                            WHEN data_type = 'NVARCHAR2' THEN data_type
                             || '('
                             || c.char_length
                             || DECODE(
                                char_used,
                                'B',
                                ' BYTE',
                                'C',
                                ' CHAR',
                                NULL
                            )
                             || ')'
                            WHEN data_type = 'LONG'      THEN 'longish'
                            ELSE data_type
                        END
                    ) AS data_type
                FROM
                    user_tab_columns c
                WHERE
                        table_name = sql_stmt
                    AND
                        column_name = sql_stmt2
            ),
            column_id,
            comments,
            pkconstraintname,
       --(select (case when data_type in ('SDO','BLOB') then '' else (select TO_CHAR(max(CREATE_DATETIME)) from sql_stmt) end) as "MAX" from all_tab_columns 
  --where table_name = sql_stmt
  --and column_name = sql_stmt2),
            "min",
            "max",
            "MinNvarChar",
            "MaxNvarChar"
        );
     --sql_stmt := 'select * from UPPER(CONCAT(CONCAT('''''''','||tablename||'),''''''''))';
       --DBMS_OUTPUT.put_line(FullQualifiedTableName);

        EXIT WHEN cur%notfound;
        FETCH cur INTO databasename,tablename,"SCHEMA",fullqualifiedtablename,column_name,column_id,comments,pkconstraintname;

        sql_stmt := tablename;
        sql_stmt2 := column_name;
           --sql_stmt3 := FullQualifiedTableName;
        sql_stmt3 := 'Select min('
         || column_name
         || ')  from '
         || fullqualifiedtablename
         || '';
        EXECUTE IMMEDIATE sql_stmt3 INTO
            "min";
        sql_stmt4 := 'Select max('
         || column_name
         || ')  from '
         || fullqualifiedtablename
         || '';
        EXECUTE IMMEDIATE sql_stmt4 INTO
            "max";
        sql_stmt5 := 'Select min(Length('
         || column_name
         || '))  from '
         || fullqualifiedtablename
         || '';
        EXECUTE IMMEDIATE sql_stmt5 INTO
            "MinNvarChar";
        sql_stmt6 := 'Select max(Length('
         || column_name
         || '))  from '
         || fullqualifiedtablename
         || '';
        EXECUTE IMMEDIATE sql_stmt6 INTO
            "MaxNvarChar";
    --DBMS_OUTPUT.PUT_LINE ('Total employees are: '||c_name);
 
             --Select max(column_name)  into  :sql_stmt4 from FullQualifiedTableName;
       --sql_stmt4 := 'Select max(' || column_name || ') from '||FullQualifiedTableName ||'';
   --DBMS_OUTPUT.put_line(sql_stmt4);
    END LOOP;

    CLOSE cur;
END;
/

SELECT
    *
FROM
    result_classification
WHERE
    table_name <> 'RESULT_CLASSIFICATION'
UNION
SELECT
    (
        SELECT
            sys_context(
                'userenv',
                'instance_name'
            )
        FROM
            dual
    ) AS databasename,
    c.table_name,
    c.owner AS "SCHEMA",
    c.owner
     || '.'
     || c.table_name AS fullqualifiedtablename,
    c.column_name,
    (
        CASE
            WHEN data_type = 'CHAR'      THEN data_type
             || '('
             || c.char_length
             || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            )
             || ')'
            WHEN data_type = 'VARCHAR'   THEN data_type
             || '('
             || c.char_length
             || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            )
             || ')'
            WHEN data_type = 'VARCHAR2'  THEN data_type
             || '('
             || c.char_length
             || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            )
             || ')'
            WHEN data_type = 'NCHAR'     THEN data_type
             || '('
             || c.char_length
             || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            )
             || ')'
            WHEN data_type = 'NUMBER'    THEN (
                CASE
                    WHEN
                        c.data_precision IS NULL
                    AND
                        c.data_scale IS NULL
                    THEN 'NUMBER'
                    WHEN
                        c.data_precision IS NULL
                    AND
                        c.data_scale IS NOT NULL
                    THEN 'NUMBER(38,'
                     || c.data_scale
                     || ')'
                    ELSE data_type
                     || '('
                     || c.data_precision
                     || ','
                     || c.data_scale
                     || ')'
                END
            )
            WHEN data_type = 'NVARCHAR'  THEN data_type
             || '('
             || c.char_length
             || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            )
             || ')'
            WHEN data_type = 'NVARCHAR2' THEN data_type
             || '('
             || c.char_length
             || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            )
             || ')'
            WHEN data_type = 'LONG'      THEN 'longish'
            ELSE data_type
        END
    ) data_type,
    c.column_id,
    com.comments,
    LISTAGG(
        cols.column_name,
        ','
    ) WITHIN GROUP(ORDER BY
        c.table_name,
        c.column_name,
        cols.constraint_name
    ) pkconstraintname,
    '' AS "MIN",
    '' AS "MAX",
    '' AS minnvarchar,
    '' AS maxnvarchar
FROM
    sys.all_col_comments com,
    sys.all_tab_columns c
    LEFT JOIN sys.all_constraints cons ON (
            c.table_name = cons.table_name
        AND
            cons.constraint_type = 'P'
    )
    LEFT JOIN all_cons_columns cols ON (
            cons.constraint_name = cols.constraint_name
        AND
            cons.owner = cols.owner
    )
    INNER JOIN sys.all_tables v ON
        c.owner = v.owner
    AND
        c.table_name = v.table_name
WHERE
        c.owner = 'APP_TAP_MASKED'--:OBJECT_OWNER  or Schema name or USER
    AND
        c.table_name = com.table_name
    AND
        c.owner = com.owner
    AND
        c.column_name = com.column_name
    AND
        c.table_name <> 'RESULT_CLASSIFICATION'
    AND
        c.data_type IN (
            'BLOB','SDO_GEOMETRY'
        ) --add column which gives error with min max
GROUP BY
    c.table_name,
    c.owner,
    c.owner || '.' || c.table_name,
    c.column_name,
    (
        CASE
            WHEN data_type = 'CHAR'      THEN data_type || '(' || c.char_length || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            ) || ')'
            WHEN data_type = 'VARCHAR'   THEN data_type || '(' || c.char_length || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            ) || ')'
            WHEN data_type = 'VARCHAR2'  THEN data_type || '(' || c.char_length || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            ) || ')'
            WHEN data_type = 'NCHAR'     THEN data_type || '(' || c.char_length || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            ) || ')'
            WHEN data_type = 'NUMBER'    THEN (
                CASE
                    WHEN
                        c.data_precision IS NULL
                    AND
                        c.data_scale IS NULL
                    THEN 'NUMBER'
                    WHEN
                        c.data_precision IS NULL
                    AND
                        c.data_scale IS NOT NULL
                    THEN 'NUMBER(38,' || c.data_scale || ')'
                    ELSE data_type || '(' || c.data_precision || ',' || c.data_scale || ')'
                END
            )
            WHEN data_type = 'NVARCHAR'  THEN data_type || '(' || c.char_length || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            ) || ')'
            WHEN data_type = 'NVARCHAR2' THEN data_type || '(' || c.char_length || DECODE(
                char_used,
                'B',
                ' BYTE',
                'C',
                ' CHAR',
                NULL
            ) || ')'
            WHEN data_type = 'LONG'      THEN 'longish'
            ELSE data_type
        END
    ),
    c.column_id,
    com.comments;
/