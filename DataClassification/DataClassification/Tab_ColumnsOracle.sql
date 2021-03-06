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
    DECODE(
        c.nullable,
        'N',
        'Y',
        'N'
    ) nullable,
    column_id,
    com.comments,
    LISTAGG(
        cols.column_name,
        ','
    ) WITHIN GROUP(ORDER BY
        c.table_name,
        c.column_name,
        cols.constraint_name,
        c.low_value,
        c.high_value
    ) pkconstraintname,
    DECODE(
        substr(data_type,1,9),
        'NUMBER',
        TO_CHAR(utl_raw.cast_to_number(c.low_value) ),
        'VARCHAR2',
        length(TO_CHAR(utl_raw.cast_to_varchar2(c.low_value) ) ),
        'NVARCHAR2',
        length(TO_CHAR(utl_raw.cast_to_nvarchar2(c.low_value) ) ),
        'BINARY_DOUBLE',
        TO_CHAR(utl_raw.cast_to_binary_double(c.low_value) ),
        'BINARY_FLOAT',
        TO_CHAR(utl_raw.cast_to_binary_float(c.low_value) ),
        'DATE',
        lpad(
            TO_CHAR(100 * to_number(
                substr(
                    low_value,
                    1,
                    2
                ),
                'XX'
            ) + to_number(
                substr(
                    low_value,
                    3,
                    2
                ),
                'XX'
            ) ) - 10100,
            4,
            '0'
        )
         || '-'
         || lpad(
            to_number(
                substr(
                    low_value,
                    5,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        )
         || '-'
         || lpad(
            to_number(
                substr(
                    low_value,
                    7,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        )
         || ' '
         || lpad(
            (to_number(
                substr(
                    low_value,
                    9,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ':'
         || lpad(
            (to_number(
                substr(
                    low_value,
                    11,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ':'
         || lpad(
            (to_number(
                substr(
                    low_value,
                    13,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ''
         || lpad(
            (to_number(
                substr(
                    low_value,
                    15,
                    8
                ),
                'XXXXXXXX'
            ) - 1),
            8,
            '0'
        ),
        'TIMESTAMP',
        lpad(
            TO_CHAR(100 * to_number(
                substr(
                    low_value,
                    1,
                    2
                ),
                'XX'
            ) + to_number(
                substr(
                    low_value,
                    3,
                    2
                ),
                'XX'
            ) ) - 10100,
            4,
            '0'
        )
         || '-'
         || lpad(
            to_number(
                substr(
                    low_value,
                    5,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        )
         || '-'
         || lpad(
            to_number(
                substr(
                    low_value,
                    7,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        )
         || ' '
         || lpad(
            (to_number(
                substr(
                    low_value,
                    9,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ':'
         || lpad(
            (to_number(
                substr(
                    low_value,
                    11,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ':'
         || lpad(
            (to_number(
                substr(
                    low_value,
                    13,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ''
         || lpad(
            (to_number(
                substr(
                    low_value,
                    15,
                    8
                ),
                'XXXXXXXX'
            ) - 1),
            8,
            '0'
        )
    ) min,
    DECODE(
        substr(data_type,1,9),
        'NUMBER',
        TO_CHAR(utl_raw.cast_to_number(high_value) ),
        'VARCHAR2',
        length(TO_CHAR(utl_raw.cast_to_varchar2(high_value) ) ),
        'NVARCHAR2',
        length(TO_CHAR(utl_raw.cast_to_nvarchar2(high_value) ) ),
        'BINARY_DOUBLE',
        TO_CHAR(utl_raw.cast_to_binary_double(high_value) ),
        'BINARY_FLOAT',
        TO_CHAR(utl_raw.cast_to_binary_float(high_value) ),
        'DATE',
        lpad(
            TO_CHAR(100 * to_number(
                substr(
                    high_value,
                    1,
                    2
                ),
                'XX'
            ) + to_number(
                substr(
                    high_value,
                    3,
                    2
                ),
                'XX'
            ) ) - 10100,
            4,
            '0'
        )
         || '-'
         || lpad(
            to_number(
                substr(
                    high_value,
                    5,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        )
         || '-'
         || lpad(
            to_number(
                substr(
                    high_value,
                    7,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        )
         || ' '
         || lpad(
            (to_number(
                substr(
                    high_value,
                    9,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ':'
         || lpad(
            (to_number(
                substr(
                    high_value,
                    11,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ':'
         || lpad(
            (to_number(
                substr(
                    high_value,
                    13,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ''
         || lpad(
            (to_number(
                substr(
                    high_value,
                    15,
                    8
                ),
                'XXXXXXXX'
            ) - 1),
            8,
            '0'
        ),
        'TIMESTAMP',
        lpad(
            TO_CHAR(100 * to_number(
                substr(
                    high_value,
                    1,
                    2
                ),
                'XX'
            ) + to_number(
                substr(
                    high_value,
                    3,
                    2
                ),
                'XX'
            ) ) - 10100,
            4,
            '0'
        )
         || '-'
         || lpad(
            to_number(
                substr(
                    high_value,
                    5,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        )
         || '-'
         || lpad(
            to_number(
                substr(
                    high_value,
                    7,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        )
         || ' '
         || lpad(
            (to_number(
                substr(
                    high_value,
                    9,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ':'
         || lpad(
            (to_number(
                substr(
                    high_value,
                    11,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ':'
         || lpad(
            (to_number(
                substr(
                    high_value,
                    13,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        )
         || ''
         || lpad(
            (to_number(
                substr(
                    high_value,
                    15,
                    8
                ),
                'XXXXXXXX'
            ) - 1),
            8,
            '0'
        )
    ) max
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
        c.owner = upper('APP_PAM_MASKED')--:OBJECT_OWNER  or Schema name or USER
    AND
        c.table_name = com.table_name
    AND
        c.owner = com.owner
    AND
        c.column_name = com.column_name
GROUP BY
    c.table_name,
    c.column_name,
    c.owner,
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
    c.char_length,
    c.data_precision,
    c.data_scale,
    DECODE(
        nullable,
        'Y',
        'Yes',
        'No'
    ),
    column_id,
    com.comments,
    DECODE(
        c.nullable,
        'N',
        'Y',
        'N'
    ),
    DECODE(
        substr(c.data_type,1,9),
        'NUMBER',
        TO_CHAR(utl_raw.cast_to_number(c.low_value) ),
        'VARCHAR2',
        length(TO_CHAR(utl_raw.cast_to_varchar2(c.low_value) ) ),
        'NVARCHAR2',
        length(TO_CHAR(utl_raw.cast_to_nvarchar2(c.low_value) ) ),
        'BINARY_DOUBLE',
        TO_CHAR(utl_raw.cast_to_binary_double(c.low_value) ),
        'BINARY_FLOAT',
        TO_CHAR(utl_raw.cast_to_binary_float(c.low_value) ),
        'DATE',
        lpad(
            TO_CHAR(100 * to_number(
                substr(
                    low_value,
                    1,
                    2
                ),
                'XX'
            ) + to_number(
                substr(
                    low_value,
                    3,
                    2
                ),
                'XX'
            ) ) - 10100,
            4,
            '0'
        ) || '-' || lpad(
            to_number(
                substr(
                    low_value,
                    5,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        ) || '-' || lpad(
            to_number(
                substr(
                    low_value,
                    7,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        ) || ' ' || lpad(
            (to_number(
                substr(
                    low_value,
                    9,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || ':' || lpad(
            (to_number(
                substr(
                    low_value,
                    11,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || ':' || lpad(
            (to_number(
                substr(
                    low_value,
                    13,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || '' || lpad(
            (to_number(
                substr(
                    low_value,
                    15,
                    8
                ),
                'XXXXXXXX'
            ) - 1),
            8,
            '0'
        ),
        'TIMESTAMP',
        lpad(
            TO_CHAR(100 * to_number(
                substr(
                    low_value,
                    1,
                    2
                ),
                'XX'
            ) + to_number(
                substr(
                    low_value,
                    3,
                    2
                ),
                'XX'
            ) ) - 10100,
            4,
            '0'
        ) || '-' || lpad(
            to_number(
                substr(
                    low_value,
                    5,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        ) || '-' || lpad(
            to_number(
                substr(
                    low_value,
                    7,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        ) || ' ' || lpad(
            (to_number(
                substr(
                    low_value,
                    9,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || ':' || lpad(
            (to_number(
                substr(
                    low_value,
                    11,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || ':' || lpad(
            (to_number(
                substr(
                    low_value,
                    13,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || '' || lpad(
            (to_number(
                substr(
                    low_value,
                    15,
                    8
                ),
                'XXXXXXXX'
            ) - 1),
            8,
            '0'
        )
    ),
    DECODE(
        substr(c.data_type,1,9),
        'NUMBER',
        TO_CHAR(utl_raw.cast_to_number(high_value) ),
        'VARCHAR2',
        length(TO_CHAR(utl_raw.cast_to_varchar2(high_value) ) ),
        'NVARCHAR2',
        length(TO_CHAR(utl_raw.cast_to_nvarchar2(high_value) ) ),
        'BINARY_DOUBLE',
        TO_CHAR(utl_raw.cast_to_binary_double(high_value) ),
        'BINARY_FLOAT',
        TO_CHAR(utl_raw.cast_to_binary_float(high_value) ),
        'DATE',
        lpad(
            TO_CHAR(100 * to_number(
                substr(
                    high_value,
                    1,
                    2
                ),
                'XX'
            ) + to_number(
                substr(
                    high_value,
                    3,
                    2
                ),
                'XX'
            ) ) - 10100,
            4,
            '0'
        ) || '-' || lpad(
            to_number(
                substr(
                    high_value,
                    5,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        ) || '-' || lpad(
            to_number(
                substr(
                    high_value,
                    7,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        ) || ' ' || lpad(
            (to_number(
                substr(
                    high_value,
                    9,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || ':' || lpad(
            (to_number(
                substr(
                    high_value,
                    11,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || ':' || lpad(
            (to_number(
                substr(
                    high_value,
                    13,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || '' || lpad(
            (to_number(
                substr(
                    high_value,
                    15,
                    8
                ),
                'XXXXXXXX'
            ) - 1),
            8,
            '0'
        ),
        'TIMESTAMP',
        lpad(
            TO_CHAR(100 * to_number(
                substr(
                    high_value,
                    1,
                    2
                ),
                'XX'
            ) + to_number(
                substr(
                    high_value,
                    3,
                    2
                ),
                'XX'
            ) ) - 10100,
            4,
            '0'
        ) || '-' || lpad(
            to_number(
                substr(
                    high_value,
                    5,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        ) || '-' || lpad(
            to_number(
                substr(
                    high_value,
                    7,
                    2
                ),
                'XX'
            ),
            2,
            '0'
        ) || ' ' || lpad(
            (to_number(
                substr(
                    high_value,
                    9,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || ':' || lpad(
            (to_number(
                substr(
                    high_value,
                    11,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || ':' || lpad(
            (to_number(
                substr(
                    high_value,
                    13,
                    2
                ),
                'XX'
            ) - 1),
            2,
            '0'
        ) || '' || lpad(
            (to_number(
                substr(
                    high_value,
                    15,
                    8
                ),
                'XXXXXXXX'
            ) - 1),
            8,
            '0'
        )
    )
ORDER BY
    c.owner,
    c.table_name,
    column_id