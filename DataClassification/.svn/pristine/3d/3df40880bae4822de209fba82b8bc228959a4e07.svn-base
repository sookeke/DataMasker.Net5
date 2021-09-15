SELECT
   UPPER(SC.TABLE_NAME) AS TABLE_NAME, 
	UPPER(SC.TABLE_SCHEMA) AS `SCHEMA`,
    UPPER(SC.COLUMN_NAME) AS COLUMN_NAME,    
	UPPER(SC.ORDINAL_POSITION) AS COLUMN_ID,
    (CASE WHEN SC.DATA_TYPE IN ('char','nchar','character varying','varchar','nvarchar','binary','varbinary')
                    AND SC.CHARACTER_MAXIMUM_LENGTH > 0 THEN
                   upper(concat("varchar(", character_maximum_length, ")"))
         WHEN SC.DATA_TYPE IN ('decimal','numeric') THEN
                UPPER(CONCAT(data_type,"(",numeric_precision,",",numeric_scale,")"))
            ELSE upper(SC.DATA_TYPE)
            END)  AS DATA_TYPE,
UPPER(SC.column_comment) as "COMMENTS",
CASE WHEN SC.IS_NULLABLE='NO' THEN 'NOT NULL' ELSE 'NULL' END  AS "Nullable",
UPPER(kcu.column_name) as PKconstraintName
from information_schema.columns as SC
LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS as tc
                 ON tc.constraint_catalog = SC.table_catalog
                 AND tc.table_schema = SC.table_schema
                 AND tc.table_name = SC.table_name
                 AND tc.constraint_type = 'PRIMARY KEY'
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu
                 ON kcu.constraint_catalog = tc.constraint_catalog
                 AND kcu.table_schema = tc.table_schema
                 AND kcu.table_name = tc.table_name
                 AND kcu.constraint_name = tc.constraint_name	
INNER JOIN INFORMATION_SCHEMA.TABLES tv ON tv.table_schema = SC.table_schema
				 AND tv.table_name = SC.table_name
				 AND tv.table_type NOT IN ('VIEW')
where SC.table_schema = 'public' #schema or database name
ORDER BY SC.table_catalog,
         SC.table_schema,
         SC.table_name,
         kcu.constraint_name,
         kcu.ordinal_position;