SELECT
   SC.TABLE_NAME AS TABLE_NAME, 
	SC.TABLE_SCHEMA AS "SCHEMA",
    SC.COLUMN_NAME AS COLUMN_NAME,
    
        CASE WHEN SC.DATA_TYPE IN ('char','nchar','character varying','varchar','nvarchar','binary','varbinary')
                    AND SC.CHARACTER_MAXIMUM_LENGTH > 0 THEN
                'varchar('||character_maximum_length||')'
          -- ELSE UPPER(SC.DATA_TYPE) END
         WHEN DATA_TYPE IN ('decimal','numeric') THEN
                'numeric('||numeric_precision||','||numeric_scale||')'
            ELSE SC.DATA_TYPE END
        AS DATA_TYPE,
		SC.ORDINAL_POSITION AS COLUMN_ID,
		(select pgd.description as COMMENT FROM pg_catalog.pg_statio_all_tables as st
  inner join pg_catalog.pg_description pgd on (pgd.objoid=st.relid)
		where pgd.objsubid= SC.ordinal_position
		 and  SC.table_schema=st.schemaname and SC.table_name=st.relname),
    CASE WHEN SC.IS_NULLABLE='NO' THEN 'NOT NULL' ELSE 'NULL' END  AS Nullable,
	kcu.column_name as PKconstraint
	from information_schema.columns as SC
	 LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                 ON tc.table_catalog = SC.table_catalog
                 AND tc.table_schema = SC.table_schema
                 AND tc.table_name = SC.table_name
                 AND tc.constraint_type = 'PRIMARY KEY'
	LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                 ON kcu.table_catalog = tc.table_catalog
                 AND kcu.table_schema = tc.table_schema
                 AND kcu.table_name = tc.table_name
                 AND kcu.constraint_name = tc.constraint_name			 
where SC.table_schema = 'public'
and SC.table_schema not in ('information_schema', 'pg_catalog')
ORDER BY SC.table_catalog,
         SC.table_schema,
         SC.table_name,
         kcu.constraint_name,
         kcu.ordinal_position;