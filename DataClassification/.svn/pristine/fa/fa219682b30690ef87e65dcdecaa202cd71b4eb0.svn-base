USE PIMS_LIS_copy; --Database name here
SET ANSI_WARNINGS OFF;


DECLARE @DatabaseName VARCHAR(MAX);
DECLARE @TABLE_NAME VARCHAR(MAX);
DECLARE @SCHEMA VARCHAR(MAX);
DECLARE @COLUMN_NAME VARCHAR(MAX);
DECLARE @FullyQualifiedTableName VARCHAR(500);
DECLARE @DATA_TYPE VARCHAR(MAX);
DECLARE @Nullable VARCHAR(MAX);
DECLARE @COLUMN_ID VARCHAR(MAX);
DECLARE @COMMENTS VARCHAR(MAX);
DECLARE @PKconstraintName VARCHAR(MAX);
DECLARE @PK_ID VARCHAR(MAX);


DECLARE @Results TABLE
(
    DatabaseName VARCHAR(MAX),
    TABLE_NAME VARCHAR(MAX),
    [SCHEMA] VARCHAR(MAX),
    COLUMN_NAME VARCHAR(MAX),
    DATA_TYPE VARCHAR(MAX),
    Nullable VARCHAR(MAX),
    COLUMN_ID VARCHAR(MAX),
    COMMENTS VARCHAR(MAX),
    PKconstraintName VARCHAR(MAX),
    PK_ID VARCHAR(MAX),
    [Min] VARCHAR(MAX),
    [Max] VARCHAR(MAX)
);

DECLARE Cur CURSOR FOR
SELECT DB_NAME() AS DatabaseName,
       UPPER(SC.TABLE_NAME) AS TABLE_NAME,
       UPPER(SC.TABLE_SCHEMA) AS [SCHEMA],
       UPPER(SC.COLUMN_NAME) AS COLUMN_NAME,
       '[' + DB_NAME() + ']' + '.[' + SC.TABLE_SCHEMA + '].' + '[' + SC.TABLE_NAME + ']' AS FullQualifiedTableName,
       UPPER(SC.DATA_TYPE) + CASE
                                 WHEN SC.DATA_TYPE IN ( 'char', 'nchar', 'varchar', 'nvarchar', 'binary', 'varbinary' )
                                      AND SC.CHARACTER_MAXIMUM_LENGTH > 0 THEN
                                     COALESCE('(' + CONVERT(VARCHAR, SC.CHARACTER_MAXIMUM_LENGTH) + ')', '')
                                 ELSE
                                     ''
                             END
       + CASE
             WHEN DATA_TYPE IN ( 'decimal', 'numeric' ) THEN
                 COALESCE(
                             '(' + CONVERT(VARCHAR, SC.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR, SC.NUMERIC_SCALE)
                             + ')',
                             ''
                         )
             ELSE
                 ''
         END AS DATA_TYPE,
       CASE
           WHEN SC.IS_NULLABLE = 'NO' THEN
               'NOT '
           ELSE
               ''
       END + 'NULL' AS Nullable,
       UPPER(SC.ORDINAL_POSITION) AS COLUMN_ID,
       UPPER(SC.DOMAIN_NAME) AS COMMENTS,
       PKconstraintName = UPPER(
                          (
                              SELECT +Col.name + ',' + ' '
                              FROM sys.identity_columns Col
                              WHERE object_id = OBJECT_ID(SC.TABLE_NAME)
                              ORDER BY Col.name DESC
                              FOR XML PATH('')
                          )
                               ),
       PK_ID = UPPER(
               (
                   SELECT +c.name + ',' + ' '
                   FROM sys.tables t
                       LEFT JOIN sys.columns c
                           ON c.object_id = t.object_id
                       LEFT JOIN sys.identity_columns idc
                           ON idc.object_id = t.object_id
                              AND idc.column_id = c.column_id
                              AND idc.is_identity = 1
                       LEFT JOIN sys.index_columns ic
                           ON ic.object_id = t.object_id
                              AND ic.column_id = c.column_id
                       LEFT JOIN sys.indexes i
                           ON i.object_id = t.object_id
                              AND i.index_id = ic.index_id
                              AND i.is_primary_key = 1
                   WHERE t.type = 'U'
                         AND
                         (
                             idc.is_identity = 1
                             OR i.is_primary_key = 1
                             OR c.name = 'ID'
                         )
                         AND t.name = SC.TABLE_NAME
                   ORDER BY c.name
                   FOR XML PATH('')
               )
                    )
FROM INFORMATION_SCHEMA.COLUMNS SC
    INNER JOIN INFORMATION_SCHEMA.TABLES AS SO
        ON (
               SO.TABLE_NAME = SC.TABLE_NAME
               AND SO.TABLE_TYPE IN ( 'BASE TABLE' )
           )
    LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        ON (
               SC.TABLE_NAME = TC.TABLE_NAME
               AND TC.CONSTRAINT_TYPE = 'Primary Key'
           )
WHERE SC.TABLE_CATALOG = 'PIMS_LIS_copy' --DataBase Name
      AND SC.DATA_TYPE NOT IN ('bit','uniqueidentifier')
ORDER BY 1,
         2,
         PKconstraintName;

OPEN Cur;

FETCH NEXT FROM Cur
INTO @DatabaseName,
     @TABLE_NAME,
     @SCHEMA,
     @COLUMN_NAME,
     @FullyQualifiedTableName,
     @DATA_TYPE,
     @Nullable,
     @COLUMN_ID,
     @COMMENTS,
     @PKconstraintName,
     @PK_ID;

WHILE @@FETCH_STATUS = 0
BEGIN
    DECLARE @SQL VARCHAR(MAX) = NULL;

    SET @SQL
        = ' Select ''' + @DatabaseName + ''' AS DatabaseName, ''' + @TABLE_NAME + ''' AS TABLE_NAME,
      '''      + @SCHEMA + ''' AS [SCHEMA],
      '''      + @COLUMN_NAME + ''' AS COLUMN_NAME,
      '''      + @DATA_TYPE + ''' AS DATA_TYPE,
	  '''      + @Nullable + ''' AS Nullable,
      '''      + @COLUMN_ID + ''' AS COLUMN_ID,
	  '' NULL '' AS COMMENTS, ' + CASE
                                      WHEN @PKconstraintName IS NULL THEN
                                          ''''''
                                      ELSE
                                          +'''' + @PKconstraintName + ''''
                                  END + ' as PKconstraintName,' + CASE
                                                                      WHEN @PK_ID IS NULL THEN
                                                                          ''''''
                                                                      ELSE
                                                                          +'''' + @PK_ID + ''''
                                                                  END + ' as PK_ID,' + 'Case when  ''' + @DATA_TYPE
          + ''' = ''BIT'' then ''NULL''
	     When  ''' + @DATA_TYPE + ''' Like ''%CHAR%'' or ''' + @DATA_TYPE
          + ''' Like ''%BINARY%'' then CAST((Select MIN(Len([' + @COLUMN_NAME + '])) from ' + @FullyQualifiedTableName
          + ' with (nolock)) AS NVARCHAR(max))
		    When  ''' + @DATA_TYPE + ''' Like ''%DATE%''  then 
			CAST((Select MIN([' + @COLUMN_NAME + ']) from ' + @FullyQualifiedTableName
          + ' with (nolock)) AS NVARCHAR(max))
	  else 
      CAST((Select MIN([' + @COLUMN_NAME + ']) from ' + @FullyQualifiedTableName
          + ' with (nolock)) AS NVARCHAR(max)) end
      AS MinValue,
	  Case when  ''' + @DATA_TYPE + ''' = ''BIT'' then ''NULL''
		  When  ''' + @DATA_TYPE + ''' Like ''%CHAR%'' or ''' + @DATA_TYPE
          + ''' Like ''%BINARY%'' then CAST((Select MAX(Len([' + @COLUMN_NAME + '])) from ' + @FullyQualifiedTableName
          + ' with (nolock)) AS NVARCHAR(max))
		    When  ''' + @DATA_TYPE + ''' Like ''%DATE%''  then 
			CAST((Select MAX([' + @COLUMN_NAME + ']) from ' + @FullyQualifiedTableName
          + ' with (nolock)) AS NVARCHAR(max))
	  else 
      CAST((Select MAX([' + @COLUMN_NAME + ']) from ' + @FullyQualifiedTableName
          + ' with (nolock)) AS NVARCHAR(max)) end
      AS MaxValue  
	  ';

    PRINT @SQL;

    INSERT INTO @Results
    EXEC (@SQL);

    FETCH NEXT FROM Cur
    INTO @DatabaseName,
         @TABLE_NAME,
         @SCHEMA,
         @COLUMN_NAME,
         @FullyQualifiedTableName,
         @DATA_TYPE,
         @Nullable,
         @COLUMN_ID,
         @COMMENTS,
         @PKconstraintName,
         @PK_ID;
END;

CLOSE Cur;

DEALLOCATE Cur;

SELECT * 
FROM @Results
UNION --add the bit data type back to result
SELECT DB_NAME() COLLATE  SQL_Latin1_General_CP1_CI_AS AS DatabaseName,
       UPPER(SC.TABLE_NAME) COLLATE  SQL_Latin1_General_CP1_CI_AS AS TABLE_NAME,
       UPPER(SC.TABLE_SCHEMA) COLLATE  SQL_Latin1_General_CP1_CI_AS AS [SCHEMA],
       UPPER(SC.COLUMN_NAME) COLLATE  SQL_Latin1_General_CP1_CI_AS AS COLUMN_NAME,
       UPPER(SC.DATA_TYPE) COLLATE  SQL_Latin1_General_CP1_CI_AS + CASE
                                 WHEN SC.DATA_TYPE IN ( 'char', 'nchar', 'varchar', 'nvarchar', 'binary', 'varbinary' )
                                      AND SC.CHARACTER_MAXIMUM_LENGTH > 0 THEN
                                     COALESCE('(' + CONVERT(VARCHAR, SC.CHARACTER_MAXIMUM_LENGTH) + ')', '')
                                 ELSE
                                     ''
                             END
       + CASE
             WHEN DATA_TYPE IN ( 'decimal', 'numeric' ) THEN
                 COALESCE(
                             '(' + CONVERT(VARCHAR, SC.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR, SC.NUMERIC_SCALE)
                             + ')',
                             ''
                         )
             ELSE
                 ''
         END AS  DATA_TYPE,
       CASE
           WHEN SC.IS_NULLABLE COLLATE  SQL_Latin1_General_CP1_CI_AS = 'NO' THEN
               'NOT '
           ELSE
               ''
       END + 'NULL' AS Nullable,
       UPPER(SC.ORDINAL_POSITION) COLLATE  SQL_Latin1_General_CP1_CI_AS AS COLUMN_ID,
       UPPER(SC.DOMAIN_NAME) COLLATE  SQL_Latin1_General_CP1_CI_AS AS COMMENTS,
       PKconstraintName = UPPER(
                          (
                              SELECT +Col.name + ',' + ' '
                              FROM sys.identity_columns Col
                              WHERE object_id = OBJECT_ID(SC.TABLE_NAME)
                              ORDER BY Col.name DESC
                              FOR XML PATH('')
                          )
                               ),
       PK_ID = UPPER(
               (
                   SELECT +c.name + ',' + ' '
                   FROM sys.tables t
                       LEFT JOIN sys.columns c
                           ON c.object_id = t.object_id
                       LEFT JOIN sys.identity_columns idc
                           ON idc.object_id = t.object_id
                              AND idc.column_id = c.column_id
                              AND idc.is_identity = 1
                       LEFT JOIN sys.index_columns ic
                           ON ic.object_id = t.object_id
                              AND ic.column_id = c.column_id
                       LEFT JOIN sys.indexes i
                           ON i.object_id = t.object_id
                              AND i.index_id = ic.index_id
                              AND i.is_primary_key = 1
                   WHERE t.type = 'U'
                         AND
                         (
                             idc.is_identity = 1
                             OR i.is_primary_key = 1
                             OR c.name = 'ID'
                         )
                         AND t.name = SC.TABLE_NAME
                   ORDER BY c.name
                   FOR XML PATH('')
               )
                    ),
       'NULL' AS [MAX],
       'NULL' AS [MIN] 
FROM INFORMATION_SCHEMA.COLUMNS SC
    INNER JOIN INFORMATION_SCHEMA.TABLES AS SO
        ON (
               SO.TABLE_NAME = SC.TABLE_NAME
               AND SO.TABLE_TYPE IN ( 'BASE TABLE' )
           )
    LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        ON (
               SC.TABLE_NAME = TC.TABLE_NAME
               AND TC.CONSTRAINT_TYPE = 'Primary Key'
           )
WHERE SC.TABLE_CATALOG = 'PIMS_LIS_copy' --DataBase Name
     AND SC.DATA_TYPE in ('bit','uniqueidentifier')
ORDER BY 1,
         2,
         PKconstraintName;
