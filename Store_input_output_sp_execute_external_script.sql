/*

** Author: Tomaz Kastrun
** Web: http://tomaztsql.wordpress.com
** Twitter: @tomaz_tsql
** Created: 17.04.2017; Ljubljana
** Saving output and input with sp_execute_external_script using Temporal Table and File Table (part #2)
** R and T-SQL

*/


USE [FileTableRChart];
GO

EXEC sys.sp_execute_external_script
     @language = N'R'
    ,@script = N'
       d <- InputDataSet 
       c <- data.frame(Num_V1 = c(1,2,3))
       c
       OutputDataSet <- c'
    ,@input_data_1 = N'SELECT 1 AS Nmbrs_From_R'

WITH RESULT SETS ((Numbers_From_R INT));



/*
1. storing the R code in table
*/

CREATE TABLE R_code 
(
 id INT
,R NVARCHAR(MAX)
)

INSERT INTO R_code
SELECT 1, '
       d <- InputDataSet 
       c <- data.frame(Num_V1 = c(1,2,3))
       c
       OutputDataSet <- c'
-- (1 row(s) affected)


DECLARE @r_code NVARCHAR(MAX)
SELECT @r_code = R FROM R_code WHERE id = 1

EXEC sys.sp_execute_external_script
     @language = N'R'
    ,@script = @r_code
    ,@input_data_1 = N'SELECT 1 AS Nmbrs_From_R'
WITH RESULT SETS ((Numbers_From_R INT));


DROP TABLE IF EXISTS R_code


/*
2. storing the R code and T-SQL code in table
*/

CREATE TABLE R_code 
(
 id INT
,R NVARCHAR(MAX)
,SQLC NVARCHAR(MAX)
)

INSERT INTO R_code
SELECT 1, '
       d <- InputDataSet 
       c <- data.frame(Num_V1 = c(1,2,3))
       c
       OutputDataSet <- c','SELECT 1 AS Nmbrs_From_R'
-- (1 row(s) affected)


DECLARE @r_code NVARCHAR(MAX)
DECLARE @sql_code NVARCHAR(MAX)
SELECT @r_code = R FROM R_code WHERE id = 1
SELECT @sql_code = SQLC FROM R_code WHERE id = 1


EXEC sys.sp_execute_external_script
     @language = N'R'
    ,@script = @r_code
    ,@input_data_1 = @sql_code
WITH RESULT SETS ((Numbers_From_R INT));

-- DROP TABLE IF EXISTS R_code

/*
3. Storing R Code and TSQL code in Temporal table
*/

CREATE TABLE R_code 
(
 id INT IDENTITY(1,1)
,CombinationID INT NOT NULL CONSTRAINT PK_ComboID PRIMARY KEY
,R NVARCHAR(MAX)
,SQLC NVARCHAR(MAX)
,Valid_From DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL
,Valid_To DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL
,PERIOD FOR SYSTEM_TIME (Valid_From, Valid_To)
)
WITH (SYSTEM_VERSIONING = ON);


INSERT INTO R_code (CombinationID, R, SQLC)
SELECT 1,'
       d <- InputDataSet 
       c <- data.frame(Num_V1 = c(1,2,3))
       c
       OutputDataSet <- c','SELECT 1 AS Nmbrs_From_R'
-- (1 row(s) affected)


SELECT  * FROM [dbo].[R_code]


DECLARE @r_code NVARCHAR(MAX)
DECLARE @sql_code NVARCHAR(MAX)
SELECT @r_code = R FROM R_code WHERE CombinationID = 1
SELECT @sql_code = SQLC FROM R_code WHERE CombinationID = 1


EXEC sys.sp_execute_external_script
     @language = N'R'
    ,@script = @r_code
    ,@input_data_1 = @sql_code
WITH RESULT SETS ((Numbers_From_R INT));


-- INSERT ANOTHER CombinationID = 1, with changed R Code
UPDATE R_code
SET R = '
       d <- InputDataSet 
       c <- data.frame(Num_V1 = c(1,2,3,4))
       c
	   d
       OutputDataSet <- c'


,SQLC = 'SELECT 1 AS Nmbrs_From_R'
WHERE
	CombinationID = 1
-- (1 row(s) affected)

--RUNNING THE SAME CODE, TEMP TABLE DOES Everything to retrieve correct Row

DECLARE @r_code NVARCHAR(MAX)
DECLARE @sql_code NVARCHAR(MAX)
SELECT @r_code = R FROM R_code WHERE CombinationID = 1
SELECT @sql_code = SQLC FROM R_code WHERE CombinationID = 1


EXEC sys.sp_execute_external_script
     @language = N'R'
    ,@script = @r_code
    ,@input_data_1 = @sql_code
WITH RESULT SETS ((Numbers_From_R INT));


-- To retrieve all the versions from temporal table
SELECT id, CombinationID, R, SQLC, Valid_From, Valid_To
FROM [dbo].[R_code] 
WHERE CombinationID = 1
UNION ALL
SELECT id, CombinationID, R, SQLC, Valid_From, Valid_To
FROM [dbo].[MSSQL_TemporalHistoryFor_130099504]
WHERE CombinationID = 1


SELECT id, CombinationID, R, SQLC, Valid_From, Valid_To 
FROM [dbo].[R_code] FOR SYSTEM_TIME BETWEEN '2017-04-17 17:03:00' AND '2017-04-17 17:10:00'
WHERE CombinationID = 1


/*
4. storing the R code in file table
*/

-- Check configuration:
SELECT db_name()

--- Check configurations
SELECT 
  DB_NAME(database_id) AS DbName
 ,non_transacted_access
 ,non_transacted_access_desc
 ,directory_name  
 ,*
FROM  sys.database_filestream_options
WHERE 
	DB_NAME(database_id) = db_name() -- 'FileTableRChart'


--- Check files
SELECT 
	 FT.Name AS [File Name]
	,IIF(FT.is_directory=1,'Directory','Files') AS [File Category]
	,FT.file_type AS [File Type]
	,(FT.cached_file_size)/1024.0 AS [File Size (KB)]
	,FT.creation_time AS [File Created Time]
	,FT.file_stream.GetFileNamespacePath(1,0) AS [File Path]
	,ISNULL(PT.file_stream.GetFileNamespacePath(1,0),'Root Directory') AS [Parent Path]
FROM 
	[dbo].[ChartsR] AS FT
LEFT JOIN [dbo].[ChartsR] AS PT
ON FT.path_locator.GetAncestor(1) = PT.path_locator
WHERE
	FT.File_type = 'R'

-- You can upload the file using
INSERT INTO [dbo].[ChartsR]  ([name],[file_stream])
SELECT'R_Combination1.R',
* FROM OPENROWSET(BULK N'C:\DataTK\00\R_Combination1.R', SINGLE_BLOB) AS FileData
GO

-- or by copy/pasting or drag & drop the file into the FileTable location folder


--- Getting R code into BLOB
SELECT 
	 FT.Name AS [File Name]
	,IIF(FT.is_directory=1,'Directory','Files') AS [File Category]
	,FT.file_type AS [File Type]
	,(FT.cached_file_size)/1024.0 AS [File Size (KB)]
	,FT.creation_time AS [File Created Time]
	,FT.file_stream.GetFileNamespacePath(1,0) AS [File Path]
	,ISNULL(PT.file_stream.GetFileNamespacePath(1,0),'Root Directory') AS [Parent Path]
	,*
FROM 
	[dbo].[ChartsR] AS FT
LEFT JOIN [dbo].[ChartsR] AS PT
ON FT.path_locator.GetAncestor(1) = PT.path_locator
WHERE
	FT.File_type = 'R'