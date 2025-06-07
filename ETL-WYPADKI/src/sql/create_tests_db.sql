IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'DWH_TestLog')
    CREATE DATABASE DWH_TestLog;
GO

USE DWH_TestLog;
GO

IF OBJECT_ID('dbo.etl_test_results', 'U') IS NOT NULL
    DROP TABLE dbo.etl_test_results;
GO

CREATE TABLE dbo.etl_test_results (
    test_id INT IDENTITY(1,1) PRIMARY KEY,
    test_name VARCHAR(100) NOT NULL,
    test_description VARCHAR(255),
    test_result BIT NOT NULL, -- 1 = pass, 0 = fail
    expected_value VARCHAR(100),
    actual_value VARCHAR(100),
    error_message VARCHAR(500),
    test_timestamp DATETIME NOT NULL DEFAULT GETDATE(),  -- czas logu testu
    execution_start DATETIME NULL,                        -- czas rozpoczêcia testu
    execution_end DATETIME NULL                           -- czas zakoñczenia testu
);
GO

CREATE INDEX IX_etl_test_results_timestamp
ON dbo.etl_test_results (test_timestamp DESC);
