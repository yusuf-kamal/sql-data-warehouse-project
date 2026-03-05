/*
  Create Database and schemas

  script puprpose:
  this script create a new database called 'DataWarehouse' after checking if it's exist or not 
  and if exist drop database and recreated 
  and the script create also the schemas (bronze,silver,gold)


  WARNING :
  this will drop the entire database named 'DataWarehous' if exist.
  prpceed with caution and ensure that you have backups before running this script .
*/


USE master;
GO
CREATE DATABASE DataWarehouse;

--Drop  and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases D WHERE D.name='DataWarehouse')BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
USE DataWarehouse;
GO
-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
