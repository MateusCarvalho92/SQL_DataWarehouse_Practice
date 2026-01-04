/*
=====================================
Create Database and Schemas
=====================================
Script Purpose: 
	This Script creates a new database name "DataWarehouse" after checking if it already exists.
	If it exists, it is dropped and recreated. Additionally create three Schemas "bronze" "silver" "gold".

WARNING:
	Running this script will drop the entire database "DataWarehouse" if it exists. ALL DATA WILL BE DELETED!
*/

USE master;
GO

-- Drop and recreate the "DataWarehouse" database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the "DataWarehouse" database
CREATE DATABASE DataWarehouse;
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
