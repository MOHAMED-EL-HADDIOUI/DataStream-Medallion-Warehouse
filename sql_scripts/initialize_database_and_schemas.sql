/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataStreamMedallionWarehouse'. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
*/

USE master;
GO

-- creating database named 'DataStreamMedallionWarehouse'
CREATE database DataStreamMedallionWarehouse;

USE DataStreamMedallionWarehouse;
GO

-- Creating schemas: bronze, silver, gold
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
