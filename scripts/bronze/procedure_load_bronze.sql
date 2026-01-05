/*
====================================================================
Stored Procedure: Load Bronze layer from Source
====================================================================

Script of Stored Procedure to load data into the bronze schema from the source CSV files.
	Truncate the tables before loading the data.
	Bulk Insert to load data from CSV files into the tables.

Reminder:
	Save frequently used SQL code in Stored Procedures in database.

	To execute the stored procedure:
		EXEC bronze.load_bronze
====================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	-- Track ETL Duration: Identify bottlenecks, optimize performance, monitor trends, detect issues
	DECLARE @start_time DATETIME, @end_time DATETIME;
	SET @start_time = GETDATE();
	BEGIN TRY	-- Error handling, data integrity and issue logging for easy debugging(1)
		PRINT '=========================================';
		PRINT '			LOADING BRONZE LAYER';
		PRINT '=========================================';

		PRINT '------------ Loading CRM Tables ---------';

		SET @start_time = GETDATE();
		PRINT '> Truncating and Inserting data into Table: bronze.crm_cust_info <';
		-- Full load bronze.crm_cust_info 
		TRUNCATE TABLE bronze.crm_cust_info; -- FULL LOAD

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,			-- skip the first row
			FIELDTERMINATOR = ',',  -- separator/limitator between fields
			TABLOCK					-- improve performance
		);
		SET @end_time = GETDATE();
		PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '> ------------------------';
		-- SELECT * FROM bronze.crm_cust_info;		    to check if everything is in the right order
		-- SELECT COUNT(*) FROM bronze.crm_cust_info;   to check if all rows were inserted

		SET @start_time = GETDATE();
		PRINT '> Truncating and Inserting data into Table: bronze.crm_prd_info <';
		-- Full load bronze.crm_prd_info
		TRUNCATE TABLE bronze.crm_prd_info; 

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '> ------------------------';

		SET @start_time = GETDATE();
		PRINT '> Truncating and Inserting data into Table: bronze.crm_sales_details <'; 
		-- Full load bronze.crm_sales_details
		TRUNCATE TABLE bronze.crm_sales_details; 

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '> ------------------------';

		PRINT '------------ Loading ERP Tables ---------';

		SET @start_time = GETDATE();
		PRINT '> Truncating and Inserting data into Table: bronze.erp_cust_az12 <'; 
		-- Full load bronze.erp_cust_az12
		TRUNCATE TABLE bronze.erp_cust_az12; 

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '> ------------------------';

		SET @start_time = GETDATE();
		PRINT '> Truncating and Inserting data into Table: bronze.erp_loc_a101 <'; 
		-- Full load bronze.erp_loc_a101
		TRUNCATE TABLE bronze.erp_loc_a101; 

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '> ------------------------';

		SET @start_time = GETDATE();
		PRINT '> Truncating and Inserting data into Table: bronze.erp_px_cat_g1v2 <'; 
		-- Full load bronze.erp_px_cat_g1v2
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; 

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '> ------------------------';

		SET @end_time = GETDATE();
		PRINT '> Load Duration for Bronze Later: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '> ------------------------';
	END TRY

	BEGIN CATCH		-- Error handling, data integrity and issue logging for easy debugging(2)
		PRINT '======= ERROR OCCURED WHILE LOADING BRONZE LAYER =======';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	END CATCH
END
