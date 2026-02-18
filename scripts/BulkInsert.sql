--method to insert data to bronze layer - bulk insert, which is used to quickly insert all data from a given csv or txt file

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @global_start_time DATETIME, @global_end_time DATETIME
    BEGIN TRY
        SET @global_start_time = GETDATE()

        PRINT '==========================================================='
        PRINT 'LOADING BRONZE LAYER'
        PRINT '==========================================================='

        PRINT '-----------------------------------------------------------'
        PRINT 'LOADING CRM SOURCE SYSTEM'
        PRINT '-----------------------------------------------------------'

        SET @start_time = GETDATE()
        PRINT '>>>>> LOADING TABLE bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info --make it empty then load data - the main idea behind "full load", basically refreshing the bronze layer

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\DataGrip SQL BS\BaraaDataWarehouse\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2, --first row is the header info like the column names, not actual data, so we tell sql to skip it
            FIELDTERMINATOR = ',', --the separator for the data in the file (in this case in csv its a comma)
            TABLOCK --locks the table, slightly better performance
        )
        SET @end_time = GETDATE()
        PRINT 'COMPLETED INSERTION, TIME TAKEN = ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS NVARCHAR) + ' seconds'

        SET @start_time = GETDATE()
        PRINT '>>>>> LOADING TABLE bronze.crm_prd_info'
        TRUNCATE TABLE bronze.crm_prd_info

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\DataGrip SQL BS\BaraaDataWarehouse\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time = GETDATE()
        PRINT 'COMPLETED INSERTION, TIME TAKEN = ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS NVARCHAR) + ' seconds'

        SET @start_time = GETDATE()
        PRINT '>>>>> LOADING TABLE bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details

        BULK INSERT bronze.crm_sales_details
            FROM 'C:\DataGrip SQL BS\BaraaDataWarehouse\datasets\source_crm\sales_details.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            )
        SET @end_time = GETDATE()
        PRINT 'COMPLETED INSERTION, TIME TAKEN = ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS NVARCHAR) + ' seconds'

        PRINT '-----------------------------------------------------------'
        PRINT 'LOADING ERP SOURCE SYSTEM'
        PRINT '-----------------------------------------------------------'

        SET @start_time = GETDATE()
        PRINT '>>>>> LOADING TABLE bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12

        BULK INSERT bronze.erp_cust_az12
                FROM 'C:\DataGrip SQL BS\BaraaDataWarehouse\datasets\source_erp\CUST_AZ12.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            )
        SET @end_time = GETDATE()
        PRINT 'COMPLETED INSERTION, TIME TAKEN = ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS NVARCHAR) + ' seconds'

        SET @start_time = GETDATE()
        PRINT '>>>>> LOADING TABLE bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101

        BULK INSERT bronze.erp_loc_a101
            FROM 'C:\DataGrip SQL BS\BaraaDataWarehouse\datasets\source_erp\LOC_A101.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            )
        SET @end_time = GETDATE()
        PRINT 'COMPLETED INSERTION, TIME TAKEN = ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS NVARCHAR) + ' seconds'

        SET @start_time = GETDATE()
        PRINT '>>>>> LOADING TABLE bronze.erp_px_cat_g1v2'
        TRUNCATE TABLE bronze.erp_px_cat_g1v2

        BULK INSERT bronze.erp_px_cat_g1v2
            FROM 'C:\DataGrip SQL BS\BaraaDataWarehouse\datasets\source_erp\PX_CAT_G1V2.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            )
        SET @end_time = GETDATE()
        PRINT 'COMPLETED INSERTION, TIME TAKEN = ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS NVARCHAR) + ' seconds'

        SET @global_end_time = GETDATE()
        PRINT '==========================================='
        PRINT 'FULL BATCH COMPLETED, TIME TAKEN = ' + CAST(DATEDIFF(SECOND, @global_end_time, @global_start_time) AS NVARCHAR) + ' seconds'
        PRINT '==========================================='
    END TRY

    BEGIN CATCH --ERROR CATCHER
        PRINT '======================================'
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE()
        PRINT 'ERROR MESSAGE: ' + CAST(ERROR_NUMBER() AS NVARCHAR)
        PRINT '======================================'
    END CATCH
END