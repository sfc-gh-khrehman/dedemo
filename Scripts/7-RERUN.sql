use schema DEMO_DB.RAW;
--drop incremental files from stage
ls @DEMO_DB.RAW.S3_STAGE;
--remove only incremental files
rm @DEMO_DB.RAW.S3_STAGE/customers/customers_incremental_20241126.csv;
rm @DEMO_DB.RAW.S3_STAGE/orders/orders_incremental_20241126.csv;

--To re-run with same data, truncate tables to clear metadata
truncate table DEMO_DB.RAW.CUSTOMERS;
truncate table DEMO_DB.RAW.ORDERS;

truncate table DEMO_DB.INT.CUSTOMERS;
truncate table DEMO_DB.INT.ORDERS;

--validate 
select count(*) from DEMO_DB.RAW.CUSTOMERS;
select count(*) from DEMO_DB.RAW.ORDERS;

select count(*) from  DEMO_DB.INT.CUSTOMERS;
select count(*) from  DEMO_DB.INT.ORDERS;

--no conent at streams since the data was already consumed
CREATE OR REPLACE TEMP TABLE RESET_TBL AS
SELECT * FROM DEMO_DB.RAW.CUSTOMERS_STREAM;

CREATE OR REPLACE TEMP TABLE RESET_TBL AS
SELECT * FROM DEMO_DB.RAW.ORDERS_STREAM;

drop table RESET_TBL;

select count(*) from DEMO_DB.RAW.CUSTOMERS_STREAM;
select count(*) from DEMO_DB.RAW.ORDERS_STREAM;






