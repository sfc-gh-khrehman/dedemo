use schema DEMO_DB.RAW;

--check data in raw tables
select * from DEMO_DB.RAW.CUSTOMERS;
select * from DEMO_DB.RAW.ORDERS;

--check streams
select * from DEMO_DB.RAW.CUSTOMERS_STREAM;
select * from DEMO_DB.RAW.ORDERS_STREAM;

--check INT tables
select * from DEMO_DB.INT.CUSTOMERS;
select * from DEMO_DB.INT.ORDERS;


--PRS Layer
SELECT * FROM DEMO_DB.PRS.CUSTOMER_ORDER_SUMMARY;
SELECT * FROM DEMO_DB.PRS.VW_CUSTOMER_PROFILE;
SELECT * FROM DEMO_DB.PRS.VW_ORDER_DETAILS;







