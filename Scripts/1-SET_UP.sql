-- Create Database and Schemas
CREATE DATABASE IF NOT EXISTS DEMO_DB;
USE DATABASE DEMO_DB;

ALTER DATABASE SET DATA_RETENTION_TIME_IN_DAYS =1;


CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS INT;
CREATE SCHEMA IF NOT EXISTS PRS;

-- Create file format for CSV files
CREATE OR REPLACE FILE FORMAT DEMO_DB.RAW.CSV_FILE_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Create external stage pointing to S3
CREATE OR REPLACE STAGE DEMO_DB.RAW.S3_STAGE
    FILE_FORMAT = DEMO_DB.RAW.CSV_FILE_FORMAT
    DIRECTORY = (ENABLE = TRUE);

-- Create RAW tables
CREATE OR REPLACE TABLE DEMO_DB.RAW.CUSTOMERS (
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP,
    raw_file_name VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE DEMO_DB.RAW.ORDERS (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_date DATE,
    order_status VARCHAR,
    amount DECIMAL(10,2),
    raw_file_name VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create streams on RAW tables
CREATE OR REPLACE STREAM DEMO_DB.RAW.CUSTOMERS_STREAM ON TABLE DEMO_DB.RAW.CUSTOMERS;
CREATE OR REPLACE STREAM DEMO_DB.RAW.ORDERS_STREAM ON TABLE DEMO_DB.RAW.ORDERS;

-- Create INT tables
CREATE OR REPLACE TABLE DEMO_DB.INT.CUSTOMERS (
    customer_sk NUMBER AUTOINCREMENT,
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE DEMO_DB.INT.ORDERS (
    order_sk NUMBER AUTOINCREMENT,
    order_id VARCHAR,
    customer_sk NUMBER,
    order_date DATE,
    order_status VARCHAR,
    amount DECIMAL(10,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);