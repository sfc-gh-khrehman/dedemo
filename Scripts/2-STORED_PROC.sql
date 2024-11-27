use schema DEMO_DB.RAW;
-- Procedure to load data from STAGE to RAW tables
CREATE OR REPLACE PROCEDURE DEMO_DB.RAW.LOAD_RAW_DATA(FILE_PATTERN STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    // Load Customers Table
    var customersSql = `
        COPY INTO DEMO_DB.RAW.CUSTOMERS (
            customer_id, first_name, last_name, email, created_at, raw_file_name
        )
        FROM (
            SELECT $1, $2, $3, $4, $5, metadata$filename
            FROM @DEMO_DB.RAW.S3_STAGE/customers/
        )
        PATTERN = '` + FILE_PATTERN + `'
        ON_ERROR = 'CONTINUE'
    `;
    
    var custStmt = snowflake.createStatement({sqlText: customersSql});
    var custResult = custStmt.execute();
    
    // Load Orders Table
    var ordersSql = `
        COPY INTO DEMO_DB.RAW.ORDERS (
            order_id, customer_id, order_date, order_status, amount, raw_file_name
        )
        FROM (
            SELECT $1, $2, $3, $4, $5, metadata$filename
            FROM @DEMO_DB.RAW.S3_STAGE/orders/
        )
        PATTERN = '` + FILE_PATTERN + `'
        ON_ERROR = 'CONTINUE'
    `;
    
    var ordStmt = snowflake.createStatement({sqlText: ordersSql});
    var ordResult = ordStmt.execute();
    
    return "Data loaded successfully";
} catch (err) {
    return "Error: " + err;
}
$$;

-- Procedure to process RAW to INT tables
CREATE OR REPLACE PROCEDURE DEMO_DB.RAW.PROCESS_RAW_DATA()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    // Process Customers
    var customersSql = `
        MERGE INTO DEMO_DB.INT.CUSTOMERS tgt
        USING (
            SELECT 
                customer_id,
                first_name,
                last_name,
                email,
                created_at
            FROM DEMO_DB.RAW.CUSTOMERS_STREAM
        ) src
        ON tgt.customer_id = src.customer_id
        WHEN MATCHED THEN UPDATE SET
            tgt.first_name = src.first_name,
            tgt.last_name = src.last_name,
            tgt.email = src.email,
            tgt.created_at = src.created_at,
            tgt.processed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            customer_id, first_name, last_name, email, created_at
        ) VALUES (
            src.customer_id, src.first_name, src.last_name, src.email, src.created_at
        )`;
    
    var custStmt = snowflake.createStatement({sqlText: customersSql});
    custStmt.execute();
    
    // Process Orders
    var ordersSql = `
        MERGE INTO DEMO_DB.INT.ORDERS tgt
        USING (
            SELECT 
                o.order_id,
                c.customer_sk,
                o.order_date,
                o.order_status,
                o.amount
            FROM DEMO_DB.RAW.ORDERS_STREAM o
            JOIN DEMO_DB.INT.CUSTOMERS c ON o.customer_id = c.customer_id
        ) src
        ON tgt.order_id = src.order_id
        WHEN MATCHED THEN UPDATE SET
            tgt.customer_sk = src.customer_sk,
            tgt.order_date = src.order_date,
            tgt.order_status = src.order_status,
            tgt.amount = src.amount,
            tgt.processed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            order_id, customer_sk, order_date, order_status, amount
        ) VALUES (
            src.order_id, src.customer_sk, src.order_date, src.order_status, src.amount
        )`;
    
    var ordStmt = snowflake.createStatement({sqlText: ordersSql});
    ordStmt.execute();
    
    return "Data processed successfully";
} catch (err) {
    return "Error: " + err;
}
$$;