-- Create dynamic table for customer order summary
CREATE OR REPLACE DYNAMIC TABLE DEMO_DB.PRS.CUSTOMER_ORDER_SUMMARY
TARGET_LAG = '1 minute'
WAREHOUSE = 'COMPUTE_WH'
AS
SELECT 
    c.customer_sk,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_amount,
    MAX(o.order_date) as last_order_date
FROM DEMO_DB.INT.CUSTOMERS c
LEFT JOIN DEMO_DB.INT.ORDERS o ON c.customer_sk = o.customer_sk
GROUP BY 1,2,3,4;

-- Create presentation views
CREATE OR REPLACE VIEW DEMO_DB.PRS.VW_CUSTOMER_PROFILE AS
SELECT 
    c.customer_sk,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    s.total_orders,
    s.total_amount,
    s.last_order_date,
    c.processed_at as last_updated
FROM DEMO_DB.INT.CUSTOMERS c
JOIN DEMO_DB.PRS.CUSTOMER_ORDER_SUMMARY s ON c.customer_sk = s.customer_sk;

CREATE OR REPLACE VIEW DEMO_DB.PRS.VW_ORDER_DETAILS AS
SELECT 
    o.order_sk,
    o.order_id,
    c.customer_id,
    c.first_name,
    c.last_name,
    o.order_date,
    o.order_status,
    o.amount,
    o.processed_at as last_updated
FROM DEMO_DB.INT.ORDERS o
JOIN DEMO_DB.INT.CUSTOMERS c ON o.customer_sk = c.customer_sk;

show parameters;