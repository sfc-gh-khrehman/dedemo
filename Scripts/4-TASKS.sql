use schema DEMO_DB.RAW;


--create tasks to schedule the pipeline
CREATE TASK DEMO_DB.RAW.LOAD_RAW_DATA_TASK 
SCHEDULE='USING CRON 0 0 * * * UTC'
SERVERLESS_TASK_MAX_STATEMENT_SIZE='XSMALL' 
AS 
CALL DEMO_DB.RAW.LOAD_RAW_DATA('.*\.csv');
;

CREATE TASK DEMO_DB.RAW.PROCESS_RAW_DATA_TASK 
AFTER DEMO_DB.RAW.LOAD_RAW_DATA_TASK
AS 
CALL DEMO_DB.RAW.PROCESS_RAW_DATA();
;

show tasks in schema DEMO_DB.RAW;

-- Resume tasks
ALTER TASK DEMO_DB.RAW.PROCESS_RAW_DATA_TASK RESUME;
ALTER TASK DEMO_DB.RAW.LOAD_RAW_DATA_TASK RESUME;

--trigger task manually
--EXECUTE TASK DEMO_DB.RAW.LOAD_RAW_DATA_TASK;

--suspend task, first child and then parent
--ALTER TASK DEMO_DB.RAW.LOAD_RAW_DATA_TASK SUSPEND;
--ALTER TASK DEMO_DB.RAW.PROCESS_RAW_DATA_TASK SUSPEND;