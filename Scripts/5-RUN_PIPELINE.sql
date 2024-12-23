use schema DEMO_DB.RAW;

--make sure you've uploaded files via the Snowsight UI in the internal stage

--load data
EXECUTE TASK DEMO_DB.RAW.LOAD_RAW_DATA_TASK;

--task history
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('hour',-1,current_timestamp()),
    RESULT_LIMIT => 10,
    TASK_NAME=>'LOAD_RAW_DATA_TASK'));