-- Task graph for the DBT PROJECT object (#185), applied by snowflake/deploy.py as
-- OLIST_RUNNER (tasks run as their owner). Daily, on OLIST_WH: serverless tasks
-- can't run dbt projects, and only warehouses count against the resource monitor.
--
--   DBT_ELEMENTARY (root, cron) -> DBT_BUILD          DBT_NOTIFY (finalizer)
--
-- A failed dbt command fails EXECUTE DBT PROJECT and so the task; the finalizer
-- runs after every graph run and emails the failed tasks with their errors.
-- Raw data comes from dlt outside Snowflake (trial: no external access).

USE SCHEMA OLIST.DBT_PROJECTS;

-- A graph can only be changed while its root is suspended
ALTER TASK IF EXISTS DBT_ELEMENTARY SUSPEND;

CREATE OR ALTER TASK DBT_ELEMENTARY
    WAREHOUSE = OLIST_WH
    SCHEDULE = 'USING CRON 0 6 * * * UTC'
    USER_TASK_TIMEOUT_MS = 1800000
    -- A broken project stops after three failed days instead of billing daily
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    COMMENT = 'Elementary models first: its on-run-end hooks of the build need them'
AS
    EXECUTE DBT PROJECT OLIST.DBT_PROJECTS.OLIST ARGS = 'run --select package:elementary';

CREATE OR ALTER TASK DBT_BUILD
    WAREHOUSE = OLIST_WH
    USER_TASK_TIMEOUT_MS = 3600000
    COMMENT = 'Models, snapshots, seeds and tests (target native)'
    AFTER DBT_ELEMENTARY
AS
    EXECUTE DBT PROJECT OLIST.DBT_PROJECTS.OLIST ARGS = 'build --exclude package:elementary';

CREATE OR ALTER TASK DBT_NOTIFY
    WAREHOUSE = OLIST_WH
    USER_TASK_TIMEOUT_MS = 300000
    COMMENT = 'Emails failed tasks of the graph run (integration OLIST_EMAIL, default recipient)'
    FINALIZE = DBT_ELEMENTARY
AS
    EXECUTE IMMEDIATE $$
    DECLARE
        failures STRING;
    BEGIN
        -- LISTAGG of no rows is '', not NULL
        SELECT NULLIF(LISTAGG(name || ' (' || state || ', query ' || query_id || '): ' || COALESCE(error_message, ''), '\n\n'), '')
            INTO :failures
            FROM TABLE(OLIST.INFORMATION_SCHEMA.TASK_HISTORY(
                ROOT_TASK_ID => SYSTEM$TASK_RUNTIME_INFO('CURRENT_ROOT_TASK_UUID')::STRING,
                SCHEDULED_TIME_RANGE_START => DATEADD('hour', -6, CURRENT_TIMESTAMP())))
            WHERE graph_run_group_id = SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID')
              AND state = 'FAILED';
        IF (failures IS NOT NULL) THEN
            LET body STRING := 'The dbt task graph in OLIST.DBT_PROJECTS failed.\n\n' || failures
                || '\n\nLogs: SELECT SYSTEM$GET_DBT_LOG(''<query id>'');';
            CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
                SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(:body),
                SNOWFLAKE.NOTIFICATION.INTEGRATION('OLIST_EMAIL'));
            RETURN 'notified: ' || failures;
        END IF;
        RETURN 'no failures';
    END;
    $$;

-- Resumes the finalizer and the children, then the root
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('OLIST.DBT_PROJECTS.DBT_ELEMENTARY');
