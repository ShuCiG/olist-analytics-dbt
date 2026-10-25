-- One-off migration (D70): day partitions on `_loaded_at` for the raw order tables
-- of an existing BigQuery dataset. dlt sets partitioning only when it creates a
-- table, and BigQuery can't add it to an existing one, so each table is copied
-- into a partitioned twin and swapped in. Rows, `_loaded_at` and the dlt state are
-- unchanged: no reload, no full refresh downstream.
--
-- Run in the BigQuery console (location EU) as a script, once per dataset: set `ds`
-- below (`raw` for prod, `airflow_raw` for Airflow). Nothing may load or build the
-- dataset meanwhile (no CD deploy running, Airflow DAG paused): between DROP and
-- RENAME the table does not exist. Already partitioned tables are skipped, so a
-- rerun is safe.
DECLARE ds STRING DEFAULT 'raw';
DECLARE partitioned BOOL;

FOR t IN (
    SELECT table_name
    FROM UNNEST(['orders', 'order_items', 'order_payments', 'order_reviews']) AS table_name
)
DO
    EXECUTE IMMEDIATE FORMAT(
        "SELECT COUNT(*) > 0 FROM `%s`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = @name AND is_partitioning_column = 'YES'",
        ds
    ) INTO partitioned USING t.table_name AS name;
    IF partitioned THEN
        SELECT FORMAT('%s.%s already partitioned: skipped', ds, t.table_name) AS status;
    ELSE
        -- LIKE keeps the schema with its REQUIRED primary key columns (CTAS would not)
        EXECUTE IMMEDIATE FORMAT(
            'CREATE TABLE `%s.%s__partitioned` LIKE `%s.%s` PARTITION BY DATE(_loaded_at)',
            ds, t.table_name, ds, t.table_name
        );
        EXECUTE IMMEDIATE FORMAT(
            'INSERT INTO `%s.%s__partitioned` SELECT * FROM `%s.%s`',
            ds, t.table_name, ds, t.table_name
        );
        EXECUTE IMMEDIATE FORMAT(
            '''SELECT IF(
                (SELECT COUNT(*) FROM `%s.%s`) = (SELECT COUNT(*) FROM `%s.%s__partitioned`),
                'row counts match', ERROR('row count mismatch: %s not swapped'))''',
            ds, t.table_name, ds, t.table_name, t.table_name
        );
        EXECUTE IMMEDIATE FORMAT('DROP TABLE `%s.%s`', ds, t.table_name);
        EXECUTE IMMEDIATE FORMAT(
            'ALTER TABLE `%s.%s__partitioned` RENAME TO %s', ds, t.table_name, t.table_name
        );
    END IF;
END FOR;
