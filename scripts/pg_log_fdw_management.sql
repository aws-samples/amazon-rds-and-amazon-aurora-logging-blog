-- Yaser Raja
-- AWS Professional Services
--
-- This function uses log_fdw to load all the available RDS / Aurora PostgreSQL DB log files as a table.
--
-- Usage:
--    1) Create this function
--    2) Run the following to load all the log files
--          SELECT public.load_postgres_log_files();
--    3) Start looking at the logs
--          SELECT * FROM logs.postgres_logs;
--
-- Here are the key features:
--   - By default, a table named "postgres_logs" is created in schema "logs".
--   - The schema name and table name can be changed via arguments.
--   - If the table already exists, it will be DROPPED
--   - If the schema 'logs' does not exist, it will be created.
--   - Each log file is loaded as a foreign table and then made child of table logs.postgres_logs
--   - By default, CSV file format is preferred, it can be changed via argument v_prefer_csv
--   - Daily, hourly and minute-based log file name formats are supported for CSV and non-CSV output files
--       - postgresql.log.YYYY-MM-DD-HHMI
--       - postgresql.log.YYYY-MM-DD-HH
--       - postgresql.log.YYYY-MM-DD
--   - Supports the scenario where log files list consist of both the file name formats
--   - When CSV format is used, a check-constraint is added to the child table created for each log file
--
CREATE OR REPLACE FUNCTION public.load_postgres_log_files(v_schema_name TEXT DEFAULT 'logs', v_table_name TEXT DEFAULT 'postgres_logs', v_prefer_csv BOOLEAN DEFAULT TRUE)
RETURNS TEXT
AS
$BODY$
DECLARE
    v_csv_supported INT := 0;
    v_hour_pattern_used INT := 0;
    v_filename TEXT;
    v_dt timestamptz;
    v_dt_max timestamptz;
    v_partition_name TEXT;
    v_ext_exists INT := 0;
    v_server_exists INT := 0;
    v_table_exists INT := 0;
    v_server_name TEXT := 'log_server';
    v_filelist_sql TEXT;
    v_enable_csv BOOLEAN := TRUE;
BEGIN
    EXECUTE FORMAT('SELECT count(1) FROM pg_catalog.pg_extension WHERE extname=%L', 'log_fdw') INTO v_ext_exists;
    IF v_ext_exists = 0 THEN
        CREATE EXTENSION log_fdw;
    END IF;

    EXECUTE 'SELECT count(1) FROM pg_catalog.pg_foreign_server WHERE srvname=$1' INTO v_server_exists USING v_server_name;
    IF v_server_exists = 0 THEN
        EXECUTE FORMAT('CREATE SERVER %s FOREIGN DATA WRAPPER log_fdw', v_server_name);
    END IF;

    EXECUTE FORMAT('CREATE SCHEMA IF NOT EXISTS %I', v_schema_name);

    -- Set the search path to make sure the tables are created in dblogs schema
    EXECUTE FORMAT('SELECT set_config(%L, %L, TRUE)', 'search_path', v_schema_name);

    -- The db log files are in UTC timezone so that date extracted from filename will also be UTC.
    --    Setting timezone to get correct table constraints.
    EXECUTE FORMAT('SELECT set_config(%L, %L, TRUE)', 'timezone', 'UTC');

    -- Check the parent table exists
    EXECUTE 'SELECT count(1) FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2' INTO v_table_exists USING v_schema_name, v_table_name;
    IF v_table_exists = 1 THEN
        RAISE NOTICE 'Table % already exists. It will be dropped.', v_table_name;
        EXECUTE FORMAT('SELECT set_config(%L, %L, TRUE)', 'client_min_messages', 'WARNING');
        EXECUTE FORMAT('DROP TABLE %I CASCADE', v_table_name);
        EXECUTE FORMAT('SELECT set_config(%L, %L, TRUE)', 'client_min_messages', 'NOTICE');
        v_table_exists = 0;
    END IF;

    -- Check the pg log format
    SELECT 1 INTO v_csv_supported FROM pg_catalog.pg_settings WHERE name='log_destination' AND setting LIKE '%csvlog%';
    IF v_csv_supported = 1 AND v_prefer_csv = TRUE THEN
        RAISE NOTICE 'CSV log format will be used.';
        v_filelist_sql = FORMAT('SELECT file_name FROM public.list_postgres_log_files() WHERE file_name LIKE %L ORDER BY 1 DESC', '%.csv');
    ELSE
        RAISE NOTICE 'Default log format will be used.';
        v_filelist_sql = FORMAT('SELECT file_name FROM public.list_postgres_log_files() WHERE file_name NOT LIKE %L ORDER BY 1 DESC', '%.csv');
        v_enable_csv = FALSE;
    END IF;

    FOR v_filename IN EXECUTE (v_filelist_sql)
    LOOP
        RAISE NOTICE 'Processing log file - %', v_filename;

        IF v_enable_csv = TRUE THEN
            -- Dynamically checking the file name pattern so that both allowed file names patters are parsed
            IF v_filename like 'postgresql.log.____-__-__-____.csv' THEN
                v_dt=substring(v_filename from 'postgresql.log.#"%#"-____.csv' for '#')::timestamp + INTERVAL '1 HOUR' * (substring(v_filename from 'postgresql.log.____-__-__-#"%#"__.csv' for '#')::int);
                v_dt_max = v_dt + INTERVAL '1 HOUR';
                v_dt=substring(v_filename from 'postgresql.log.#"%#"-____.csv' for '#')::timestamp + INTERVAL '1 HOUR' * (substring(v_filename from 'postgresql.log.____-__-__-#"%#"__.csv' for '#')::int) + INTERVAL '1 MINUTE' * (substring(v_filename from 'postgresql.log.____-__-__-__#"%#".csv' for '#')::int);
            ELSIF v_filename like 'postgresql.log.____-__-__-__.csv' THEN
                v_dt=substring(v_filename from 'postgresql.log.#"%#"-__.csv' for '#')::timestamp + INTERVAL '1 HOUR' * (substring(v_filename from 'postgresql.log.____-__-__-#"%#".csv' for '#')::int);
                v_dt_max = v_dt + INTERVAL '1 HOUR';
            ELSIF v_filename like 'postgresql.log.____-__-__.csv' THEN
                v_dt=substring(v_filename from 'postgresql.log.#"%#".csv' for '#')::timestamp;
                v_dt_max = v_dt + INTERVAL '1 DAY';
            ELSE
                RAISE NOTICE '        Skipping file';
                CONTINUE;
            END IF;
        ELSE
            IF v_filename like 'postgresql.log.____-__-__-____' THEN
                v_dt=substring(v_filename from 'postgresql.log.#"%#"-____' for '#')::timestamp + INTERVAL '1 HOUR' * (substring(v_filename from 'postgresql.log.____-__-__-#"%#"__' for '#')::int) + INTERVAL '1 MINUTE' * (substring(v_filename from 'postgresql.log.____-__-__-__#"%#"' for '#')::int);
            ELSIF v_filename like 'postgresql.log.____-__-__-__' THEN
                v_dt=substring(v_filename from 'postgresql.log.#"%#"-__' for '#')::timestamp + INTERVAL '1 HOUR' * (substring(v_filename from 'postgresql.log.____-__-__-#"%#"' for '#')::int);
            ELSIF v_filename like 'postgresql.log.____-__-__' THEN
                v_dt=substring(v_filename from 'postgresql.log.#"%#"' for '#')::timestamp;
            ELSE
                RAISE NOTICE '        Skipping file';
                CONTINUE;
            END IF;
        END IF;
        v_partition_name=CONCAT(v_table_name, '_', to_char(v_dt, 'YYYYMMDD_HH24MI'));
        EXECUTE FORMAT('SELECT public.create_foreign_table_for_log_file(%L, %L, %L)', v_partition_name, v_server_name, v_filename);

        IF v_table_exists = 0 THEN
            EXECUTE FORMAT('CREATE TABLE %I (LIKE %I INCLUDING ALL)', v_table_name, v_partition_name);
            v_table_exists = 1;
        END IF;

        EXECUTE FORMAT('ALTER TABLE %I INHERIT %I', v_partition_name, v_table_name);

        IF v_enable_csv = TRUE THEN
            EXECUTE FORMAT('ALTER TABLE %I ADD CONSTRAINT check_date_range CHECK (log_time>=%L and log_time < %L)', v_partition_name, v_dt, v_dt_max);
        END IF;

    END LOOP;

    RETURN FORMAT('Postgres logs loaded to table %I.%I', v_schema_name, v_table_name);
END;
$BODY$
LANGUAGE plpgsql;
