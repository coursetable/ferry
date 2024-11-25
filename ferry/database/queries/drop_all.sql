DO $$
DECLARE
    obj RECORD;
BEGIN
    -- Loop through all tables in the schema
    FOR obj IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        -- Drop each table with CASCADE
        EXECUTE format('DROP TABLE %I.%I CASCADE;', 'public', obj.tablename);
    END LOOP;
END $$;
