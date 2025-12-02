-- Adding a PK to these 2 table to allow pg_repack to process these tables, enabling reclaiming disk space and defragmenting the tables.

-- in rev 10, we need to modify the table InvalidChildCounts that is created in rev 4 but,
-- the table is actually created in PrepareDatabase.sql that will be run after the upgrade script
-- hence this check before executin the ALTER TABLE.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = 'invalidchildcounts'
    ) THEN
        ALTER TABLE InvalidChildCounts ADD COLUMN pk BIGSERIAL PRIMARY KEY;
    END IF;
END $$;

-- same for GlobalIntegersChanges table that is created in rev2:
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = 'globalintegerschanges'
    ) THEN
        ALTER TABLE GlobalIntegersChanges ADD COLUMN pk BIGSERIAL PRIMARY KEY;
    END IF;
END $$;


-- Adding the queues timeout (Queues table was created in rev5)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = 'queues'
    ) THEN
        ALTER TABLE Queues ADD COLUMN reservedUntil BIGINT DEFAULT NULL;
    END IF;
END $$;
