DO $$
DECLARE
    pg_version text;
BEGIN
    SELECT version() INTO pg_version;

    IF substring(pg_version from 'PostgreSQL (\d+)\.')::int >= 11 THEN
        -- PostgreSQL 11 or later
        EXECUTE 'CREATE INDEX IF NOT EXISTS ChildrenIndex2 ON Resources USING btree (parentId ASC NULLS LAST) INCLUDE (publicId, internalId)';
    ELSE
        EXECUTE 'CREATE INDEX IF NOT EXISTS ChildrenIndex2 ON Resources USING btree (parentId ASC NULLS LAST, publicId, internalId)';
    END IF;
END $$;

DROP INDEX IF EXISTS ChildrenIndex;  -- replaced by ChildrenIndex2 but no need to uninstall ChildrenIndex2 when downgrading