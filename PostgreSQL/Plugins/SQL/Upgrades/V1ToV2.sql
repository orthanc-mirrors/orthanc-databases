-- This file contains part of the changes required to upgrade from revision 1 to revision 2 (v 6.0)
-- It actually contains only the changes that:
   -- can not be executed with an idempotent statement in SQL
   -- or would polute the PrepareIndexV2.sql
-- This file is executed only if the current schema is in revision 1 and it is executed before PrepareIndexV2.sql
-- that is idempotent.

-- add unique constraints if they donot exists
DO $body$
BEGIN

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = 'public' 
            AND table_name = 'resources'
            AND constraint_name = 'uniquepublicid')
    THEN
        ALTER TABLE Resources ADD CONSTRAINT UniquePublicId UNIQUE (publicId);
        RAISE NOTICE 'UniquePublicId constraint added to Resources.';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = 'public' 
            AND table_name = 'patientrecyclingorder'
            AND constraint_name = 'uniquepatientid')
    THEN
        ALTER TABLE PatientRecyclingOrder ADD CONSTRAINT UniquePatientId UNIQUE (patientId);
        RAISE NOTICE 'UniquePatientId constraint added to PatientRecyclingOrder.';
    END IF;

END $body$ LANGUAGE plpgsql;


-- In V2, we'll now use temporary tables so we need to remove the old tables that might have been used in previous revisions !
-- these statements, although idempotent, are not part of PrepareIndexV2.sql to keep it clean
DROP TABLE IF EXISTS DeletedFiles;
DROP TABLE IF EXISTS RemainingAncestor;
DROP TABLE IF EXISTS DeletedResources;

-- These triggers disappears and are not replaced in V2
DROP TRIGGER IF EXISTS CountResourcesTracker ON Resources;

