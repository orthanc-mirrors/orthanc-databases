-- This file contains an SQL procedure to downgrade from schema Rev3 to Rev2 (version = 6, revision = 1).
  -- It actually deletes the ChildCount table and triggers
  -- It actually does not uninstall ChildrenIndex2 because it is anyway more efficient than 
     -- ChildrenIndex and is not incompatible with previous revisions.

-- remove the childCount column in resources
DO $body$
BEGIN
	IF EXISTS (SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='resources' AND column_name='childcount') THEN
		ALTER TABLE Resources DROP COLUMN childcount;
	ELSE
		raise notice 'the resources.childcount column does not exists';
	END IF;

END $body$;


------------------- re-install old CreateInstance function -------------------
CREATE OR REPLACE FUNCTION CreateInstance(
  IN patient_public_id TEXT,
  IN study_public_id TEXT,
  IN series_public_id TEXT,
  IN instance_public_id TEXT,
  OUT is_new_patient BIGINT,
  OUT is_new_study BIGINT,
  OUT is_new_series BIGINT,
  OUT is_new_instance BIGINT,
  OUT patient_internal_id BIGINT,
  OUT study_internal_id BIGINT,
  OUT series_internal_id BIGINT,
  OUT instance_internal_id BIGINT) AS $body$

BEGIN
	is_new_patient := 1;
	is_new_study := 1;
	is_new_series := 1;
	is_new_instance := 1;

	BEGIN
        INSERT INTO "resources" VALUES (DEFAULT, 0, patient_public_id, NULL) RETURNING internalid INTO patient_internal_id;
    EXCEPTION
        WHEN unique_violation THEN
            is_new_patient := 0;
            SELECT internalid INTO patient_internal_id FROM "resources" WHERE publicId = patient_public_id FOR UPDATE;  -- also locks the resource and its parent to prevent from deletion while we complete this transaction
    END;

	BEGIN
        INSERT INTO "resources" VALUES (DEFAULT, 1, study_public_id, patient_internal_id) RETURNING internalid INTO study_internal_id;
    EXCEPTION
        WHEN unique_violation THEN
            is_new_study := 0;
            SELECT internalid INTO study_internal_id FROM "resources" WHERE publicId = study_public_id FOR UPDATE;  -- also locks the resource and its parent to prevent from deletion while we complete this transaction    END;
    END;

	BEGIN
	    INSERT INTO "resources" VALUES (DEFAULT, 2, series_public_id, study_internal_id) RETURNING internalid INTO series_internal_id;
    EXCEPTION
        WHEN unique_violation THEN
            is_new_series := 0;
            SELECT internalid INTO series_internal_id FROM "resources" WHERE publicId = series_public_id FOR UPDATE;  -- also locks the resource and its parent to prevent from deletion while we complete this transaction    END;
    END;

  	BEGIN
		INSERT INTO "resources" VALUES (DEFAULT, 3, instance_public_id, series_internal_id) RETURNING internalid INTO instance_internal_id;
    EXCEPTION
        WHEN unique_violation THEN
            is_new_instance := 0;
            SELECT internalid INTO instance_internal_id FROM "resources" WHERE publicId = instance_public_id FOR UPDATE;  -- also locks the resource and its parent to prevent from deletion while we complete this transaction
    END;

    IF is_new_instance > 0 THEN
        -- Move the patient to the end of the recycling order.
        PERFORM PatientAddedOrUpdated(patient_internal_id, 1);
    END IF;  
END;
$body$ LANGUAGE plpgsql;




-- set the global properties that actually documents the DB version, revision and some of the capabilities
-- modify only the ones that have changed
DELETE FROM GlobalProperties WHERE property IN (4, 11);
INSERT INTO GlobalProperties VALUES (4, 2); -- GlobalProperty_DatabasePatchLevel
