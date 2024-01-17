-- This SQL file creates a DB in version.revision 6.2 directly
-- It is also run after upgrade scripts to create new tables and or create/replace triggers and functions.
-- This script is self contained, it contains everything that needs to be run to create an Orthanc DB.
-- Note to developers: 
--   - it is and must stay idempotent.
--   - it is executed when the DB is "locked", only one Orthanc instance can execute it at a given time.                

CREATE TABLE IF NOT EXISTS GlobalProperties(
       property INTEGER PRIMARY KEY,
       value TEXT
       );

CREATE TABLE IF NOT EXISTS Resources(
       internalId BIGSERIAL NOT NULL PRIMARY KEY,
       resourceType INTEGER NOT NULL,
       publicId VARCHAR(64) NOT NULL,
       parentId BIGINT REFERENCES Resources(internalId) ON DELETE CASCADE,
       CONSTRAINT UniquePublicId UNIQUE (publicId)
       );

CREATE TABLE IF NOT EXISTS MainDicomTags(
       id BIGINT REFERENCES Resources(internalId) ON DELETE CASCADE,
       tagGroup INTEGER,
       tagElement INTEGER,
       value TEXT,
       PRIMARY KEY(id, tagGroup, tagElement)
       );

CREATE TABLE IF NOT EXISTS DicomIdentifiers(
       id BIGINT REFERENCES Resources(internalId) ON DELETE CASCADE,
       tagGroup INTEGER,
       tagElement INTEGER,
       value TEXT,
       PRIMARY KEY(id, tagGroup, tagElement)
       );

CREATE TABLE IF NOT EXISTS Metadata(
       id BIGINT REFERENCES Resources(internalId) ON DELETE CASCADE,
       type INTEGER NOT NULL,
       value TEXT,
       revision INTEGER,
       PRIMARY KEY(id, type)
       );

CREATE TABLE IF NOT EXISTS AttachedFiles(
       id BIGINT REFERENCES Resources(internalId) ON DELETE CASCADE,
       fileType INTEGER,
       uuid VARCHAR(64) NOT NULL,
       compressedSize BIGINT,
       uncompressedSize BIGINT,
       compressionType INTEGER,
       uncompressedHash VARCHAR(40),
       compressedHash VARCHAR(40),
       revision INTEGER,
       PRIMARY KEY(id, fileType)
       );              

CREATE TABLE IF NOT EXISTS Changes(
       seq BIGSERIAL NOT NULL PRIMARY KEY,
       changeType INTEGER,
       internalId BIGINT REFERENCES Resources(internalId) ON DELETE CASCADE,
       resourceType INTEGER,
       date VARCHAR(64)
       );

CREATE TABLE IF NOT EXISTS ExportedResources(
       seq BIGSERIAL NOT NULL PRIMARY KEY,
       resourceType INTEGER,
       publicId VARCHAR(64),
       remoteModality TEXT,
       patientId VARCHAR(64),
       studyInstanceUid TEXT,
       seriesInstanceUid TEXT,
       sopInstanceUid TEXT,
       date VARCHAR(64)
       ); 

CREATE TABLE IF NOT EXISTS PatientRecyclingOrder(
       seq BIGSERIAL NOT NULL PRIMARY KEY,
       patientId BIGINT REFERENCES Resources(internalId) ON DELETE CASCADE,
       CONSTRAINT UniquePatientId UNIQUE (patientId)
       );

CREATE TABLE IF NOT EXISTS Labels(
        id BIGINT REFERENCES Resources(internalId) ON DELETE CASCADE,
        label TEXT, 
        PRIMARY KEY(id, label)
        );

CREATE TABLE IF NOT EXISTS GlobalIntegers(
       key INTEGER PRIMARY KEY,
       value BIGINT);
-- GlobalIntegers keys:
-- 0: CompressedSize
-- 1: UncompressedSize
-- 2: PatientsCount
-- 3: StudiesCount
-- 4: SeriesCount
-- 5: InstancesCount
-- 6: ChangeSeq
-- 7: PatientRecyclingOrderSeq

CREATE TABLE IF NOT EXISTS ServerProperties(
        server VARCHAR(64) NOT NULL,
        property INTEGER, value TEXT, 
        PRIMARY KEY(server, property)
        );

CREATE INDEX IF NOT EXISTS ChildrenIndex ON Resources(parentId);
CREATE INDEX IF NOT EXISTS PublicIndex ON Resources(publicId);
CREATE INDEX IF NOT EXISTS ResourceTypeIndex ON Resources(resourceType);
CREATE INDEX IF NOT EXISTS PatientRecyclingIndex ON PatientRecyclingOrder(patientId);

CREATE INDEX IF NOT EXISTS MainDicomTagsIndex ON MainDicomTags(id);
CREATE INDEX IF NOT EXISTS DicomIdentifiersIndex1 ON DicomIdentifiers(id);
CREATE INDEX IF NOT EXISTS DicomIdentifiersIndex2 ON DicomIdentifiers(tagGroup, tagElement);
CREATE INDEX IF NOT EXISTS DicomIdentifiersIndexValues ON DicomIdentifiers(value);

CREATE INDEX IF NOT EXISTS ChangesIndex ON Changes(internalId);
CREATE INDEX IF NOT EXISTS LabelsIndex1 ON LABELS(id);
CREATE INDEX IF NOT EXISTS LabelsIndex2 ON LABELS(label);

------------------- Trigram index creation -------------------


-- Apply fix for performance issue (speed up wildcard search by using GIN trigrams). This implements the patch suggested
-- in issue #47, BUT we also keep the original "DicomIdentifiersIndexValues", as it leads to better
-- performance for "strict" searches (i.e. searches involving no wildcard).
-- https://www.postgresql.org/docs/current/static/pgtrgm.html
-- https://orthanc.uclouvain.be/bugs/show_bug.cgi?id=47

DO $body$
begin
	IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name='pg_trgm') THEN
		CREATE EXTENSION IF NOT EXISTS pg_trgm;
        CREATE INDEX IF NOT EXISTS DicomIdentifiersIndexValues2 ON DicomIdentifiers USING gin(value gin_trgm_ops);
	ELSE
		RAISE NOTICE 'pg_trgm extension is not available on you system';
	END IF;
END $body$;


------------------- PatientAdded trigger & PatientRecyclingOrder -------------------
DROP TRIGGER IF EXISTS PatientAdded ON Resources;

CREATE OR REPLACE FUNCTION PatientAddedOrUpdated(
    IN patient_id BIGINT,
    IN is_update BIGINT
    )
RETURNS VOID AS $body$
BEGIN
    DECLARE
        newSeq BIGINT;
    BEGIN
        UPDATE GlobalIntegers SET value = value + 1 WHERE key = 7 RETURNING value INTO newSeq;
        IF is_update > 0 THEN
            -- Note: Protected patients are not listed in this table !  So, they won't be updated
            UPDATE PatientRecyclingOrder SET seq = newSeq WHERE PatientRecyclingOrder.patientId = patient_id;
        ELSE
            INSERT INTO PatientRecyclingOrder VALUES (newSeq, patient_id);
        END IF;
    END;
END;
$body$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION PatientAddedFunc() 
RETURNS TRIGGER AS $body$
BEGIN
  -- The "0" corresponds to "OrthancPluginResourceType_Patient"
  IF new.resourceType = 0 THEN
    PERFORM PatientAddedOrUpdated(new.internalId, 0);
  END IF;
  RETURN NULL;
END;
$body$ LANGUAGE plpgsql;

CREATE TRIGGER PatientAdded
AFTER INSERT ON Resources
FOR EACH ROW
EXECUTE PROCEDURE PatientAddedFunc();

-- initial population of PatientRecyclingOrderSeq
INSERT INTO GlobalIntegers
    SELECT 7, CAST(COALESCE(MAX(seq), 0) AS BIGINT) FROM PatientRecyclingOrder
    ON CONFLICT DO NOTHING;


------------------- ResourceDeleted trigger -------------------
DROP TRIGGER IF EXISTS ResourceDeleted ON Resources;

-- The following trigger combines 2 triggers from SQLite:
-- ResourceDeleted + ResourceDeletedParentCleaning
CREATE OR REPLACE FUNCTION ResourceDeletedFunc() 
RETURNS TRIGGER AS $body$
BEGIN
  -- RAISE NOTICE 'ResourceDeletedFunc %', old.publicId;
  INSERT INTO DeletedResources VALUES (old.resourceType, old.publicId);
  
  -- If this resource is the latest child, delete the parent
  DELETE FROM Resources WHERE internalId = old.parentId
                              AND NOT EXISTS (SELECT 1 FROM Resources WHERE parentId = old.parentId);
  RETURN NULL;
END;
$body$ LANGUAGE plpgsql;

CREATE TRIGGER ResourceDeleted
AFTER DELETE ON Resources
FOR EACH ROW
EXECUTE PROCEDURE ResourceDeletedFunc();


------------------- DeleteResource function -------------------

CREATE OR REPLACE FUNCTION DeleteResource(
    IN id BIGINT,
    OUT remaining_ancestor_resource_type INTEGER,
    OUT remaining_anncestor_public_id TEXT) AS $body$

DECLARE
    deleted_row RECORD;
    locked_row RECORD;

BEGIN

    SET client_min_messages = warning;   -- suppress NOTICE:  relation "deletedresources" already exists, skipping
    
    -- note: temporary tables are created at session (connection) level -> they are likely to exist
    -- these tables are used by the triggers
    CREATE TEMPORARY TABLE IF NOT EXISTS DeletedResources(
        resourceType INTEGER NOT NULL,
        publicId VARCHAR(64) NOT NULL
        );

    RESET client_min_messages;

    -- clear the temporary table in case it has been created earlier in the session
    DELETE FROM DeletedResources;
    
    -- create/clear the DeletedFiles temporary table
    PERFORM CreateDeletedFilesTemporaryTable();

    -- Before deleting an object, we need to lock its parent until the end of the transaction to avoid that
    -- 2 threads deletes the last 2 instances of a series at the same time -> none of them would realize
    -- that they are deleting the last instance and the parent resources would not be deleted.
    -- Locking only the immediate parent is sufficient to prevent from this.
    SELECT * INTO locked_row FROM resources WHERE internalid = (SELECT parentid FROM resources WHERE internalid = id) FOR UPDATE;

    -- delete the resource itself
    DELETE FROM Resources WHERE internalId=id RETURNING * INTO deleted_row;
    -- note: there is a ResourceDeletedFunc trigger that will execute here and delete the parents if there are no remaining children + 

    -- If this resource still has siblings, keep track of the remaining parent
    -- (a parent that must not be deleted but whose LastUpdate must be updated)
    SELECT resourceType, publicId INTO remaining_ancestor_resource_type, remaining_anncestor_public_id
        FROM Resources 
        WHERE internalId = deleted_row.parentId
            AND EXISTS (SELECT 1 FROM Resources WHERE parentId = deleted_row.parentId);

END;

$body$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION CreateDeletedFilesTemporaryTable(
) RETURNS VOID AS $body$

BEGIN

    SET client_min_messages = warning;   -- suppress NOTICE:  relation "deletedresources" already exists, skipping
    
    -- note: temporary tables are created at session (connection) level -> they are likely to exist
    CREATE TEMPORARY TABLE IF NOT EXISTS DeletedFiles(
        uuid VARCHAR(64) NOT NULL,
        fileType INTEGER,
        compressedSize BIGINT,
        uncompressedSize BIGINT,
        compressionType INTEGER,
        uncompressedHash VARCHAR(40),
        compressedHash VARCHAR(40)
        );

    RESET client_min_messages;

    -- clear the temporary table in case it has been created earlier in the session
    DELETE FROM DeletedFiles;
END;

$body$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION AttachedFileDeletedFunc() 
RETURNS TRIGGER AS $body$
BEGIN
  INSERT INTO DeletedFiles VALUES
    (old.uuid, old.filetype, old.compressedSize,
     old.uncompressedSize, old.compressionType,
     old.uncompressedHash, old.compressedHash);
  RETURN NULL;
END;
$body$ LANGUAGE plpgsql;

CREATE TRIGGER AttachedFileDeleted
AFTER DELETE ON AttachedFiles
FOR EACH ROW
EXECUTE PROCEDURE AttachedFileDeletedFunc();


------------------- Fast Statistics -------------------

-- initial population of GlobalIntegers if not already there
INSERT INTO GlobalIntegers
    SELECT 0, CAST(COALESCE(SUM(compressedSize), 0) AS BIGINT) FROM AttachedFiles
    ON CONFLICT DO NOTHING;

INSERT INTO GlobalIntegers
    SELECT 1, CAST(COALESCE(SUM(uncompressedSize), 0) AS BIGINT) FROM AttachedFiles
    ON CONFLICT DO NOTHING;

INSERT INTO GlobalIntegers
    SELECT 2, CAST(COALESCE(COUNT(*), 0) AS BIGINT) FROM Resources WHERE resourceType = 0  -- Count patients
    ON CONFLICT DO NOTHING;

INSERT INTO GlobalIntegers
    SELECT 3, CAST(COALESCE(COUNT(*), 0) AS BIGINT) FROM Resources WHERE resourceType = 1  -- Count studies
    ON CONFLICT DO NOTHING;

INSERT INTO GlobalIntegers
    SELECT 4, CAST(COALESCE(COUNT(*), 0) AS BIGINT) FROM Resources WHERE resourceType = 2  -- Count series
    ON CONFLICT DO NOTHING;

INSERT INTO GlobalIntegers
    SELECT 5, CAST(COALESCE(COUNT(*), 0) AS BIGINT) FROM Resources WHERE resourceType = 3  -- Count instances
    ON CONFLICT DO NOTHING;


-- this table stores all changes that needs to be performed to the GlobalIntegers table
-- This way, each transaction can add row independently in this table without having to lock
-- any row (which was the case with previous FastTotalSize).
-- These changes will be applied at regular interval by an external thread or when someone
-- requests the statistics
CREATE TABLE IF NOT EXISTS GlobalIntegersChanges(
    key INTEGER,
    value BIGINT);

CREATE OR REPLACE FUNCTION UpdateSingleStatistic(
    IN statistics_key INTEGER,
    OUT new_value BIGINT
) AS $body$
BEGIN

  -- Delete the current changes, sum them and update the GlobalIntegers row.
  -- New rows can be added in the meantime, they won't be deleted or summed.
  WITH deleted_rows AS (
      DELETE FROM GlobalIntegersChanges
      WHERE GlobalIntegersChanges.key = statistics_key
      RETURNING value
  )
  UPDATE GlobalIntegers
  SET value = value + (
      SELECT COALESCE(SUM(value), 0)
      FROM deleted_rows
  )
  WHERE GlobalIntegers.key = statistics_key
  RETURNING value INTO new_value;

END;
$body$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION UpdateStatistics(
  OUT patients_cunt BIGINT,
  OUT studies_count BIGINT,
  OUT series_count BIGINT,
  OUT instances_count BIGINT,
  OUT total_compressed_size BIGINT,
  OUT total_uncompressed_size BIGINT
) AS $body$
BEGIN

  SELECT UpdateSingleStatistic(0) INTO total_compressed_size;
  SELECT UpdateSingleStatistic(1) INTO total_uncompressed_size;
  SELECT UpdateSingleStatistic(2) INTO patients_cunt;
  SELECT UpdateSingleStatistic(3) INTO studies_count;
  SELECT UpdateSingleStatistic(4) INTO series_count;
  SELECT UpdateSingleStatistic(5) INTO instances_count;

END;
$body$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION IncrementResourcesTrackerFunc()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO GlobalIntegersChanges VALUES(new.resourceType + 2, 1);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION DecrementResourcesTrackerFunc()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO GlobalIntegersChanges VALUES(old.resourceType + 2, -1);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION AttachedFileIncrementSizeFunc()
RETURNS TRIGGER AS $body$
BEGIN
  INSERT INTO GlobalIntegersChanges VALUES(0, new.compressedSize);
  INSERT INTO GlobalIntegersChanges VALUES(1, new.uncompressedSize);
  RETURN NULL;
END;
$body$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION AttachedFileDecrementSizeFunc() 
RETURNS TRIGGER AS $body$
BEGIN
  INSERT INTO GlobalIntegersChanges VALUES(0, -old.compressedSize);
  INSERT INTO GlobalIntegersChanges VALUES(1, -old.uncompressedSize);
  RETURN NULL;
END;
$body$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS AttachedFileIncrementSize on AttachedFiles;
CREATE TRIGGER AttachedFileIncrementSize
AFTER INSERT ON AttachedFiles
FOR EACH ROW
EXECUTE PROCEDURE AttachedFileIncrementSizeFunc();

DROP TRIGGER IF EXISTS AttachedFileDecrementSize on AttachedFiles;
CREATE TRIGGER AttachedFileDecrementSize
AFTER DELETE ON AttachedFiles
FOR EACH ROW
EXECUTE PROCEDURE AttachedFileDecrementSizeFunc();

DROP TRIGGER IF EXISTS IncrementResourcesTracker on Resources;
CREATE TRIGGER IncrementResourcesTracker
AFTER INSERT ON Resources
FOR EACH ROW
EXECUTE PROCEDURE IncrementResourcesTrackerFunc();

DROP TRIGGER IF EXISTS DecrementResourcesTracker on Resources;
CREATE TRIGGER DecrementResourcesTracker
AFTER DELETE ON Resources
FOR EACH ROW
EXECUTE PROCEDURE DecrementResourcesTrackerFunc();


------------------- InsertOrUpdateMetadata function -------------------
CREATE OR REPLACE FUNCTION InsertOrUpdateMetadata(resource_ids BIGINT[],
                                                  metadata_types INTEGER[], 
                                                  metadata_values TEXT[],
                                                  revisions INTEGER[])
RETURNS VOID AS $body$
BEGIN
  	FOR i IN 1 .. ARRAY_LENGTH(resource_ids, 1) LOOP
		-- RAISE NOTICE 'Parameter %: % % %', i, resource_ids[i], metadata_types[i], metadata_values[i];
		INSERT INTO Metadata VALUES(resource_ids[i], metadata_types[i], metadata_values[i], revisions[i]) 
          ON CONFLICT (id, type) DO UPDATE SET value = EXCLUDED.value, revision = EXCLUDED.revision;
	END LOOP;
  
END;
$body$ LANGUAGE plpgsql;


------------------- GetLastChange function -------------------
DROP TRIGGER IF EXISTS InsertedChange ON Changes;

-- insert the value if not already there
INSERT INTO GlobalIntegers
    SELECT 6, CAST(COALESCE(MAX(seq), 0) AS BIGINT) FROM Changes
    ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION InsertedChangeFunc() 
RETURNS TRIGGER AS $body$
BEGIN
    UPDATE GlobalIntegers SET value = new.seq WHERE key = 6;
    RETURN NULL;
END;
$body$ LANGUAGE plpgsql;

CREATE TRIGGER InsertedChange
AFTER INSERT ON Changes
FOR EACH ROW
EXECUTE PROCEDURE InsertedChangeFunc();


------------------- CreateInstance function -------------------
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
DELETE FROM GlobalProperties WHERE property IN (1, 4, 6, 10, 11, 12, 13);
INSERT INTO GlobalProperties VALUES (1, 6); -- GlobalProperty_DatabaseSchemaVersion
INSERT INTO GlobalProperties VALUES (4, 2); -- GlobalProperty_DatabasePatchLevel
INSERT INTO GlobalProperties VALUES (6, 1); -- GlobalProperty_GetTotalSizeIsFast
INSERT INTO GlobalProperties VALUES (10, 1); -- GlobalProperty_HasTrigramIndex
INSERT INTO GlobalProperties VALUES (11, 3); -- GlobalProperty_HasCreateInstance  -- this is actually the 3rd version of HasCreateInstance
INSERT INTO GlobalProperties VALUES (12, 1); -- GlobalProperty_HasFastCountResources
INSERT INTO GlobalProperties VALUES (13, 1); -- GlobalProperty_GetLastChangeIndex
