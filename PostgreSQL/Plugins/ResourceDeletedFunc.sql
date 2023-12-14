-- this script can be used either the first time we create the DB or during an upgrade
DROP TRIGGER IF EXISTS ResourceDeleted ON Resources;

-- The following trigger combines 2 triggers from SQLite:
-- ResourceDeleted + ResourceDeletedParentCleaning
CREATE OR REPLACE FUNCTION ResourceDeletedFunc() 
RETURNS TRIGGER AS $body$
BEGIN
  --RAISE NOTICE 'Delete resource %', old.parentId;
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

-- we'll now use temporary tables so we need to remove the old tables !
DROP TABLE IF EXISTS DeletedFiles;
DROP TABLE IF EXISTS RemainingAncestor;
DROP TABLE IF EXISTS DeletedResources;


CREATE OR REPLACE FUNCTION DeleteResource(
    IN id BIGINT,
    OUT remainingAncestorResourceType INTEGER,
    OUT remainingAncestorPublicId TEXT) AS $body$

DECLARE
    deleted_row RECORD;

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


    -- delete the resource itself
    DELETE FROM Resources WHERE internalId=id RETURNING * INTO deleted_row;
    -- note: there is a ResourceDeletedFunc trigger that will execute here and delete the parents if there are no remaining children + 

    -- If this resource still has siblings, keep track of the remaining parent
    -- (a parent that must not be deleted but whose LastUpdate must be updated)
    SELECT resourceType, publicId INTO remainingAncestorResourceType, remainingAncestorPublicId
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
