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
