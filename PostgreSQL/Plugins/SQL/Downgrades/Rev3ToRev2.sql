-- This file contains an SQL procedure to downgrade from schema Rev3 to Rev2 (version = 6, revision = 1).
  -- It actually deletes the ChildCount table and triggers
  -- It actually does not uninstall ChildrenIndex2 because it is anyway more efficient than 
     -- ChildrenIndex and is not incompatible with previous revisions.

DROP TRIGGER IF EXISTS DecrementChildCount ON Resources;
DROP TRIGGER IF EXISTS IncrementChildCount ON Resources;
DROP TABLE ChildCount;
DROP FUNCTION UpdateChildCount;


-- set the global properties that actually documents the DB version, revision and some of the capabilities
-- modify only the ones that have changed
DELETE FROM GlobalProperties WHERE property IN (4, 11);
INSERT INTO GlobalProperties VALUES (4, 2); -- GlobalProperty_DatabasePatchLevel
