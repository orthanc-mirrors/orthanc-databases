ALTER TABLE InvalidChildCounts DROP COLUMN pk;
ALTER TABLE GlobalIntegersChanges DROP COLUMN pk;
----------

-- set the global properties that actually documents the DB version, revision and some of the capabilities
-- modify only the ones that have changed
DELETE FROM GlobalProperties WHERE property IN (4);
INSERT INTO GlobalProperties VALUES (4, 6); -- GlobalProperty_DatabasePatchLevel
